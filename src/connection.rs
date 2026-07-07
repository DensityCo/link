use crate::alarms::{AlarmStore, DISCONNECTED_ALARM};
use crate::client::{ClientError, ClientEvent};
use crate::config::Config;
use crate::console::{ConsoleBackend, ConsoleOutput};
use crate::deployment::DeploymentManager;
use crate::extensions::HealthReporter;
use crate::identify::IdentifyController;
use crate::phoenix_session::PhoenixSession;
use crate::reboot::RebootController;
use crate::scripts::ScriptController;
use crate::session_state::{SessionDependencies, SessionState};
use serde_json::Value;
use std::future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

const TASK_ERROR_CHANNEL_CAPACITY: usize = 16;
const CONSOLE_OUTPUT_CHANNEL_CAPACITY: usize = 128;

pub(crate) struct ConnectionParts {
    pub(crate) config: Config,
    pub(crate) serial: String,
    pub(crate) join_payload: Value,
    pub(crate) deployment_manager: DeploymentManager,
    pub(crate) health_reporter: Arc<dyn HealthReporter>,
    pub(crate) console_backend: Option<Arc<dyn ConsoleBackend>>,
    pub(crate) reboot_controller: RebootController,
    pub(crate) identify_controller: IdentifyController,
    pub(crate) script_controller: ScriptController,
    pub(crate) alarm_store: AlarmStore,
}

pub(crate) struct ConnectionLoop {
    parts: ConnectionParts,
}

impl ConnectionLoop {
    pub(crate) fn new(parts: ConnectionParts) -> Self {
        Self { parts }
    }

    pub(crate) async fn run(self, event_tx: mpsc::Sender<ClientEvent>) -> Result<(), ClientError> {
        let mut protocol =
            match PhoenixSession::connect(&self.parts.config, &self.parts.serial).await {
                Ok(protocol) => protocol,
                Err(error) => {
                    self.parts
                        .alarm_store
                        .set(DISCONNECTED_ALARM, error.to_string());
                    return Err(error);
                }
            };
        let _ = event_tx.send(ClientEvent::Connected).await;

        let transport_tx = protocol.sender();
        let (task_error_tx, mut task_error_rx) = mpsc::channel(TASK_ERROR_CHANNEL_CAPACITY);

        let device_channel = match protocol.join_device(&self.parts.join_payload).await {
            Ok(channel) => channel,
            Err(error) => {
                self.parts
                    .alarm_store
                    .set(DISCONNECTED_ALARM, error.to_string());
                return Err(error);
            }
        };
        self.parts.alarm_store.clear(DISCONNECTED_ALARM);
        let _ = event_tx.send(ClientEvent::Joined).await;

        let console_channel = if self.parts.console_backend.is_some() {
            match protocol.join_console(&self.parts.join_payload).await {
                Ok(channel) => Some(channel),
                Err(error) => {
                    self.parts
                        .alarm_store
                        .set(DISCONNECTED_ALARM, error.to_string());
                    return Err(error);
                }
            }
        } else {
            None
        };

        let (console_output_tx, mut console_output_rx) =
            mpsc::channel::<ConsoleOutput>(CONSOLE_OUTPUT_CHANNEL_CAPACITY);
        let session_dependencies = SessionDependencies {
            deployment_manager: self.parts.deployment_manager,
            health_reporter: self.parts.health_reporter,
            console_backend: self.parts.console_backend,
            reboot_controller: self.parts.reboot_controller,
            identify_controller: self.parts.identify_controller,
            script_controller: self.parts.script_controller,
            alarm_source: Arc::new(self.parts.alarm_store.clone()),
            alarm_store: self.parts.alarm_store.clone(),
            transport_tx: transport_tx.clone(),
            task_error_tx: task_error_tx.clone(),
            event_tx: event_tx.clone(),
            data_dir: self.parts.config.data_dir(),
            console_timeout_secs: self
                .parts
                .config
                .console
                .as_ref()
                .map_or(5 * 60, |console| console.timeout_secs()),
        };
        let mut session = SessionState::new(
            device_channel,
            console_channel,
            console_output_tx,
            session_dependencies,
        );

        let heartbeat_interval = Duration::from_secs(self.parts.config.heartbeat_interval_secs())
            .max(Duration::from_secs(1));
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);

        loop {
            let console_timeout_at = session.console_timeout_at();

            tokio::select! {
                msg = protocol.recv() => {
                    match msg {
                        Ok(Some(msg)) => session.handle_message(msg).await?,
                        Ok(None) => {
                            if let Ok(error) = task_error_rx.try_recv() {
                                return connection_task_error(
                                    error,
                                    &event_tx,
                                    &self.parts.alarm_store,
                                )
                                .await;
                            }
                            info!("connection closed");
                            self.parts.alarm_store.set(DISCONNECTED_ALARM, "connection closed");
                            let _ = event_tx.send(ClientEvent::Disconnected("connection closed".to_string())).await;
                            return Ok(());
                        }
                        Err(error) => {
                            let reason = error.to_string();
                            error!(error = %reason, "protocol error");
                            self.parts.alarm_store.set(DISCONNECTED_ALARM, reason.clone());
                            let _ = event_tx.send(ClientEvent::Disconnected(reason)).await;
                            return Err(error);
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    protocol.send_heartbeat().await?;
                    debug!("sent heartbeat");
                }
                Some(output) = console_output_rx.recv() => {
                    session.forward_console_output(output).await?;
                }
                _ = wait_for_console_timeout(console_timeout_at), if console_timeout_at.is_some() => {
                    session.stop_console_for_timeout().await?;
                }
                Some(error) = task_error_rx.recv() => {
                    return connection_task_error(error, &event_tx, &self.parts.alarm_store).await;
                }
            }
        }
    }
}

async fn wait_for_console_timeout(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => future::pending().await,
    }
}

async fn connection_task_error(
    error: ClientError,
    event_tx: &mpsc::Sender<ClientEvent>,
    alarm_store: &AlarmStore,
) -> Result<(), ClientError> {
    let reason = error.to_string();
    error!(error = %reason, "connection task failed");
    alarm_store.set(DISCONNECTED_ALARM, reason.clone());
    let _ = event_tx.send(ClientEvent::Disconnected(reason)).await;
    Err(error)
}
