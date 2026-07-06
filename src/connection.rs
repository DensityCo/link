use crate::client::{ClientError, ClientEvent};
use crate::config::Config;
use crate::console::{ConsoleBackend, ConsoleOutput};
use crate::deployment::DeploymentManager;
use crate::extensions::HealthReporter;
use crate::message_router::{MessageRouter, RouterDependencies};
use crate::outbound::{self, OutboundReceiver};
use crate::protocol::{ChannelBuilder, Message};
use crate::task_set::TaskSet;
use crate::transport;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

pub(crate) struct ConnectionParts {
    pub(crate) config: Config,
    pub(crate) serial: String,
    pub(crate) join_payload: Value,
    pub(crate) deployment_manager: DeploymentManager,
    pub(crate) health_reporter: Arc<dyn HealthReporter>,
    pub(crate) console_backend: Option<Arc<dyn ConsoleBackend>>,
}

pub(crate) struct ConnectionLoop {
    parts: ConnectionParts,
}

impl ConnectionLoop {
    pub(crate) fn new(parts: ConnectionParts) -> Self {
        Self { parts }
    }

    pub(crate) async fn run(self, event_tx: mpsc::Sender<ClientEvent>) -> Result<(), ClientError> {
        let ws_stream = transport::connect(&self.parts.config, &self.parts.serial).await?;
        let _ = event_tx.send(ClientEvent::Connected).await;

        let (mut write, mut read) = ws_stream.split();
        let device_channel = ChannelBuilder::new("device".to_string());
        send_raw(
            &mut write,
            device_channel.join(self.parts.join_payload.clone()),
        )
        .await?;
        info!(topic = %device_channel.topic, "sent channel join");

        let join_reply = wait_for_reply(&mut read, &device_channel.join_ref).await?;
        if !join_reply.reply_ok() {
            let reason = join_reply
                .payload
                .get("response")
                .and_then(|response| response.get("reason"))
                .and_then(|reason| reason.as_str())
                .unwrap_or("unknown");
            return Err(ClientError::JoinRejected(reason.to_string()));
        }
        info!("joined device channel");
        let _ = event_tx.send(ClientEvent::Joined).await;

        let (outbound_tx, outbound_rx) = outbound::channel();
        let (connection_error_tx, mut connection_error_rx) = mpsc::unbounded_channel();
        let mut writer_tasks = TaskSet::new();
        spawn_writer(
            &mut writer_tasks,
            write,
            outbound_rx,
            connection_error_tx.clone(),
        );

        let console_channel = if self.parts.console_backend.is_some() {
            let channel = ChannelBuilder::new("console".to_string());
            outbound_tx
                .send(channel.join(self.parts.join_payload.clone()))
                .await?;
            info!("sent console channel join");
            Some(channel)
        } else {
            None
        };

        let (console_output_tx, mut console_output_rx) = mpsc::unbounded_channel::<ConsoleOutput>();
        let router_dependencies = RouterDependencies {
            deployment_manager: self.parts.deployment_manager,
            health_reporter: self.parts.health_reporter,
            console_backend: self.parts.console_backend,
            outbound_tx: outbound_tx.clone(),
            connection_error_tx: connection_error_tx.clone(),
            event_tx: event_tx.clone(),
            data_dir: self.parts.config.data_dir(),
            console_timeout_secs: self
                .parts
                .config
                .console
                .as_ref()
                .map_or(5 * 60, |console| console.timeout_secs()),
        };
        let mut router = MessageRouter::new(
            device_channel.clone(),
            console_channel,
            console_output_tx,
            router_dependencies,
        );

        let heartbeat_interval = Duration::from_secs(self.parts.config.heartbeat_interval_secs());
        let mut next_heartbeat = Instant::now() + heartbeat_interval;

        loop {
            let console_timeout_at = router.console_timeout_at();
            let console_timeout_enabled = router.console_session_active();

            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(tungstenite::Message::Text(text))) => {
                            debug!(message = %text, "received websocket text");
                            match Message::from_json(&text) {
                                Ok(msg) => router.handle_message(msg).await?,
                                Err(error) => {
                                    warn!(error = %error, "failed to parse message");
                                }
                            }
                        }
                        Some(Ok(tungstenite::Message::Close(_))) | None => {
                            if let Ok(error) = connection_error_rx.try_recv() {
                                return connection_task_error(error, &event_tx).await;
                            }
                            info!("connection closed");
                            let _ = event_tx.send(ClientEvent::Disconnected("connection closed".to_string())).await;
                            return Ok(());
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            error!(error = %error, "websocket error");
                            let _ = event_tx.send(ClientEvent::Disconnected(error.to_string())).await;
                            return Err(ClientError::WebSocket(error.to_string()));
                        }
                    }
                }
                _ = tokio::time::sleep_until(next_heartbeat) => {
                    outbound_tx.send(device_channel.heartbeat()).await?;
                    debug!("sent heartbeat");
                    next_heartbeat = Instant::now() + heartbeat_interval;
                }
                Some(output) = console_output_rx.recv() => {
                    router.forward_console_output(output).await?;
                }
                _ = tokio::time::sleep_until(console_timeout_at), if console_timeout_enabled => {
                    router.stop_console_for_timeout().await?;
                }
                Some(error) = connection_error_rx.recv() => {
                    return connection_task_error(error, &event_tx).await;
                }
            }
        }
    }
}

async fn send_raw<S>(write: &mut S, message: Message) -> Result<(), ClientError>
where
    S: SinkExt<tungstenite::Message> + Unpin,
    S::Error: std::fmt::Display,
{
    write
        .send(tungstenite::Message::Text(message.to_json().into()))
        .await
        .map_err(|error| ClientError::WebSocket(error.to_string()))
}

fn spawn_writer<S>(
    tasks: &mut TaskSet,
    mut write: S,
    mut outbound_rx: OutboundReceiver,
    connection_error_tx: mpsc::UnboundedSender<ClientError>,
) where
    S: SinkExt<tungstenite::Message> + Unpin + Send + 'static,
    S::Error: std::fmt::Display + Send + 'static,
{
    tasks.spawn(async move {
        while let Some(outbound) = outbound_rx.recv().await {
            match write
                .send(tungstenite::Message::Text(
                    outbound.message.to_json().into(),
                ))
                .await
            {
                Ok(()) => {
                    let _ = outbound.result_tx.send(Ok(()));
                }
                Err(error) => {
                    let reason = error.to_string();
                    let _ = outbound.result_tx.send(Err(reason.clone()));
                    let _ = connection_error_tx.send(ClientError::WebSocket(reason));
                    break;
                }
            }
        }
    });
}

async fn wait_for_reply<S>(read: &mut S, join_ref: &str) -> Result<Message, ClientError>
where
    S: StreamExt<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin,
{
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(tungstenite::Message::Text(text))) => {
                        if let Ok(msg) = Message::from_json(&text) {
                            if msg.is_reply() && msg.msg_ref.as_deref() == Some(join_ref) {
                                return Ok(msg);
                            }
                        }
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(error)) => return Err(ClientError::WebSocket(error.to_string())),
                    None => return Err(ClientError::ChannelClosed),
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(ClientError::Connection("join reply timeout".to_string()));
            }
        }
    }
}

async fn connection_task_error(
    error: ClientError,
    event_tx: &mpsc::Sender<ClientEvent>,
) -> Result<(), ClientError> {
    let reason = error.to_string();
    error!(error = %reason, "connection task failed");
    let _ = event_tx.send(ClientEvent::Disconnected(reason)).await;
    Err(error)
}
