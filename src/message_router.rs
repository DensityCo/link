use crate::client::{ClientError, ClientEvent};
use crate::console::{ConsoleBackend, ConsoleOutput};
use crate::console_handler::ConsoleHandler;
use crate::deployment::DeploymentManager;
use crate::deployment_supervisor::DeploymentSupervisor;
use crate::extensions::HealthReporter;
use crate::extensions_handler::ExtensionsHandler;
use crate::outbound::OutboundSender;
use crate::protocol::{ChannelBuilder, Message, ProtocolEvent};
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

pub(crate) struct RouterDependencies {
    pub(crate) deployment_manager: DeploymentManager,
    pub(crate) health_reporter: Arc<dyn HealthReporter>,
    pub(crate) console_backend: Option<Arc<dyn ConsoleBackend>>,
    pub(crate) outbound_tx: OutboundSender,
    pub(crate) connection_error_tx: mpsc::UnboundedSender<ClientError>,
    pub(crate) event_tx: mpsc::Sender<ClientEvent>,
    pub(crate) data_dir: PathBuf,
    pub(crate) console_timeout_secs: u64,
}

pub(crate) struct MessageRouter {
    device_channel: ChannelBuilder,
    outbound_tx: OutboundSender,
    event_tx: mpsc::Sender<ClientEvent>,
    console: ConsoleHandler,
    extensions: ExtensionsHandler,
    deployments: DeploymentSupervisor,
}

impl MessageRouter {
    pub(crate) fn new(
        device_channel: ChannelBuilder,
        console_channel: Option<ChannelBuilder>,
        console_output_tx: mpsc::UnboundedSender<ConsoleOutput>,
        deps: RouterDependencies,
    ) -> Self {
        let console = ConsoleHandler::new(
            console_channel,
            deps.console_backend,
            deps.data_dir,
            console_output_tx,
            deps.outbound_tx.clone(),
            deps.event_tx.clone(),
            deps.console_timeout_secs,
        );
        let extensions = ExtensionsHandler::new(
            deps.health_reporter,
            deps.outbound_tx.clone(),
            deps.event_tx.clone(),
        );
        let deployments = DeploymentSupervisor::new(
            deps.deployment_manager,
            device_channel.clone(),
            deps.outbound_tx.clone(),
            deps.event_tx.clone(),
            deps.connection_error_tx,
        );

        Self {
            device_channel,
            outbound_tx: deps.outbound_tx,
            event_tx: deps.event_tx,
            console,
            extensions,
            deployments,
        }
    }

    pub(crate) fn console_timeout_at(&self) -> Instant {
        self.console.timeout_at()
    }

    pub(crate) fn console_session_active(&self) -> bool {
        self.console.session_active()
    }

    pub(crate) async fn handle_message(&mut self, msg: Message) -> Result<(), ClientError> {
        if msg.topic == "extensions" {
            self.extensions.handle_message(msg).await?;
            return Ok(());
        }

        if msg.topic == "console" {
            self.console.handle_message(msg).await?;
            return Ok(());
        }

        match msg.event() {
            Some(ProtocolEvent::ExtensionsGet) => {
                info!("received extensions request");
                self.extensions.join_available_extensions().await?;
            }
            Some(ProtocolEvent::Update) => {
                self.deployments.handle_update(msg).await;
            }
            Some(ProtocolEvent::Reboot) => {
                info!("received reboot command");
                let _ = self
                    .outbound_tx
                    .push(&self.device_channel, ProtocolEvent::Rebooting, json!({}))
                    .await;
                let _ = self.event_tx.send(ClientEvent::RebootRequested).await;
            }
            Some(ProtocolEvent::PhxReply) => {
                debug!(
                    ref_id = ?msg.msg_ref,
                    status = ?msg.reply_status(),
                    "received reply"
                );
            }
            Some(ProtocolEvent::PhxError) => {
                warn!(topic = %msg.topic, "channel error");
            }
            Some(ProtocolEvent::PhxClose) => {
                info!(topic = %msg.topic, "channel closed by server");
                let _ = self
                    .event_tx
                    .send(ClientEvent::Disconnected(
                        "channel closed by server".to_string(),
                    ))
                    .await;
                return Err(ClientError::ChannelClosed);
            }
            Some(event) => {
                debug!(event = event.as_str(), "unhandled event");
            }
            None => {
                debug!(event = msg.event, "unhandled event");
            }
        }
        Ok(())
    }

    pub(crate) async fn forward_console_output(
        &mut self,
        output: ConsoleOutput,
    ) -> Result<(), ClientError> {
        self.console.forward_output(output).await
    }

    pub(crate) async fn stop_console_for_timeout(&mut self) -> Result<(), ClientError> {
        self.console.stop_for_timeout().await
    }
}
