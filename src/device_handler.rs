use crate::alarms::AlarmStore;
use crate::client::{ClientError, ClientEvent};
use crate::deployment::DeploymentManager;
use crate::deployment_handler::DeploymentHandler;
use crate::device_reporter::DeviceReporter;
use crate::extensions_handler::ExtensionsHandler;
use crate::identify::IdentifyController;
use crate::protocol::{DeviceChannel, Message, ProtocolEvent};
use crate::reboot::{RebootController, RebootReason};
use crate::scripts::{ScriptController, ScriptHandler};
use crate::transport::TransportSender;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub(crate) struct DeviceHandler {
    reporter: DeviceReporter,
    event_tx: mpsc::Sender<ClientEvent>,
    reboot_controller: RebootController,
    identify_controller: IdentifyController,
    deployments: DeploymentHandler,
    scripts: ScriptHandler,
}

pub(crate) struct DeviceHandlerDependencies {
    pub(crate) deployment_manager: DeploymentManager,
    pub(crate) alarm_store: AlarmStore,
    pub(crate) reboot_controller: RebootController,
    pub(crate) identify_controller: IdentifyController,
    pub(crate) script_controller: ScriptController,
    pub(crate) transport_tx: TransportSender,
    pub(crate) event_tx: mpsc::Sender<ClientEvent>,
    pub(crate) task_error_tx: mpsc::Sender<ClientError>,
}

impl DeviceHandler {
    pub(crate) fn new(channel: DeviceChannel, deps: DeviceHandlerDependencies) -> Self {
        let reporter = DeviceReporter::new(channel, deps.transport_tx);
        let deployments = DeploymentHandler::new(
            deps.deployment_manager,
            deps.alarm_store,
            reporter.clone(),
            deps.reboot_controller.clone(),
            deps.event_tx.clone(),
            deps.task_error_tx.clone(),
        );
        let scripts = ScriptHandler::new(
            deps.script_controller,
            reporter.clone(),
            deps.event_tx.clone(),
            deps.task_error_tx,
        );

        Self {
            reporter,
            event_tx: deps.event_tx,
            reboot_controller: deps.reboot_controller,
            identify_controller: deps.identify_controller,
            deployments,
            scripts,
        }
    }

    pub(crate) async fn handle_message(
        &mut self,
        msg: Message,
        extensions: &mut ExtensionsHandler,
    ) -> Result<(), ClientError> {
        match msg.event() {
            Some(ProtocolEvent::ExtensionsGet) => {
                info!("received extensions request");
                extensions.join_available_extensions().await?;
            }
            Some(ProtocolEvent::Update) => {
                self.deployments.handle_update(msg).await;
            }
            Some(ProtocolEvent::Reboot) => {
                self.handle_reboot().await?;
            }
            Some(ProtocolEvent::Identify) => {
                self.handle_identify().await?;
            }
            Some(ProtocolEvent::ScriptsRun) => {
                self.scripts.handle_run(msg).await;
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

    async fn handle_reboot(&mut self) -> Result<(), ClientError> {
        info!("received reboot command");
        let _ = self.event_tx.send(ClientEvent::RebootRequested).await;
        let reason = RebootReason::ServerRequested;

        if self.reboot_controller.is_enabled_for(reason) {
            self.reporter.rebooting().await?;
            self.reboot_controller.execute(reason).await?;
        } else {
            warn!("reboot command received but reboot is not enabled");
        }

        Ok(())
    }

    async fn handle_identify(&mut self) -> Result<(), ClientError> {
        info!("received identify command");
        let _ = self.event_tx.send(ClientEvent::IdentifyRequested).await;

        if self.identify_controller.is_enabled() {
            self.identify_controller.execute().await?;
        } else {
            info!("identify command received but no identify action is configured");
        }

        Ok(())
    }
}
