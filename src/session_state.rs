use crate::alarms::{AlarmSource, AlarmStore};
use crate::client::{ClientError, ClientEvent};
use crate::console::{ConsoleBackend, ConsoleOutput};
use crate::console_handler::ConsoleHandler;
use crate::deployment::DeploymentManager;
use crate::device_handler::{DeviceHandler, DeviceHandlerDependencies};
use crate::extensions::HealthReporter;
use crate::extensions_handler::ExtensionsHandler;
use crate::identify::IdentifyController;
use crate::protocol::{ConsoleChannel, DeviceChannel, ExtensionsChannel, Message};
use crate::reboot::RebootController;
use crate::scripts::ScriptController;
use crate::transport::TransportSender;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub(crate) struct SessionDependencies {
    pub(crate) deployment_manager: DeploymentManager,
    pub(crate) health_reporter: Arc<dyn HealthReporter>,
    pub(crate) alarm_source: Arc<dyn AlarmSource>,
    pub(crate) alarm_store: AlarmStore,
    pub(crate) console_backend: Option<Arc<dyn ConsoleBackend>>,
    pub(crate) reboot_controller: RebootController,
    pub(crate) identify_controller: IdentifyController,
    pub(crate) script_controller: ScriptController,
    pub(crate) transport_tx: TransportSender,
    pub(crate) task_error_tx: mpsc::Sender<ClientError>,
    pub(crate) event_tx: mpsc::Sender<ClientEvent>,
    pub(crate) data_dir: PathBuf,
    pub(crate) console_timeout_secs: u64,
}

pub(crate) struct SessionState {
    device: DeviceHandler,
    console: ConsoleHandler,
    extensions: ExtensionsHandler,
}

impl SessionState {
    pub(crate) fn new(
        device_channel: DeviceChannel,
        console_channel: Option<ConsoleChannel>,
        console_output_tx: mpsc::Sender<ConsoleOutput>,
        deps: SessionDependencies,
    ) -> Self {
        let console = ConsoleHandler::new(
            console_channel,
            deps.console_backend,
            deps.data_dir,
            console_output_tx,
            deps.transport_tx.clone(),
            deps.event_tx.clone(),
            deps.console_timeout_secs,
        );
        let extensions = ExtensionsHandler::new(
            deps.health_reporter,
            deps.alarm_source,
            deps.transport_tx.clone(),
            deps.event_tx.clone(),
        );
        let device = DeviceHandler::new(
            device_channel,
            DeviceHandlerDependencies {
                deployment_manager: deps.deployment_manager,
                alarm_store: deps.alarm_store,
                reboot_controller: deps.reboot_controller,
                identify_controller: deps.identify_controller,
                script_controller: deps.script_controller,
                transport_tx: deps.transport_tx,
                event_tx: deps.event_tx,
                task_error_tx: deps.task_error_tx,
            },
        );

        Self {
            device,
            console,
            extensions,
        }
    }

    pub(crate) fn console_timeout_at(&self) -> Option<Instant> {
        self.console.timeout_at()
    }

    pub(crate) async fn handle_message(&mut self, msg: Message) -> Result<(), ClientError> {
        if msg.topic == ExtensionsChannel::TOPIC {
            self.extensions.handle_message(msg).await?;
            return Ok(());
        }

        if msg.topic == ConsoleChannel::TOPIC {
            self.console.handle_message(msg).await?;
            return Ok(());
        }

        self.device.handle_message(msg, &mut self.extensions).await
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
