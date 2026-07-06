use crate::client::{ClientError, ClientEvent};
use crate::deployment::{Deployment, DeploymentManager};
use crate::outbound::OutboundSender;
use crate::protocol::{ChannelBuilder, Message};
use crate::task_set::TaskSet;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub(crate) struct DeploymentSupervisor {
    manager: DeploymentManager,
    device_channel: ChannelBuilder,
    outbound_tx: OutboundSender,
    event_tx: mpsc::Sender<ClientEvent>,
    error_tx: mpsc::UnboundedSender<ClientError>,
    tasks: TaskSet,
}

impl DeploymentSupervisor {
    pub(crate) fn new(
        manager: DeploymentManager,
        device_channel: ChannelBuilder,
        outbound_tx: OutboundSender,
        event_tx: mpsc::Sender<ClientEvent>,
        error_tx: mpsc::UnboundedSender<ClientError>,
    ) -> Self {
        Self {
            manager,
            device_channel,
            outbound_tx,
            event_tx,
            error_tx,
            tasks: TaskSet::new(),
        }
    }

    pub(crate) async fn handle_update(&mut self, msg: Message) {
        info!("received deployment request");
        match Deployment::from_payload(&msg.payload) {
            Ok(deployment) => {
                let _ = self
                    .event_tx
                    .send(ClientEvent::DeploymentAvailable(deployment.clone()))
                    .await;
                let deployment_manager = self.manager.clone();
                let deployment_channel = self.device_channel.clone();
                let deployment_outbound_tx = self.outbound_tx.clone();
                let deployment_event_tx = self.event_tx.clone();
                let deployment_error_tx = self.error_tx.clone();
                self.tasks.spawn(async move {
                    if let Err(error) = crate::client_deployment::handle_deployment(
                        deployment_manager,
                        deployment,
                        deployment_channel,
                        deployment_outbound_tx,
                        deployment_event_tx,
                    )
                    .await
                    {
                        let _ = deployment_error_tx.send(error);
                    }
                });
            }
            Err(error) => {
                warn!(error = %error, "failed to parse deployment message");
            }
        }
    }
}
