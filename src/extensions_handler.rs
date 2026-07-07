use crate::alarms::AlarmSource;
use crate::client::{ClientError, ClientEvent};
use crate::extensions::{
    available_extensions_payload, extension_requested, HealthReporter, HEALTH_EXTENSION_NAME,
};
use crate::protocol::{ExtensionsChannel, Message};
use crate::transport::TransportSender;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub(crate) struct ExtensionsHandler {
    channel: Option<ExtensionsChannel>,
    health_attached: bool,
    health_reporter: Arc<dyn HealthReporter>,
    alarm_source: Arc<dyn AlarmSource>,
    transport_tx: TransportSender,
    event_tx: mpsc::Sender<ClientEvent>,
}

impl ExtensionsHandler {
    pub(crate) fn new(
        health_reporter: Arc<dyn HealthReporter>,
        alarm_source: Arc<dyn AlarmSource>,
        transport_tx: TransportSender,
        event_tx: mpsc::Sender<ClientEvent>,
    ) -> Self {
        Self {
            channel: None,
            health_attached: false,
            health_reporter,
            alarm_source,
            transport_tx,
            event_tx,
        }
    }

    pub(crate) async fn join_available_extensions(&mut self) -> Result<(), ClientError> {
        let channel = ExtensionsChannel::new();
        let join_msg = channel.join(available_extensions_payload());
        self.transport_tx.send(join_msg).await?;
        self.channel = Some(channel);
        info!("sent extensions channel join");
        Ok(())
    }

    pub(crate) async fn handle_message(&mut self, msg: Message) -> Result<(), ClientError> {
        if msg.is_reply() {
            return self.handle_reply(&msg).await;
        }

        let Some(channel) = self.channel.as_ref().cloned() else {
            warn!(event = %msg.event, "received extension event before extensions channel joined");
            return Ok(());
        };

        match msg.event.as_str() {
            "attach" => {
                if extension_requested(&msg.payload, HEALTH_EXTENSION_NAME) {
                    let attached = self.attach_health(&channel).await?;
                    if attached {
                        self.report_health(&channel).await?;
                    }
                }
            }
            "detach" => {
                if extension_requested(&msg.payload, HEALTH_EXTENSION_NAME) {
                    self.detach_health(&channel).await?;
                }
            }
            "health:check" => {
                if self.health_attached {
                    self.report_health(&channel).await?;
                } else {
                    warn!("health check requested before health extension attached");
                }
            }
            other => {
                debug!(event = other, "unhandled extensions event");
            }
        }

        Ok(())
    }

    async fn handle_reply(&mut self, msg: &Message) -> Result<(), ClientError> {
        debug!(
            ref_id = ?msg.msg_ref,
            status = ?msg.reply_status(),
            payload = %msg.payload,
            "received extensions reply"
        );

        let Some(channel) = self.channel.as_ref().cloned() else {
            return Ok(());
        };

        if msg.msg_ref.as_deref() != Some(channel.join_ref()) || !msg.reply_ok() {
            return Ok(());
        }

        info!("joined extensions channel");
        let _ = self.event_tx.send(ClientEvent::ExtensionsJoined).await;
        let response = msg.payload.get("response").unwrap_or(&msg.payload);

        if !extension_requested(response, HEALTH_EXTENSION_NAME) {
            info!("health extension not selected by server; health reports will not be sent");
            return Ok(());
        }

        if self.attach_health(&channel).await? {
            self.report_health(&channel).await?;
        }

        Ok(())
    }

    async fn attach_health(&mut self, channel: &ExtensionsChannel) -> Result<bool, ClientError> {
        if !self.health_attached {
            self.transport_tx.send(channel.health_attached()).await?;
            self.health_attached = true;
            info!("attached health extension");
            return Ok(true);
        }
        Ok(false)
    }

    async fn detach_health(&mut self, channel: &ExtensionsChannel) -> Result<(), ClientError> {
        if self.health_attached {
            self.transport_tx.send(channel.health_detached()).await?;
            self.health_attached = false;
            info!("detached health extension");
        }
        Ok(())
    }

    async fn report_health(&mut self, channel: &ExtensionsChannel) -> Result<(), ClientError> {
        let mut report = self.health_reporter.report();
        report.alarms.extend(self.alarm_source.alarms());
        self.transport_tx
            .send(channel.health_report(&report))
            .await?;
        let _ = self.event_tx.send(ClientEvent::HealthReported).await;
        info!("reported health");
        Ok(())
    }
}
