use crate::client::{ClientError, ClientEvent};
use crate::extensions::{
    available_extensions_payload, extension_requested, HealthReporter, HEALTH_EXTENSION_NAME,
};
use crate::outbound::OutboundSender;
use crate::protocol::{ChannelBuilder, Message};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub(crate) struct ExtensionsHandler {
    channel: Option<ChannelBuilder>,
    health_attached: bool,
    health_reporter: Arc<dyn HealthReporter>,
    outbound_tx: OutboundSender,
    event_tx: mpsc::Sender<ClientEvent>,
}

impl ExtensionsHandler {
    pub(crate) fn new(
        health_reporter: Arc<dyn HealthReporter>,
        outbound_tx: OutboundSender,
        event_tx: mpsc::Sender<ClientEvent>,
    ) -> Self {
        Self {
            channel: None,
            health_attached: false,
            health_reporter,
            outbound_tx,
            event_tx,
        }
    }

    pub(crate) async fn join_available_extensions(&mut self) -> Result<(), ClientError> {
        let channel = ChannelBuilder::new("extensions".to_string());
        let join_msg = channel.join(available_extensions_payload());
        self.outbound_tx.send(join_msg).await?;
        self.channel = Some(channel);
        info!("sent extensions channel join");
        Ok(())
    }

    pub(crate) async fn handle_message(&mut self, msg: Message) -> Result<(), ClientError> {
        if msg.is_reply() {
            debug!(
                ref_id = ?msg.msg_ref,
                status = ?msg.reply_status(),
                payload = %msg.payload,
                "received extensions reply"
            );

            if let Some(channel) = self.channel.as_ref().cloned() {
                if msg.msg_ref.as_deref() == Some(channel.join_ref.as_str()) && msg.reply_ok() {
                    info!("joined extensions channel");
                    let _ = self.event_tx.send(ClientEvent::ExtensionsJoined).await;
                    let response = msg.payload.get("response").unwrap_or(&msg.payload);
                    if extension_requested(response, HEALTH_EXTENSION_NAME) {
                        let attached = self.attach_health(&channel).await?;
                        if attached {
                            self.report_health(&channel).await?;
                        }
                    } else {
                        info!(
                            "health extension not selected by server; health reports will not be sent"
                        );
                    }
                }
            }

            return Ok(());
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

    async fn attach_health(&mut self, channel: &ChannelBuilder) -> Result<bool, ClientError> {
        if !self.health_attached {
            self.outbound_tx
                .push_custom(channel, "health:attached", json!({}))
                .await?;
            self.health_attached = true;
            info!("attached health extension");
            return Ok(true);
        }
        Ok(false)
    }

    async fn detach_health(&mut self, channel: &ChannelBuilder) -> Result<(), ClientError> {
        if self.health_attached {
            self.outbound_tx
                .push_custom(channel, "health:detached", json!({}))
                .await?;
            self.health_attached = false;
            info!("detached health extension");
        }
        Ok(())
    }

    async fn report_health(&mut self, channel: &ChannelBuilder) -> Result<(), ClientError> {
        let report = self.health_reporter.report();
        self.outbound_tx
            .push_custom(channel, "health:report", json!({ "value": report }))
            .await?;
        let _ = self.event_tx.send(ClientEvent::HealthReported).await;
        info!("reported health");
        Ok(())
    }
}
