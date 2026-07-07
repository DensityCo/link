use crate::client::ClientError;
use crate::config::Config;
use crate::protocol::{ConsoleChannel, DeviceChannel, Message};
use crate::transport::{TransportConnection, TransportSender};
use serde_json::Value;
use tokio::time::{Duration, Instant};
use tracing::info;

const JOIN_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct PhoenixSession {
    transport: TransportConnection,
    device_channel: Option<DeviceChannel>,
}

impl PhoenixSession {
    pub(crate) async fn connect(config: &Config, serial: &str) -> Result<Self, ClientError> {
        let transport = TransportConnection::connect(config, serial).await?;
        Ok(Self {
            transport,
            device_channel: None,
        })
    }

    pub(crate) fn sender(&self) -> TransportSender {
        self.transport.sender()
    }

    pub(crate) async fn join_device(
        &mut self,
        join_payload: &Value,
    ) -> Result<DeviceChannel, ClientError> {
        let channel = DeviceChannel::new();
        self.transport
            .send(channel.join(join_payload.clone()))
            .await?;
        info!(topic = %channel.topic(), "sent channel join");

        let join_reply = self.wait_for_reply(channel.join_ref()).await?;
        if !join_reply.reply_ok() {
            let reason = join_reply
                .payload
                .get("response")
                .and_then(|response| response.get("reason"))
                .and_then(|reason| reason.as_str())
                .unwrap_or("unknown");
            return Err(ClientError::JoinRejected(reason.to_string()));
        }

        self.device_channel = Some(channel.clone());
        info!("joined device channel");
        Ok(channel)
    }

    pub(crate) async fn join_console(
        &self,
        join_payload: &Value,
    ) -> Result<ConsoleChannel, ClientError> {
        let channel = ConsoleChannel::new();
        self.transport
            .send(channel.join(join_payload.clone()))
            .await?;
        info!("sent console channel join");
        Ok(channel)
    }

    pub(crate) async fn send_heartbeat(&self) -> Result<(), ClientError> {
        let Some(channel) = self.device_channel.as_ref() else {
            return Err(ClientError::Connection(
                "cannot send heartbeat before device channel joins".to_string(),
            ));
        };

        self.transport.send(channel.heartbeat()).await?;
        Ok(())
    }

    pub(crate) async fn recv(&mut self) -> Result<Option<Message>, ClientError> {
        self.transport.recv().await.map_err(ClientError::Transport)
    }

    async fn wait_for_reply(&mut self, join_ref: &str) -> Result<Message, ClientError> {
        let deadline = Instant::now() + JOIN_TIMEOUT;

        loop {
            tokio::select! {
                msg = self.recv() => {
                    match msg? {
                        Some(msg) if msg.is_reply() && msg.msg_ref.as_deref() == Some(join_ref) => {
                            return Ok(msg);
                        }
                        Some(_) => {}
                        None => return Err(ClientError::ChannelClosed),
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ClientError::Connection("join reply timeout".to_string()));
                }
            }
        }
    }
}
