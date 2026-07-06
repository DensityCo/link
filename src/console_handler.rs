use crate::client::{ClientError, ClientEvent};
use crate::console::{
    ConsoleBackend, ConsoleError, ConsoleFileReceiver, ConsoleOutput, ConsoleSession,
};
use crate::outbound::OutboundSender;
use crate::protocol::ChannelBuilder;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

pub(crate) struct ConsoleHandler {
    channel: Option<ChannelBuilder>,
    backend: Option<Arc<dyn ConsoleBackend>>,
    session: Option<Box<dyn ConsoleSession>>,
    deadline: Option<Instant>,
    file_receiver: ConsoleFileReceiver,
    output_tx: mpsc::UnboundedSender<ConsoleOutput>,
    outbound_tx: OutboundSender,
    event_tx: mpsc::Sender<ClientEvent>,
    timeout_secs: u64,
}

impl ConsoleHandler {
    pub(crate) fn new(
        channel: Option<ChannelBuilder>,
        backend: Option<Arc<dyn ConsoleBackend>>,
        data_dir: PathBuf,
        output_tx: mpsc::UnboundedSender<ConsoleOutput>,
        outbound_tx: OutboundSender,
        event_tx: mpsc::Sender<ClientEvent>,
        timeout_secs: u64,
    ) -> Self {
        Self {
            channel,
            backend,
            session: None,
            deadline: None,
            file_receiver: ConsoleFileReceiver::new(data_dir),
            output_tx,
            outbound_tx,
            event_tx,
            timeout_secs,
        }
    }

    pub(crate) fn timeout_at(&self) -> Instant {
        self.deadline
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(365 * 24 * 60 * 60))
    }

    pub(crate) fn session_active(&self) -> bool {
        self.session.is_some()
    }

    pub(crate) async fn handle_message(
        &mut self,
        msg: crate::protocol::Message,
    ) -> Result<(), ClientError> {
        if msg.is_reply() {
            debug!(
                ref_id = ?msg.msg_ref,
                status = ?msg.reply_status(),
                payload = %msg.payload,
                "received console reply"
            );

            if let Some(channel) = self.channel.as_ref() {
                if msg.msg_ref.as_deref() == Some(channel.join_ref.as_str()) && msg.reply_ok() {
                    info!("joined console channel");
                    let _ = self.event_tx.send(ClientEvent::ConsoleJoined).await;
                }
            }

            return Ok(());
        }

        let Some(channel) = self.channel.as_ref().cloned() else {
            warn!(event = %msg.event, "received console event before console channel joined");
            return Ok(());
        };

        match msg.event.as_str() {
            "dn" => {
                self.ensure_session().await?;

                if let Some(session) = self.session.as_mut() {
                    let data = msg
                        .payload
                        .get("data")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    session.write_input(data)?;
                    self.reset_deadline();
                }
            }
            "window_size" => {
                self.ensure_session().await?;

                if let Some(session) = self.session.as_mut() {
                    let (rows, cols) = console_size_from_payload(&msg.payload);
                    session.resize(rows, cols)?;
                    self.reset_deadline();
                }
            }
            "restart" => {
                self.stop_session().await?;
                self.push_text(&channel, "\r*** Restarting shell ***\r")
                    .await?;
                self.ensure_session().await?;
            }
            "file-data/start" => {
                let result = msg
                    .payload
                    .get("filename")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| ConsoleError::File("missing filename".to_string()))
                    .and_then(|filename| self.file_receiver.start(filename));

                match result {
                    Ok(path) => info!(path = %path.display(), "started console file upload"),
                    Err(error) => {
                        warn!(error = %error, "failed to start console file upload");
                        self.push_text(
                            &channel,
                            &format!("\rconsole file upload failed: {error}\r\n"),
                        )
                        .await?;
                    }
                }
            }
            "file-data" => {
                let result = msg
                    .payload
                    .get("data")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| ConsoleError::File("missing data".to_string()))
                    .and_then(|data| self.file_receiver.append_base64(data));

                if let Err(error) = result {
                    warn!(error = %error, "failed to write console file upload chunk");
                    self.push_text(
                        &channel,
                        &format!("\rconsole file upload failed: {error}\r\n"),
                    )
                    .await?;
                }
            }
            "file-data/stop" => {
                if let Some(path) = self.file_receiver.finish() {
                    info!(path = %path.display(), "finished console file upload");
                }
            }
            other => {
                debug!(event = other, "unhandled console event");
            }
        }

        Ok(())
    }

    pub(crate) async fn forward_output(
        &mut self,
        output: ConsoleOutput,
    ) -> Result<(), ClientError> {
        let Some(channel) = self.channel.as_ref().cloned() else {
            return Ok(());
        };

        self.reset_deadline();
        self.push_text(&channel, &output.data).await
    }

    pub(crate) async fn stop_for_timeout(&mut self) -> Result<(), ClientError> {
        if let Some(channel) = self.channel.as_ref().cloned() {
            self.push_text(
                &channel,
                "\r****************************************\r\n*   Session timeout due to inactivity  *\r\n*                                      *\r\n*   Press any key to continue...       *\r\n****************************************\r\n",
            )
            .await?;
        }

        self.stop_session().await
    }

    async fn ensure_session(&mut self) -> Result<(), ClientError> {
        if self.session.is_some() {
            return Ok(());
        }

        let Some(backend) = self.backend.as_ref() else {
            warn!("console message received but no console backend is configured");
            return Ok(());
        };

        let session = backend.start(self.output_tx.clone())?;
        self.session = Some(session);
        self.reset_deadline();
        let _ = self.event_tx.send(ClientEvent::ConsoleStarted).await;
        info!("started console session");
        Ok(())
    }

    async fn stop_session(&mut self) -> Result<(), ClientError> {
        if let Some(mut session) = self.session.take() {
            if let Err(error) = session.stop() {
                warn!(error = %error, "failed to stop console session cleanly");
            }
            self.deadline = None;
            let _ = self.event_tx.send(ClientEvent::ConsoleStopped).await;
            info!("stopped console session");
        }
        Ok(())
    }

    async fn push_text(&mut self, channel: &ChannelBuilder, data: &str) -> Result<(), ClientError> {
        if data.is_empty() {
            return Ok(());
        }

        self.outbound_tx
            .push_custom(channel, "up", json!({ "data": data }))
            .await?;
        Ok(())
    }

    fn reset_deadline(&mut self) {
        self.deadline = Some(Instant::now() + Duration::from_secs(self.timeout_secs));
    }
}

fn console_size_from_payload(payload: &serde_json::Value) -> (u16, u16) {
    let rows = payload
        .get("height")
        .or_else(|| payload.get("rows"))
        .and_then(|value| value.as_u64())
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(24);

    let cols = payload
        .get("width")
        .or_else(|| payload.get("cols"))
        .and_then(|value| value.as_u64())
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(80);

    (rows.max(1), cols.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn console_size_accepts_console_and_local_shell_shapes() {
        assert_eq!(
            console_size_from_payload(&json!({"height": 40, "width": 120})),
            (40, 120)
        );
        assert_eq!(
            console_size_from_payload(&json!({"rows": 30, "cols": 100})),
            (30, 100)
        );
        assert_eq!(console_size_from_payload(&json!({})), (24, 80));
    }
}
