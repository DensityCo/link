use crate::protocol::{ChannelBuilder, Message, ProtocolEvent};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct OutboundMessage {
    pub message: Message,
    pub result_tx: oneshot::Sender<Result<(), String>>,
}

pub(crate) type OutboundReceiver = mpsc::UnboundedReceiver<OutboundMessage>;

#[derive(Debug, Error)]
pub(crate) enum OutboundError {
    #[error("outbound channel closed")]
    Closed,
    #[error("websocket write failed: {0}")]
    Write(String),
}

#[derive(Clone)]
pub(crate) struct OutboundSender {
    tx: mpsc::UnboundedSender<OutboundMessage>,
}

impl OutboundSender {
    pub async fn send(&self, message: Message) -> Result<(), OutboundError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.tx
            .send(OutboundMessage { message, result_tx })
            .map_err(|_| OutboundError::Closed)?;

        match result_rx.await.map_err(|_| OutboundError::Closed)? {
            Ok(()) => Ok(()),
            Err(reason) => Err(OutboundError::Write(reason)),
        }
    }

    pub async fn push(
        &self,
        channel: &ChannelBuilder,
        event: ProtocolEvent,
        payload: Value,
    ) -> Result<(), OutboundError> {
        self.send(channel.push(event, payload)).await
    }

    pub async fn push_custom(
        &self,
        channel: &ChannelBuilder,
        event: &str,
        payload: Value,
    ) -> Result<(), OutboundError> {
        self.send(channel.push_custom(event, payload)).await
    }
}

pub(crate) fn channel() -> (OutboundSender, OutboundReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (OutboundSender { tx }, rx)
}
