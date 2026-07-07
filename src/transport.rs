use crate::auth::shared_secret::SharedSecretAuth;
use crate::config::{AuthConfig, Config};
use crate::protocol::Message;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use tungstenite::http;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WsWrite = SplitSink<WsStream, tungstenite::Message>;
type WsRead = SplitStream<WsStream>;

const OUTBOUND_CHANNEL_CAPACITY: usize = 128;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("auth error: {0}")]
    Auth(String),
    #[error("transport channel closed")]
    Closed,
    #[error("websocket write failed: {0}")]
    Write(String),
}

pub(crate) struct TransportConnection {
    read: WsRead,
    sender: TransportSender,
    _writer_tasks: JoinSet<()>,
}

impl TransportConnection {
    pub(crate) async fn connect(config: &Config, serial: &str) -> Result<Self, TransportError> {
        let ws_stream = connect_websocket(config, serial).await?;
        let (write, read) = ws_stream.split();
        let (sender, outbound_rx) = channel();
        let mut writer_tasks = JoinSet::new();
        spawn_writer(&mut writer_tasks, write, outbound_rx);

        Ok(Self {
            read,
            sender,
            _writer_tasks: writer_tasks,
        })
    }

    pub(crate) fn sender(&self) -> TransportSender {
        self.sender.clone()
    }

    pub(crate) async fn send(&self, message: Message) -> Result<(), TransportError> {
        self.sender.send(message).await
    }

    pub(crate) async fn recv(&mut self) -> Result<Option<Message>, TransportError> {
        loop {
            match self.read.next().await {
                Some(Ok(tungstenite::Message::Text(text))) => {
                    debug!(message = %text, "received websocket text");
                    match Message::from_json(&text) {
                        Ok(message) => return Ok(Some(message)),
                        Err(error) => {
                            warn!(error = %error, "failed to parse message");
                            continue;
                        }
                    }
                }
                Some(Ok(tungstenite::Message::Close(_))) | None => return Ok(None),
                Some(Ok(_)) => continue,
                Some(Err(error)) => return Err(TransportError::Connection(error.to_string())),
            }
        }
    }
}

struct OutboundMessage {
    message: Message,
    result_tx: oneshot::Sender<Result<(), String>>,
}

type OutboundReceiver = mpsc::Receiver<OutboundMessage>;

#[derive(Clone)]
pub(crate) struct TransportSender {
    tx: mpsc::Sender<OutboundMessage>,
}

impl TransportSender {
    pub(crate) async fn send(&self, message: Message) -> Result<(), TransportError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.tx
            .send(OutboundMessage { message, result_tx })
            .await
            .map_err(|_| TransportError::Closed)?;

        result_rx
            .await
            .map_err(|_| TransportError::Closed)?
            .map_err(TransportError::Write)
    }
}

fn channel() -> (TransportSender, OutboundReceiver) {
    let (tx, rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
    (TransportSender { tx }, rx)
}

fn spawn_writer(tasks: &mut JoinSet<()>, mut write: WsWrite, mut outbound_rx: OutboundReceiver) {
    tasks.spawn(async move {
        while let Some(outbound) = outbound_rx.recv().await {
            match write
                .send(tungstenite::Message::Text(outbound.message.to_json()))
                .await
            {
                Ok(()) => {
                    let _ = outbound.result_tx.send(Ok(()));
                }
                Err(error) => {
                    let _ = outbound.result_tx.send(Err(error.to_string()));
                    break;
                }
            }
        }
    });
}

async fn connect_websocket(config: &Config, serial: &str) -> Result<WsStream, TransportError> {
    let url = config.socket_url();
    info!(url = %url, "connecting to server");

    match &config.auth {
        AuthConfig::Mtls {
            cert_path,
            key_path,
            ca_cert_path,
        } => {
            let tls_config = crate::auth::mtls::build_tls_config(cert_path, key_path, ca_cert_path)
                .map_err(|e| TransportError::Auth(e.to_string()))?;

            let connector = tokio_tungstenite::Connector::Rustls(tls_config);

            let (ws_stream, _response) = tokio_tungstenite::connect_async_tls_with_config(
                &url,
                None,
                false,
                Some(connector),
            )
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

            Ok(ws_stream)
        }
        AuthConfig::SharedSecret { key, secret } => {
            let auth = SharedSecretAuth::new(key.clone(), secret.clone());
            let auth_headers = auth
                .auth_headers(serial)
                .map_err(|e| TransportError::Auth(e.to_string()))?;
            let tls_config = crate::tls::webpki_client_config()
                .map_err(|e| TransportError::Connection(e.to_string()))?;
            let connector = tokio_tungstenite::Connector::Rustls(tls_config);

            use tungstenite::client::IntoClientRequest;
            let mut request = url
                .into_client_request()
                .map_err(|e| TransportError::Connection(e.to_string()))?;

            for (name, value) in &auth_headers {
                request.headers_mut().insert(
                    http::header::HeaderName::from_bytes(name.as_bytes())
                        .map_err(|e| TransportError::Connection(e.to_string()))?,
                    http::header::HeaderValue::from_str(value)
                        .map_err(|e| TransportError::Connection(e.to_string()))?,
                );
            }

            let (ws_stream, _response) = tokio_tungstenite::connect_async_tls_with_config(
                request,
                None,
                false,
                Some(connector),
            )
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

            Ok(ws_stream)
        }
    }
}
