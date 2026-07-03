use crate::auth::shared_secret::SharedSecretAuth;
use crate::config::{AuthConfig, Config};
use thiserror::Error;
use tracing::info;
use tungstenite::http;

pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("auth error: {0}")]
    Auth(String),
}

pub async fn connect(config: &Config, serial: &str) -> Result<WsStream, TransportError> {
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
