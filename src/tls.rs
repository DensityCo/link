use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TlsClientError {
    #[error("TLS configuration error: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("HTTP client TLS configuration error: {0}")]
    Reqwest(#[from] reqwest::Error),
}

fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn webpki_root_store() -> rustls::RootCertStore {
    rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
}

fn build_webpki_client_config() -> Result<rustls::ClientConfig, rustls::Error> {
    Ok(
        rustls::ClientConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(webpki_root_store())
            .with_no_client_auth(),
    )
}

pub fn webpki_client_config() -> Result<Arc<rustls::ClientConfig>, rustls::Error> {
    Ok(Arc::new(build_webpki_client_config()?))
}

pub fn mtls_client_config(
    root_store: rustls::RootCertStore,
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<rustls::ClientConfig>, rustls::Error> {
    let config = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, key)?;

    Ok(Arc::new(config))
}

pub fn reqwest_client() -> Result<reqwest::Client, TlsClientError> {
    Ok(reqwest::Client::builder()
        .use_preconfigured_tls(build_webpki_client_config()?)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webpki_roots_are_loaded() {
        assert!(!webpki_root_store().is_empty());
    }

    #[test]
    fn webpki_client_config_builds() {
        let config = webpki_client_config().unwrap();
        assert!(config.enable_sni);
    }

    #[test]
    fn reqwest_client_builds() {
        reqwest_client().unwrap();
    }
}
