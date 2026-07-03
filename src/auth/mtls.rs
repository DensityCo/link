use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MtlsError {
    #[error("failed to read file {path}: {source}")]
    FileRead {
        path: String,
        source: std::io::Error,
    },
    #[error("no certificates found in {0}")]
    NoCerts(String),
    #[error("no private key found in {0}")]
    NoKey(String),
    #[error("TLS configuration error: {0}")]
    Tls(#[from] rustls::Error),
}

/// Build a rustls ClientConfig for mTLS connection.
pub fn build_tls_config(
    cert_path: &Path,
    key_path: &Path,
    ca_cert_path: &Path,
) -> Result<Arc<rustls::ClientConfig>, MtlsError> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let mut root_store = rustls::RootCertStore::empty();

    // Add the CA certificate
    let ca_certs = load_certs(ca_cert_path)?;
    for cert in ca_certs {
        root_store.add(cert).map_err(|e| {
            MtlsError::Tls(rustls::Error::General(format!(
                "failed to add CA cert: {}",
                e
            )))
        })?;
    }

    crate::tls::mtls_client_config(root_store, certs, key).map_err(MtlsError::Tls)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, MtlsError> {
    let file = std::fs::File::open(path).map_err(|e| MtlsError::FileRead {
        path: path.display().to_string(),
        source: e,
    })?;
    let mut reader = std::io::BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| MtlsError::FileRead {
            path: path.display().to_string(),
            source: e,
        })?;
    if certs.is_empty() {
        return Err(MtlsError::NoCerts(path.display().to_string()));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, MtlsError> {
    let file = std::fs::File::open(path).map_err(|e| MtlsError::FileRead {
        path: path.display().to_string(),
        source: e,
    })?;
    let mut reader = std::io::BufReader::new(file);

    // Try to read any private key format (PKCS#8, RSA, EC)
    for item in rustls_pemfile::read_all(&mut reader) {
        match item {
            Ok(rustls_pemfile::Item::Pkcs8Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Ok(rustls_pemfile::Item::Pkcs1Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Ok(rustls_pemfile::Item::Sec1Key(key)) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            _ => continue,
        }
    }

    Err(MtlsError::NoKey(path.display().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn missing_cert_file() {
        let result = load_certs(&PathBuf::from("/nonexistent/cert.pem"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, MtlsError::FileRead { .. }));
    }

    #[test]
    fn missing_key_file() {
        let result = load_private_key(&PathBuf::from("/nonexistent/key.pem"));
        assert!(result.is_err());
    }

    #[test]
    fn empty_cert_file() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("empty.pem");
        std::fs::write(&cert_path, "").unwrap();
        let result = load_certs(&cert_path);
        assert!(matches!(result, Err(MtlsError::NoCerts(_))));
    }

    #[test]
    fn empty_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("empty.pem");
        std::fs::write(&key_path, "").unwrap();
        let result = load_private_key(&key_path);
        assert!(matches!(result, Err(MtlsError::NoKey(_))));
    }
}
