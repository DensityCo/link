use crate::deployment::{Deployment, DeploymentError};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tracing::info;

/// Download firmware from a pre-signed URL to a local file.
///
/// Reports progress via a callback: fn(bytes_downloaded, total_bytes_option).
pub async fn download_firmware<F>(
    deployment: &Deployment,
    dest_dir: &Path,
    mut on_progress: F,
) -> Result<PathBuf, DeploymentError>
where
    F: FnMut(u64, Option<u64>),
{
    let client =
        crate::tls::reqwest_client().map_err(|e| DeploymentError::Download(e.to_string()))?;
    let response = client
        .get(&deployment.firmware_url)
        .send()
        .await
        .map_err(|e| DeploymentError::Download(e.to_string()))?;

    if !response.status().is_success() {
        return Err(DeploymentError::Download(format!(
            "HTTP {}",
            response.status()
        )));
    }

    let total_size = response.content_length();
    let artifact_name = firmware_file_name(deployment);
    let dest_path = dest_dir.join(&artifact_name);
    let temp_path = dest_dir.join(format!("{}.tmp", artifact_name));
    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(DeploymentError::Io)?;

    let mut downloaded: u64 = 0;
    let mut hasher = Sha256::new();
    let mut stream = response.bytes_stream();

    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| DeploymentError::Download(e.to_string()))?;
        file.write_all(&chunk).await.map_err(DeploymentError::Io)?;
        hasher.update(&chunk);
        downloaded += chunk.len() as u64;
        on_progress(downloaded, total_size);
    }

    file.flush().await.map_err(DeploymentError::Io)?;
    drop(file);

    if let Some(expected) = deployment.size {
        if downloaded != expected {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(DeploymentError::SizeMismatch {
                expected,
                actual: downloaded,
            });
        }
    }

    if let Some(expected) = deployment
        .checksum
        .as_deref()
        .map(str::trim)
        .filter(|checksum| !checksum.is_empty())
    {
        let actual = uppercase_hex(&hasher.finalize());
        if !actual.eq_ignore_ascii_case(expected) {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(DeploymentError::ChecksumMismatch {
                expected: expected.to_string(),
                actual,
            });
        }
    }

    let _ = tokio::fs::remove_file(&dest_path).await;
    tokio::fs::rename(&temp_path, &dest_path)
        .await
        .map_err(DeploymentError::Io)?;
    info!(downloaded_bytes = downloaded, path = %dest_path.display(), "firmware download complete");

    Ok(dest_path)
}

fn firmware_file_name(deployment: &Deployment) -> String {
    let stem = firmware_file_stem(&deployment.firmware_meta.uuid);
    let suffix = artifact_suffix_from_url(&deployment.firmware_url).unwrap_or_default();

    format!("{stem}{suffix}")
}

fn firmware_file_stem(uuid: &str) -> String {
    let sanitized: String = uuid
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();

    let sanitized = sanitized.trim_matches('_');
    if sanitized.is_empty() {
        "firmware-unknown".to_string()
    } else {
        format!("firmware-{}", sanitized)
    }
}

fn artifact_suffix_from_url(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url).ok()?;

    suffix_from_url_path(&parsed).or_else(|| suffix_from_proxy_query(&parsed))
}

fn suffix_from_url_path(url: &reqwest::Url) -> Option<String> {
    let filename = url.path_segments()?.rfind(|segment| !segment.is_empty())?;
    suffix_from_filename(filename)
}

fn suffix_from_proxy_query(url: &reqwest::Url) -> Option<String> {
    let encoded_url = url
        .query_pairs()
        .find_map(|(key, value)| (key == "firmware").then_some(value))?;
    let decoded_url = URL_SAFE_NO_PAD.decode(encoded_url.as_bytes()).ok()?;
    let decoded_url = String::from_utf8(decoded_url).ok()?;

    artifact_suffix_from_url(&decoded_url)
}

fn suffix_from_filename(filename: &str) -> Option<String> {
    let suffix = filename.get(filename.find('.')?..)?;
    let sanitized: String = suffix
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();

    let sanitized = sanitized.trim_matches('_');
    if sanitized.len() > 1 && sanitized.chars().any(|ch| ch.is_ascii_alphanumeric()) {
        Some(sanitized.to_string())
    } else {
        None
    }
}

fn uppercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }

    encoded
}

/// Calculate progress percentage (0-100).
pub fn progress_percent(downloaded: u64, total: Option<u64>) -> u8 {
    match total {
        Some(total) if total > 0 => ((downloaded * 100) / total).min(100) as u8,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployment::FirmwareMeta;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn progress_calculation() {
        assert_eq!(progress_percent(0, Some(100)), 0);
        assert_eq!(progress_percent(50, Some(100)), 50);
        assert_eq!(progress_percent(100, Some(100)), 100);
        assert_eq!(progress_percent(200, Some(100)), 100);
        assert_eq!(progress_percent(50, None), 0);
        assert_eq!(progress_percent(50, Some(0)), 0);
    }

    #[test]
    fn firmware_file_stem_is_path_safe() {
        assert_eq!(firmware_file_stem("abc-123"), "firmware-abc-123");
        assert_eq!(firmware_file_stem("../bad uuid"), "firmware-bad_uuid");
        assert_eq!(firmware_file_stem("///"), "firmware-unknown");
    }

    #[test]
    fn artifact_suffix_preserves_known_file_suffixes() {
        assert_eq!(
            artifact_suffix_from_url("https://example.com/releases/artifact.tar.gz?token=abc")
                .as_deref(),
            Some(".tar.gz")
        );
        assert_eq!(
            artifact_suffix_from_url("https://example.com/firmware.fw").as_deref(),
            Some(".fw")
        );
    }

    #[test]
    fn artifact_suffix_reads_nerves_hub_proxy_query() {
        let encoded = URL_SAFE_NO_PAD.encode("https://example.com/releases/artifact.tar.zst");
        let proxy_url = format!("https://proxy.example.com/download?firmware={encoded}");

        assert_eq!(
            artifact_suffix_from_url(&proxy_url).as_deref(),
            Some(".tar.zst")
        );
    }

    #[test]
    fn uppercase_hex_encodes_bytes() {
        assert_eq!(uppercase_hex(&[0xab, 0xcd, 0x01]), "ABCD01");
    }

    #[test]
    fn firmware_file_name_uses_artifact_suffix() {
        let mut deployment = deployment("https://example.com/releases/artifact.tar.gz");
        assert_eq!(firmware_file_name(&deployment), "firmware-test-uuid.tar.gz");

        deployment.firmware_url = "https://example.com/download".to_string();
        assert_eq!(firmware_file_name(&deployment), "firmware-test-uuid");
    }

    #[tokio::test]
    async fn download_rejects_checksum_mismatch() {
        let body = b"artifact bytes";
        let firmware_url = spawn_test_firmware_server(body).await;
        let mut deployment = deployment(&firmware_url);
        deployment.checksum = Some("BAD".to_string());
        let dir = tempfile::tempdir().unwrap();

        let err = download_firmware(&deployment, dir.path(), |_, _| {})
            .await
            .unwrap_err();

        assert!(matches!(err, DeploymentError::ChecksumMismatch { .. }));
    }

    fn deployment(firmware_url: &str) -> Deployment {
        Deployment {
            update_available: true,
            firmware_url: firmware_url.to_string(),
            firmware_meta: FirmwareMeta {
                uuid: "test-uuid".to_string(),
                version: "1.0.0".to_string(),
                platform: "x86_64".to_string(),
                architecture: "x86_64".to_string(),
                product: "test".to_string(),
                author: None,
                description: None,
                fwup_version: None,
                vcs_identifier: None,
                misc: None,
                extra: BTreeMap::new(),
            },
            size: None,
            checksum: None,
            partials_checksums: None,
            deployment_id: None,
            extra: BTreeMap::from([("custom".to_string(), json!(true))]),
        }
    }

    async fn spawn_test_firmware_server(body: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });

        format!("http://{addr}/artifact.tar.gz")
    }
}
