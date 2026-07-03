use crate::deployment::{Deployment, DeploymentError};
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
    let file_stem = firmware_file_stem(&deployment.firmware_meta.uuid);
    let dest_path = dest_dir.join(format!("{}.fw", file_stem));
    let temp_path = dest_dir.join(format!("{}.fw.tmp", file_stem));
    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(DeploymentError::Io)?;

    let mut downloaded: u64 = 0;
    let mut stream = response.bytes_stream();

    use futures_util::StreamExt;
    use tokio::io::AsyncWriteExt;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| DeploymentError::Download(e.to_string()))?;
        file.write_all(&chunk).await.map_err(DeploymentError::Io)?;
        downloaded += chunk.len() as u64;
        on_progress(downloaded, total_size);
    }

    file.flush().await.map_err(DeploymentError::Io)?;
    drop(file);
    let _ = tokio::fs::remove_file(&dest_path).await;
    tokio::fs::rename(&temp_path, &dest_path)
        .await
        .map_err(DeploymentError::Io)?;
    info!(downloaded_bytes = downloaded, path = %dest_path.display(), "firmware download complete");

    Ok(dest_path)
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
}
