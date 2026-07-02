use crate::update::UpdateError;
use std::path::Path;
use tracing::{info, warn};

/// Apply firmware using the fwup CLI tool.
pub async fn apply_firmware(
    firmware_path: &Path,
    devpath: &str,
    task: &str,
) -> Result<(), UpdateError> {
    info!(
        firmware = %firmware_path.display(),
        devpath,
        task,
        "applying firmware with fwup"
    );

    let output = tokio::process::Command::new("fwup")
        .arg("-a")
        .arg("-d")
        .arg(devpath)
        .arg("-i")
        .arg(firmware_path)
        .arg("-t")
        .arg(task)
        .output()
        .await
        .map_err(|e| UpdateError::Fwup(format!("failed to execute fwup: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!(stderr = %stderr, "fwup failed");
        return Err(UpdateError::Fwup(format!(
            "fwup exit {}: {}",
            output.status,
            stderr.trim()
        )));
    }

    info!("firmware applied successfully");
    Ok(())
}
