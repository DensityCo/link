use crate::deployment::DeploymentError;
use std::ffi::OsString;
use std::path::Path;
use tracing::{info, warn};

/// Apply firmware using the fwup CLI tool.
pub async fn apply_firmware(
    firmware_path: &Path,
    devpath: &str,
    task: &str,
    public_keys: &[String],
) -> Result<(), DeploymentError> {
    info!(
        firmware = %firmware_path.display(),
        devpath,
        task,
        "applying firmware with fwup"
    );

    let output = tokio::process::Command::new("fwup")
        .args(fwup_args(firmware_path, devpath, task, public_keys))
        .output()
        .await
        .map_err(|e| DeploymentError::Fwup(format!("failed to execute fwup: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!(stderr = %stderr, "fwup failed");
        return Err(DeploymentError::Fwup(format!(
            "fwup exit {}: {}",
            output.status,
            stderr.trim()
        )));
    }

    info!("firmware applied successfully");
    Ok(())
}

fn fwup_args(
    firmware_path: &Path,
    devpath: &str,
    task: &str,
    public_keys: &[String],
) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("-a"),
        OsString::from("-d"),
        OsString::from(devpath),
        OsString::from("-i"),
        firmware_path.as_os_str().to_os_string(),
        OsString::from("-t"),
        OsString::from(task),
    ];

    for key in public_keys {
        args.push(OsString::from("--public-key"));
        args.push(OsString::from(key));
    }

    args
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_as_strings(args: Vec<OsString>) -> Vec<String> {
        args.into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn fwup_args_include_public_keys() {
        let keys = vec!["key-1".to_string(), "key-2".to_string()];
        let args = args_as_strings(fwup_args(
            Path::new("/tmp/firmware.fw"),
            "/dev/mmcblk0",
            "upgrade",
            &keys,
        ));

        assert_eq!(
            args,
            vec![
                "-a",
                "-d",
                "/dev/mmcblk0",
                "-i",
                "/tmp/firmware.fw",
                "-t",
                "upgrade",
                "--public-key",
                "key-1",
                "--public-key",
                "key-2",
            ]
        );
    }

    #[test]
    fn fwup_args_omit_public_key_options_without_keys() {
        let args = args_as_strings(fwup_args(
            Path::new("/tmp/firmware.fw"),
            "/dev/mmcblk0",
            "upgrade",
            &[],
        ));

        assert!(!args.iter().any(|arg| arg == "--public-key"));
    }
}
