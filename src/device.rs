use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::error;

pub const DEFAULT_DEVICE_INFO_COMMAND_TIMEOUT_SECS: u64 = 5;

#[derive(Debug, Error)]
pub enum DeviceInfoError {
    #[error("missing required field: {0}")]
    Missing(&'static str),
}

#[derive(Debug, Error)]
pub enum DeviceInfoSourceError {
    #[error("failed to read device info JSON file {path:?}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse device info JSON file {path:?}: {source}")]
    ParseFile {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("invalid device info JSON file {path:?}: {source}")]
    InvalidFile {
        path: PathBuf,
        source: DeviceInfoError,
    },
    #[error("failed to run device info command `{command}`: {source}")]
    RunCommand {
        command: String,
        source: std::io::Error,
    },
    #[error("device info command `{command}` failed with {status}: {stderr}")]
    CommandFailed {
        command: String,
        status: ExitStatus,
        stderr: String,
    },
    #[error("device info command `{command}` timed out after {timeout:?}")]
    CommandTimedOut { command: String, timeout: Duration },
    #[error("failed to parse device info JSON from command `{command}`: {source}")]
    ParseCommandOutput {
        command: String,
        source: serde_json::Error,
    },
    #[error("invalid device info from command `{command}`: {source}")]
    InvalidCommandOutput {
        command: String,
        source: DeviceInfoError,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirmwareMetadata {
    pub uuid: String,
    pub version: String,
    pub platform: String,
    pub architecture: String,
    pub product: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeviceRuntimeState {
    pub currently_downloading_uuid: Option<String>,
    pub firmware_validated: Option<bool>,
    pub firmware_auto_revert_detected: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub serial_number: String,
    pub firmware: FirmwareMetadata,
    pub device_api_version: String,
    pub fwup_version: Option<String>,
    pub console_version: Option<String>,
    #[serde(default)]
    pub runtime_state: DeviceRuntimeState,
    #[serde(default)]
    pub extra_join_params: BTreeMap<String, Value>,
}

impl DeviceInfo {
    pub fn validate(&self) -> Result<(), DeviceInfoError> {
        if self.serial_number.trim().is_empty() {
            return Err(DeviceInfoError::Missing("serial_number"));
        }
        if self.firmware.uuid.trim().is_empty() {
            return Err(DeviceInfoError::Missing("firmware.uuid"));
        }
        if self.firmware.version.trim().is_empty() {
            return Err(DeviceInfoError::Missing("firmware.version"));
        }
        if self.firmware.platform.trim().is_empty() {
            return Err(DeviceInfoError::Missing("firmware.platform"));
        }
        if self.firmware.architecture.trim().is_empty() {
            return Err(DeviceInfoError::Missing("firmware.architecture"));
        }
        if self.firmware.product.trim().is_empty() {
            return Err(DeviceInfoError::Missing("firmware.product"));
        }
        Ok(())
    }

    pub fn join_payload(&self) -> Value {
        let mut payload = Map::new();

        payload.insert(
            "device_api_version".to_string(),
            Value::String(self.device_api_version.clone()),
        );

        if let Some(fwup_version) = &self.fwup_version {
            payload.insert(
                "fwup_version".to_string(),
                Value::String(fwup_version.clone()),
            );
        }

        if let Some(console_version) = &self.console_version {
            payload.insert(
                "console_version".to_string(),
                Value::String(console_version.clone()),
            );
        }

        // These are NervesHub protocol keys, not an instruction to read Nerves KV.
        payload.insert(
            "nerves_fw_uuid".to_string(),
            Value::String(self.firmware.uuid.clone()),
        );
        payload.insert(
            "nerves_fw_version".to_string(),
            Value::String(self.firmware.version.clone()),
        );
        payload.insert(
            "nerves_fw_platform".to_string(),
            Value::String(self.firmware.platform.clone()),
        );
        payload.insert(
            "nerves_fw_architecture".to_string(),
            Value::String(self.firmware.architecture.clone()),
        );
        payload.insert(
            "nerves_fw_product".to_string(),
            Value::String(self.firmware.product.clone()),
        );

        for (key, value) in &self.extra_join_params {
            payload.insert(key.clone(), value.clone());
        }

        if let Some(uuid) = &self.runtime_state.currently_downloading_uuid {
            payload.insert(
                "currently_downloading_uuid".to_string(),
                Value::String(uuid.clone()),
            );
        }

        let mut meta = Map::new();
        if let Some(value) = self.runtime_state.firmware_auto_revert_detected {
            meta.insert(
                "firmware_auto_revert_detected".to_string(),
                Value::Bool(value),
            );
        }
        if let Some(value) = self.runtime_state.firmware_validated {
            meta.insert("firmware_validated".to_string(), Value::Bool(value));
        }
        if !meta.is_empty() {
            payload.insert("meta".to_string(), Value::Object(meta));
        }

        Value::Object(payload)
    }
}

pub trait DeviceInfoProvider: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn device_info(&self) -> Result<DeviceInfo, Self::Error>;
}

impl<F, E> DeviceInfoProvider for F
where
    F: Fn() -> Result<DeviceInfo, E> + Send + Sync,
    E: std::error::Error + Send + Sync + 'static,
{
    type Error = E;

    fn device_info(&self) -> Result<DeviceInfo, Self::Error> {
        self()
    }
}

pub(crate) type DeviceInfoProviderError = Box<dyn std::error::Error + Send + Sync>;

pub(crate) trait DynDeviceInfoProvider: Send + Sync {
    fn device_info(&self) -> Result<DeviceInfo, DeviceInfoProviderError>;
}

impl<P> DynDeviceInfoProvider for P
where
    P: DeviceInfoProvider + 'static,
{
    fn device_info(&self) -> Result<DeviceInfo, DeviceInfoProviderError> {
        DeviceInfoProvider::device_info(self)
            .map_err(|error| Box::new(error) as DeviceInfoProviderError)
    }
}

#[derive(Debug, Clone)]
pub struct JsonFileDeviceInfoProvider {
    path: PathBuf,
}

impl JsonFileDeviceInfoProvider {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DeviceInfoProvider for JsonFileDeviceInfoProvider {
    type Error = DeviceInfoSourceError;

    fn device_info(&self) -> Result<DeviceInfo, Self::Error> {
        let content = std::fs::read_to_string(&self.path).map_err(|source| {
            error!(
                path = %self.path.display(),
                error = %source,
                "failed to read device info JSON file"
            );
            DeviceInfoSourceError::ReadFile {
                path: self.path.clone(),
                source,
            }
        })?;
        let info: DeviceInfo = serde_json::from_str(&content).map_err(|source| {
            error!(
                path = %self.path.display(),
                error = %source,
                "failed to parse device info JSON file"
            );
            DeviceInfoSourceError::ParseFile {
                path: self.path.clone(),
                source,
            }
        })?;
        info.validate().map_err(|source| {
            error!(
                path = %self.path.display(),
                error = %source,
                "device info JSON file is missing required metadata"
            );
            DeviceInfoSourceError::InvalidFile {
                path: self.path.clone(),
                source,
            }
        })?;
        Ok(info)
    }
}

#[derive(Debug, Clone)]
pub struct CommandDeviceInfoProvider {
    command: String,
    args: Vec<String>,
    timeout: Duration,
}

impl CommandDeviceInfoProvider {
    pub fn new(command: impl Into<String>) -> Self {
        Self::with_args_and_timeout(command, Vec::new(), default_device_info_command_timeout())
    }

    pub fn with_args(command: impl Into<String>, args: impl Into<Vec<String>>) -> Self {
        Self::with_args_and_timeout(command, args, default_device_info_command_timeout())
    }

    pub fn with_timeout(command: impl Into<String>, timeout: Duration) -> Self {
        Self::with_args_and_timeout(command, Vec::new(), timeout)
    }

    pub fn with_args_and_timeout(
        command: impl Into<String>,
        args: impl Into<Vec<String>>,
        timeout: Duration,
    ) -> Self {
        Self {
            command: command.into(),
            args: args.into(),
            timeout: normalize_device_info_command_timeout(timeout),
        }
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    fn run_command(&self) -> Result<Output, DeviceInfoSourceError> {
        let mut child = Command::new(&self.command)
            .args(&self.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|source| {
                error!(
                    command = %self.command,
                    args = ?self.args,
                    error = %source,
                    "failed to run device info command"
                );
                DeviceInfoSourceError::RunCommand {
                    command: self.command.clone(),
                    source,
                }
            })?;

        let started_at = Instant::now();
        loop {
            match child.try_wait() {
                Ok(Some(_status)) => {
                    return child.wait_with_output().map_err(|source| {
                        error!(
                            command = %self.command,
                            args = ?self.args,
                            error = %source,
                            "failed to read device info command output"
                        );
                        DeviceInfoSourceError::RunCommand {
                            command: self.command.clone(),
                            source,
                        }
                    });
                }
                Ok(None) => {
                    if started_at.elapsed() >= self.timeout {
                        if let Err(source) = child.kill() {
                            error!(
                                command = %self.command,
                                args = ?self.args,
                                error = %source,
                                "failed to kill timed out device info command"
                            );
                        }
                        if let Err(source) = child.wait() {
                            error!(
                                command = %self.command,
                                args = ?self.args,
                                error = %source,
                                "failed to reap timed out device info command"
                            );
                        }
                        error!(
                            command = %self.command,
                            args = ?self.args,
                            timeout_secs = self.timeout.as_secs_f64(),
                            "device info command timed out"
                        );
                        return Err(DeviceInfoSourceError::CommandTimedOut {
                            command: self.command.clone(),
                            timeout: self.timeout,
                        });
                    }

                    let remaining = self.timeout.saturating_sub(started_at.elapsed());
                    std::thread::sleep(remaining.min(Duration::from_millis(10)));
                }
                Err(source) => {
                    error!(
                        command = %self.command,
                        args = ?self.args,
                        error = %source,
                        "failed to wait for device info command"
                    );
                    return Err(DeviceInfoSourceError::RunCommand {
                        command: self.command.clone(),
                        source,
                    });
                }
            }
        }
    }
}

impl DeviceInfoProvider for CommandDeviceInfoProvider {
    type Error = DeviceInfoSourceError;

    fn device_info(&self) -> Result<DeviceInfo, Self::Error> {
        let output = self.run_command()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            error!(
                command = %self.command,
                args = ?self.args,
                status = %output.status,
                stderr = %stderr,
                "device info command failed"
            );
            return Err(DeviceInfoSourceError::CommandFailed {
                command: self.command.clone(),
                status: output.status,
                stderr,
            });
        }

        let info: DeviceInfo = serde_json::from_slice(&output.stdout).map_err(|source| {
            error!(
                command = %self.command,
                args = ?self.args,
                error = %source,
                "failed to parse device info command output"
            );
            DeviceInfoSourceError::ParseCommandOutput {
                command: self.command.clone(),
                source,
            }
        })?;
        info.validate().map_err(|source| {
            error!(
                command = %self.command,
                args = ?self.args,
                error = %source,
                "device info command returned missing required metadata"
            );
            DeviceInfoSourceError::InvalidCommandOutput {
                command: self.command.clone(),
                source,
            }
        })?;
        Ok(info)
    }
}

fn default_device_info_command_timeout() -> Duration {
    Duration::from_secs(DEFAULT_DEVICE_INFO_COMMAND_TIMEOUT_SECS)
}

fn normalize_device_info_command_timeout(timeout: Duration) -> Duration {
    timeout.max(Duration::from_millis(1))
}

#[derive(Debug, Clone)]
pub enum DeviceInfoSourceProvider {
    JsonFile(JsonFileDeviceInfoProvider),
    Command(CommandDeviceInfoProvider),
}

impl DeviceInfoProvider for DeviceInfoSourceProvider {
    type Error = DeviceInfoSourceError;

    fn device_info(&self) -> Result<DeviceInfo, Self::Error> {
        match self {
            DeviceInfoSourceProvider::JsonFile(provider) => {
                DeviceInfoProvider::device_info(provider)
            }
            DeviceInfoSourceProvider::Command(provider) => {
                DeviceInfoProvider::device_info(provider)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct StaticDeviceInfoProvider {
    info: DeviceInfo,
}

impl StaticDeviceInfoProvider {
    pub fn new(info: DeviceInfo) -> Self {
        Self { info }
    }
}

impl DeviceInfoProvider for StaticDeviceInfoProvider {
    type Error = DeviceInfoError;

    fn device_info(&self) -> Result<DeviceInfo, Self::Error> {
        self.info.validate()?;
        Ok(self.info.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn device_info_json(version: &str) -> String {
        json!({
            "serial_number": "device-001",
            "firmware": {
                "uuid": "fw-uuid",
                "version": version,
                "platform": "rpi4",
                "architecture": "arm",
                "product": "test-product"
            },
            "device_api_version": "2.3.0"
        })
        .to_string()
    }

    #[test]
    fn json_file_provider_reads_device_info() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("device-info.json");
        std::fs::write(&path, device_info_json("1.2.3")).unwrap();

        let provider = JsonFileDeviceInfoProvider::new(&path);
        let info = DeviceInfoProvider::device_info(&provider).unwrap();

        assert_eq!(info.serial_number, "device-001");
        assert_eq!(info.firmware.version, "1.2.3");
        assert!(info.extra_join_params.is_empty());
    }

    #[test]
    fn command_provider_reads_device_info_from_stdout() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("device-info.json");
        std::fs::write(&path, device_info_json("2.3.4")).unwrap();

        let provider =
            CommandDeviceInfoProvider::with_args("/bin/cat", vec![path.display().to_string()]);
        let info = DeviceInfoProvider::device_info(&provider).unwrap();

        assert_eq!(info.serial_number, "device-001");
        assert_eq!(info.firmware.version, "2.3.4");
    }

    #[test]
    fn command_provider_times_out() {
        let provider = CommandDeviceInfoProvider::with_args_and_timeout(
            "/bin/sh",
            vec!["-c".to_string(), "sleep 2".to_string()],
            Duration::from_millis(50),
        );

        let err = DeviceInfoProvider::device_info(&provider).unwrap_err();

        assert!(matches!(err, DeviceInfoSourceError::CommandTimedOut { .. }));
    }
}
