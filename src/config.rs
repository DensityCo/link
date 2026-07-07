use crate::device::{DeviceInfo, DeviceRuntimeState, FirmwareMetadata};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("missing required field: {0}")]
    Missing(&'static str),
}

#[derive(Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    Mtls {
        cert_path: PathBuf,
        key_path: PathBuf,
        ca_cert_path: PathBuf,
    },
    SharedSecret {
        key: String,
        secret: String,
    },
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthConfig::Mtls {
                cert_path,
                key_path,
                ca_cert_path,
            } => f
                .debug_struct("Mtls")
                .field("cert_path", cert_path)
                .field("key_path", key_path)
                .field("ca_cert_path", ca_cert_path)
                .finish(),
            AuthConfig::SharedSecret { key, secret: _ } => f
                .debug_struct("SharedSecret")
                .field("key", key)
                .field("secret", &"<redacted>")
                .finish(),
        }
    }
}

impl AuthConfig {
    pub fn endpoint_path(&self) -> &'static str {
        match self {
            AuthConfig::SharedSecret { .. } => "/device-socket/websocket",
            AuthConfig::Mtls { .. } => "/socket/websocket",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub host: String,
    pub auth: AuthConfig,
    pub serial_number: Option<String>,
    pub fwup_devpath: Option<String>,
    pub fwup_task: Option<String>,
    pub fwup_public_keys: Option<Vec<String>>,
    pub firmware: Option<FirmwareMetadata>,
    pub heartbeat_interval_secs: Option<u64>,
    pub data_dir: Option<PathBuf>,
    pub device_api_version: Option<String>,
    pub console_version: Option<String>,
    pub fwup_version: Option<String>,
    pub currently_downloading_uuid: Option<String>,
    pub firmware_validated: Option<bool>,
    pub firmware_auto_revert_detected: Option<bool>,
    pub join_params: Option<BTreeMap<String, Value>>,
    pub console: Option<ConsoleConfig>,
    pub reboot: Option<RebootConfig>,
    pub identify: Option<IdentifyConfig>,
    pub scripts: Option<ScriptsConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsoleConfig {
    pub enabled: Option<bool>,
    pub version: Option<String>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub timeout_secs: Option<u64>,
    pub rows: Option<u16>,
    pub cols: Option<u16>,
}

impl ConsoleConfig {
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    pub fn version(&self) -> &str {
        self.version.as_deref().unwrap_or("2.0.0")
    }

    pub fn command(&self) -> &str {
        self.command.as_deref().unwrap_or("/bin/sh")
    }

    pub fn args(&self) -> &[String] {
        self.args.as_deref().unwrap_or(&[])
    }

    pub fn timeout_secs(&self) -> u64 {
        self.timeout_secs.unwrap_or(5 * 60)
    }

    pub fn rows(&self) -> u16 {
        self.rows.unwrap_or(24).max(1)
    }

    pub fn cols(&self) -> u16 {
        self.cols.unwrap_or(80).max(1)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RebootConfig {
    pub enabled: Option<bool>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub after_firmware_apply: Option<bool>,
    pub on_server_request: Option<bool>,
}

impl RebootConfig {
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    pub fn command(&self) -> &str {
        self.command.as_deref().unwrap_or("reboot")
    }

    pub fn args(&self) -> &[String] {
        self.args.as_deref().unwrap_or(&[])
    }

    pub fn after_firmware_apply(&self) -> bool {
        self.after_firmware_apply.unwrap_or(true)
    }

    pub fn on_server_request(&self) -> bool {
        self.on_server_request.unwrap_or(true)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdentifyConfig {
    pub enabled: Option<bool>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
}

impl IdentifyConfig {
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    pub fn command(&self) -> Option<&str> {
        self.command
            .as_deref()
            .map(str::trim)
            .filter(|command| !command.is_empty())
    }

    pub fn args(&self) -> &[String] {
        self.args.as_deref().unwrap_or(&[])
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScriptsConfig {
    pub enabled: Option<bool>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub timeout_secs: Option<u64>,
}

impl ScriptsConfig {
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }

    pub fn command(&self) -> Option<&str> {
        self.command
            .as_deref()
            .map(str::trim)
            .filter(|command| !command.is_empty())
    }

    pub fn args(&self) -> &[String] {
        self.args.as_deref().unwrap_or(&[])
    }

    pub fn timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.timeout_secs.unwrap_or(10))
    }
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)?;
        content.parse()
    }

    pub fn from_toml(content: &str) -> Result<Self, ConfigError> {
        content.parse()
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.host.is_empty() {
            return Err(ConfigError::Missing("host"));
        }
        if self.identify.as_ref().is_some_and(IdentifyConfig::enabled)
            && self
                .identify
                .as_ref()
                .and_then(IdentifyConfig::command)
                .is_none()
        {
            return Err(ConfigError::Missing("identify.command"));
        }
        if self.scripts.as_ref().is_some_and(ScriptsConfig::enabled)
            && self
                .scripts
                .as_ref()
                .and_then(ScriptsConfig::command)
                .is_none()
        {
            return Err(ConfigError::Missing("scripts.command"));
        }
        Ok(())
    }

    pub fn socket_url(&self) -> String {
        let host = self.normalized_host();
        format!("{}{}?vsn=2.0.0", host, self.auth.endpoint_path())
    }
}

impl std::str::FromStr for Config {
    type Err = ConfigError;

    fn from_str(content: &str) -> Result<Self, Self::Err> {
        let config: Config = toml::from_str(content)?;
        config.validate()?;
        Ok(config)
    }
}

impl Config {
    fn normalized_host(&self) -> String {
        let host = self.host.trim().trim_end_matches('/');
        if let Some(rest) = host.strip_prefix("https://") {
            format!("wss://{}", rest)
        } else if let Some(rest) = host.strip_prefix("http://") {
            format!("ws://{}", rest)
        } else if host.starts_with("ws://") || host.starts_with("wss://") {
            host.to_string()
        } else {
            format!("wss://{}", host)
        }
    }

    pub fn heartbeat_interval_secs(&self) -> u64 {
        self.heartbeat_interval_secs.unwrap_or(30)
    }

    pub fn fwup_devpath(&self) -> &str {
        self.fwup_devpath.as_deref().unwrap_or("/dev/mmcblk0")
    }

    pub fn fwup_task(&self) -> &str {
        self.fwup_task.as_deref().unwrap_or("upgrade")
    }

    pub fn fwup_public_keys(&self) -> &[String] {
        self.fwup_public_keys.as_deref().unwrap_or(&[])
    }

    pub fn data_dir(&self) -> PathBuf {
        self.data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("/tmp/link"))
    }

    pub fn device_api_version(&self) -> &str {
        self.device_api_version.as_deref().unwrap_or("2.3.0")
    }

    pub fn console_version(&self) -> Option<&str> {
        self.console
            .as_ref()
            .filter(|console| console.enabled())
            .map(|console| console.version())
            .or(self.console_version.as_deref())
    }

    pub fn console_enabled(&self) -> bool {
        self.console.as_ref().is_some_and(ConsoleConfig::enabled)
    }

    pub fn reboot_enabled(&self) -> bool {
        self.reboot.as_ref().is_some_and(RebootConfig::enabled)
    }

    pub fn identify_enabled(&self) -> bool {
        self.identify.as_ref().is_some_and(IdentifyConfig::enabled)
    }

    pub fn scripts_enabled(&self) -> bool {
        self.scripts.as_ref().is_some_and(ScriptsConfig::enabled)
    }

    pub fn fwup_version(&self) -> Option<&str> {
        self.fwup_version.as_deref()
    }

    pub fn device_info(&self) -> Result<DeviceInfo, ConfigError> {
        let serial_number = self
            .serial_number
            .as_deref()
            .map(str::trim)
            .filter(|serial| !serial.is_empty())
            .ok_or(ConfigError::Missing("serial_number"))?
            .to_string();

        let firmware = self
            .firmware
            .clone()
            .ok_or(ConfigError::Missing("firmware"))?;

        Ok(DeviceInfo {
            serial_number,
            firmware,
            device_api_version: self.device_api_version().to_string(),
            fwup_version: self.fwup_version.clone(),
            console_version: self.console_version().map(str::to_string),
            runtime_state: DeviceRuntimeState {
                currently_downloading_uuid: self.currently_downloading_uuid.clone(),
                firmware_validated: self.firmware_validated,
                firmware_auto_revert_detected: self.firmware_auto_revert_detected,
            },
            extra_join_params: self.join_params.clone().unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mtls_config() {
        let toml = r#"
host = "https://fleet.fabric.density.ai/"
serial_number = "device-1234"

[auth]
type = "mtls"
cert_path = "/etc/link/cert.pem"
key_path = "/etc/link/key.pem"
ca_cert_path = "/etc/link/ca.pem"

[firmware]
uuid = "aaaa-bbbb"
version = "1.0.0"
platform = "rpi4"
architecture = "arm"
product = "my-product"
"#;
        let config = Config::from_toml(toml).unwrap();
        assert_eq!(config.host, "https://fleet.fabric.density.ai/");
        assert_eq!(
            config.socket_url(),
            "wss://fleet.fabric.density.ai/socket/websocket?vsn=2.0.0"
        );
        assert!(matches!(config.auth, AuthConfig::Mtls { .. }));
        assert_eq!(config.firmware.unwrap().uuid, "aaaa-bbbb");
    }

    #[test]
    fn parse_shared_secret_config() {
        let toml = r#"
host = "https://fleet.fabric.density.ai/"
serial_number = "device-1234"

[auth]
type = "shared_secret"
key = "my-key"
secret = "super-secret"

[firmware]
uuid = "aaaa-bbbb"
version = "1.0.0"
platform = "rpi4"
architecture = "arm"
product = "my-product"
"#;
        let config = Config::from_toml(toml).unwrap();
        assert_eq!(
            config.socket_url(),
            "wss://fleet.fabric.density.ai/device-socket/websocket?vsn=2.0.0"
        );
        assert!(matches!(config.auth, AuthConfig::SharedSecret { .. }));
    }

    #[test]
    fn shared_secret_debug_redacts_secret() {
        let auth = AuthConfig::SharedSecret {
            key: "key-1".to_string(),
            secret: "super-secret".to_string(),
        };
        let debug = format!("{:?}", auth);

        assert!(debug.contains("key-1"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn http_base_url_converts_to_ws_socket_url() {
        let toml = r#"
host = "http://localhost:4000/"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        assert_eq!(
            config.socket_url(),
            "ws://localhost:4000/device-socket/websocket?vsn=2.0.0"
        );
    }

    #[test]
    fn missing_host_fails() {
        let toml = r#"
host = ""
serial_number = "device-1234"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        assert!(Config::from_toml(toml).is_err());
    }

    #[test]
    fn runtime_identity_can_omit_static_serial() {
        let toml = r#"
host = "example.com"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        assert!(config.serial_number.is_none());
    }

    #[test]
    fn defaults() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        assert_eq!(config.heartbeat_interval_secs(), 30);
        assert_eq!(config.fwup_devpath(), "/dev/mmcblk0");
        assert_eq!(config.fwup_task(), "upgrade");
        assert!(config.fwup_public_keys().is_empty());
        assert_eq!(config.device_api_version(), "2.3.0");
        assert!(!config.reboot_enabled());
        assert!(!config.identify_enabled());
        assert!(!config.scripts_enabled());
    }

    #[test]
    fn parses_fwup_public_keys() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"
fwup_public_keys = ["key-1", "key-2"]

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();

        assert_eq!(
            config.fwup_public_keys(),
            &["key-1".to_string(), "key-2".to_string()]
        );
    }

    #[test]
    fn host_with_scheme_is_preserved() {
        let toml = r#"
host = "ws://localhost:4000"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        assert_eq!(
            config.socket_url(),
            "ws://localhost:4000/device-socket/websocket?vsn=2.0.0"
        );
    }

    #[test]
    fn device_info_is_config_backed() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"
device_api_version = "2.3.0"
console_version = "2.0.0"
fwup_version = "1.12.0"
firmware_validated = true

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        let info = config.device_info().unwrap();
        let payload = info.join_payload();

        assert_eq!(info.serial_number, "dev-1");
        assert_eq!(payload["fwup_version"], "1.12.0");
        assert_eq!(payload["console_version"], "2.0.0");
        assert_eq!(payload["meta"]["firmware_validated"], true);
    }

    #[test]
    fn device_info_requires_static_serial() {
        let toml = r#"
host = "example.com"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        let err = config.device_info().unwrap_err();

        assert!(matches!(err, ConfigError::Missing("serial_number")));
    }

    #[test]
    fn console_config_is_opt_in() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[console]
enabled = true
version = "2.0.0"
command = "/bin/sh"
args = ["-l"]
timeout_secs = 60
rows = 30
cols = 100

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        let console = config.console.as_ref().unwrap();

        assert!(config.console_enabled());
        assert_eq!(config.console_version(), Some("2.0.0"));
        assert_eq!(console.command(), "/bin/sh");
        assert_eq!(console.args(), &["-l".to_string()]);
        assert_eq!(console.timeout_secs(), 60);
        assert_eq!(console.rows(), 30);
        assert_eq!(console.cols(), 100);
        assert_eq!(
            config.device_info().unwrap().console_version.as_deref(),
            Some("2.0.0")
        );
    }

    #[test]
    fn disabled_console_config_does_not_enable_console() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[console]
enabled = false

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();

        assert!(!config.console_enabled());
        assert_eq!(config.console_version(), None);
    }

    #[test]
    fn reboot_config_is_opt_in() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[reboot]
enabled = true
command = "/sbin/reboot"
args = ["--force"]
after_firmware_apply = false
on_server_request = true

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        let reboot = config.reboot.as_ref().unwrap();

        assert!(config.reboot_enabled());
        assert_eq!(reboot.command(), "/sbin/reboot");
        assert_eq!(reboot.args(), &["--force".to_string()]);
        assert!(!reboot.after_firmware_apply());
        assert!(reboot.on_server_request());
    }

    #[test]
    fn identify_config_is_opt_in() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[identify]
enabled = true
command = "/usr/bin/identify-device"
args = ["--blink"]

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        let identify = config.identify.as_ref().unwrap();

        assert!(config.identify_enabled());
        assert_eq!(identify.command(), Some("/usr/bin/identify-device"));
        assert_eq!(identify.args(), &["--blink".to_string()]);
    }

    #[test]
    fn enabled_identify_config_requires_command() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[identify]
enabled = true

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let err = Config::from_toml(toml).unwrap_err();

        assert!(matches!(err, ConfigError::Missing("identify.command")));
    }

    #[test]
    fn scripts_config_is_opt_in() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[scripts]
enabled = true
command = "/bin/sh"
args = ["-s"]
timeout_secs = 20

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let config = Config::from_toml(toml).unwrap();
        let scripts = config.scripts.as_ref().unwrap();

        assert!(config.scripts_enabled());
        assert_eq!(scripts.command(), Some("/bin/sh"));
        assert_eq!(scripts.args(), &["-s".to_string()]);
        assert_eq!(scripts.timeout(), std::time::Duration::from_secs(20));
    }

    #[test]
    fn enabled_scripts_config_requires_command() {
        let toml = r#"
host = "example.com"
serial_number = "dev-1"

[auth]
type = "shared_secret"
key = "k"
secret = "s"

[scripts]
enabled = true

[firmware]
uuid = "u"
version = "v"
platform = "p"
architecture = "a"
product = "pr"
"#;
        let err = Config::from_toml(toml).unwrap_err();

        assert!(matches!(err, ConfigError::Missing("scripts.command")));
    }
}
