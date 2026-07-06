use crate::config::{Config, ConfigError};
use crate::connection::{ConnectionLoop, ConnectionParts};
use crate::console::{ConsoleBackend, ConsoleError, PtyConsoleBackend};
use crate::deployment::{self, Deployment, DeploymentManager};
use crate::device::{DeviceInfo, DeviceInfoError, DeviceInfoProvider};
use crate::extensions::{HealthReporter, SystemHealthReporter};
use crate::transport;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("join rejected: {0}")]
    JoinRejected(String),
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("device info error: {0}")]
    DeviceInfo(#[from] DeviceInfoError),
    #[error("device info provider error: {0}")]
    DeviceInfoProvider(String),
    #[error("transport error: {0}")]
    Transport(#[from] transport::TransportError),
    #[error("deployment error: {0}")]
    Deployment(#[from] deployment::DeploymentError),
    #[error("console error: {0}")]
    Console(#[from] ConsoleError),
    #[error("channel closed")]
    ChannelClosed,
}

impl From<crate::outbound::OutboundError> for ClientError {
    fn from(error: crate::outbound::OutboundError) -> Self {
        ClientError::WebSocket(error.to_string())
    }
}

/// Events that the client can emit to the caller.
#[derive(Debug)]
pub enum ClientEvent {
    Connected,
    Joined,
    ExtensionsJoined,
    ConsoleJoined,
    ConsoleStarted,
    ConsoleStopped,
    HealthReported,
    DeploymentAvailable(Deployment),
    FirmwareDownloaded(std::path::PathBuf),
    FirmwareApplied,
    RebootRequested,
    Disconnected(String),
}

/// A device protocol client with platform-agnostic device metadata.
pub struct LinkClient {
    config: Config,
    device_info: DeviceInfo,
    deployment_manager: DeploymentManager,
    health_reporter: Arc<dyn HealthReporter>,
    console_backend: Option<Arc<dyn ConsoleBackend>>,
}

impl LinkClient {
    pub fn new(config: Config) -> Result<Self, ClientError> {
        let device_info = config.device_info()?;
        info!(
            serial = %device_info.serial_number.as_str(),
            "using configured device serial number"
        );
        Self::with_device_info(config, device_info)
    }

    pub fn with_device_info(config: Config, device_info: DeviceInfo) -> Result<Self, ClientError> {
        device_info.validate()?;
        let deployment_manager = DeploymentManager::from_config(&config);
        let console_backend = config
            .console
            .as_ref()
            .filter(|console| console.enabled())
            .map(|console| {
                Arc::new(PtyConsoleBackend::from_config(console)) as Arc<dyn ConsoleBackend>
            });
        Ok(Self {
            config,
            device_info,
            deployment_manager,
            health_reporter: Arc::new(SystemHealthReporter),
            console_backend,
        })
    }

    pub fn from_provider<P>(config: Config, provider: P) -> Result<Self, ClientError>
    where
        P: DeviceInfoProvider,
    {
        let device_info = provider
            .device_info()
            .map_err(|e| ClientError::DeviceInfoProvider(e.to_string()))?;
        Self::with_device_info(config, device_info)
    }

    pub fn set_device_info(&mut self, device_info: DeviceInfo) -> Result<(), ClientError> {
        device_info.validate()?;
        self.device_info = device_info;
        Ok(())
    }

    pub fn set_deployment_manager(&mut self, deployment_manager: DeploymentManager) {
        self.deployment_manager = deployment_manager;
    }

    pub fn with_deployment_manager(mut self, deployment_manager: DeploymentManager) -> Self {
        self.deployment_manager = deployment_manager;
        self
    }

    pub fn set_health_reporter<R>(&mut self, reporter: R)
    where
        R: HealthReporter + 'static,
    {
        self.health_reporter = Arc::new(reporter);
    }

    pub fn with_health_reporter<R>(mut self, reporter: R) -> Self
    where
        R: HealthReporter + 'static,
    {
        self.set_health_reporter(reporter);
        self
    }

    pub fn set_console_backend<B>(&mut self, backend: B)
    where
        B: ConsoleBackend + 'static,
    {
        self.console_backend = Some(Arc::new(backend));
        if self.device_info.console_version.is_none() {
            self.device_info.console_version = Some("2.0.0".to_string());
        }
    }

    pub fn disable_console(&mut self) {
        self.console_backend = None;
    }

    pub fn serial(&self) -> &str {
        &self.device_info.serial_number
    }

    /// Build the join payload with firmware metadata.
    pub fn join_payload(&self) -> serde_json::Value {
        self.device_info.join_payload()
    }

    /// Connect to the server and run the event loop.
    /// Sends events through the returned channel.
    pub async fn run(&self, event_tx: mpsc::Sender<ClientEvent>) -> Result<(), ClientError> {
        ConnectionLoop::new(ConnectionParts {
            config: self.config.clone(),
            serial: self.serial().to_string(),
            join_payload: self.join_payload(),
            deployment_manager: self.deployment_manager.clone(),
            health_reporter: Arc::clone(&self.health_reporter),
            console_backend: self.console_backend.clone(),
        })
        .run(event_tx)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;
    use crate::device::{
        DeviceInfo, DeviceRuntimeState, FirmwareMetadata, StaticDeviceInfoProvider,
    };
    use std::collections::BTreeMap;

    fn test_config() -> Config {
        Config {
            host: "example.com".to_string(),
            auth: AuthConfig::SharedSecret {
                key: "test-key".to_string(),
                secret: "test-secret".to_string(),
            },
            serial_number: Some("test-device-001".to_string()),
            fwup_devpath: None,
            fwup_task: None,
            firmware: Some(FirmwareMetadata {
                uuid: "fw-uuid-123".to_string(),
                version: "1.0.0".to_string(),
                platform: "rpi4".to_string(),
                architecture: "arm".to_string(),
                product: "test-product".to_string(),
            }),
            heartbeat_interval_secs: None,
            data_dir: None,
            device_api_version: None,
            console_version: None,
            fwup_version: None,
            currently_downloading_uuid: None,
            firmware_validated: None,
            firmware_auto_revert_detected: None,
            join_params: None,
            console: None,
        }
    }

    #[test]
    fn client_creation() {
        let client = LinkClient::new(test_config()).unwrap();
        assert_eq!(client.serial(), "test-device-001");
    }

    #[test]
    fn join_payload_contains_metadata() {
        let client = LinkClient::new(test_config()).unwrap();
        let payload = client.join_payload();
        assert_eq!(payload["nerves_fw_uuid"], "fw-uuid-123");
        assert_eq!(payload["nerves_fw_version"], "1.0.0");
        assert_eq!(payload["nerves_fw_platform"], "rpi4");
        assert_eq!(payload["nerves_fw_architecture"], "arm");
        assert_eq!(payload["nerves_fw_product"], "test-product");
        assert_eq!(payload["device_api_version"], "2.3.0");
    }

    #[test]
    fn join_payload_custom_api_version() {
        let mut config = test_config();
        config.device_api_version = Some("2.0.0".to_string());
        let client = LinkClient::new(config).unwrap();
        let payload = client.join_payload();
        assert_eq!(payload["device_api_version"], "2.0.0");
    }

    #[test]
    fn client_can_use_device_info_provider() {
        let mut config = test_config();
        config.serial_number = None;
        let firmware = config.firmware.take().unwrap();

        let info = DeviceInfo {
            serial_number: "provider-device-001".to_string(),
            firmware,
            device_api_version: "2.3.0".to_string(),
            fwup_version: Some("1.12.0".to_string()),
            console_version: Some("2.0.0".to_string()),
            runtime_state: DeviceRuntimeState {
                firmware_validated: Some(true),
                ..DeviceRuntimeState::default()
            },
            extra_join_params: BTreeMap::new(),
        };

        let client =
            LinkClient::from_provider(config, StaticDeviceInfoProvider::new(info)).unwrap();
        let payload = client.join_payload();

        assert_eq!(client.serial(), "provider-device-001");
        assert_eq!(payload["fwup_version"], "1.12.0");
        assert_eq!(payload["meta"]["firmware_validated"], true);
    }

    #[test]
    fn client_can_use_runtime_device_info_directly() {
        let mut config = test_config();
        config.serial_number = None;
        config.firmware = None;

        let info = DeviceInfo {
            serial_number: "runtime-device-001".to_string(),
            firmware: FirmwareMetadata {
                uuid: "runtime-fw".to_string(),
                version: "2.0.0".to_string(),
                platform: "x86_64".to_string(),
                architecture: "x86_64".to_string(),
                product: "runtime-product".to_string(),
            },
            device_api_version: "2.3.0".to_string(),
            fwup_version: Some("1.13.0".to_string()),
            console_version: None,
            runtime_state: DeviceRuntimeState {
                currently_downloading_uuid: Some("download-123".to_string()),
                firmware_validated: Some(false),
                firmware_auto_revert_detected: Some(true),
            },
            extra_join_params: BTreeMap::new(),
        };

        let client = LinkClient::with_device_info(config, info).unwrap();
        let payload = client.join_payload();

        assert_eq!(client.serial(), "runtime-device-001");
        assert_eq!(payload["nerves_fw_uuid"], "runtime-fw");
        assert_eq!(payload["fwup_version"], "1.13.0");
        assert_eq!(payload["currently_downloading_uuid"], "download-123");
        assert_eq!(payload["meta"]["firmware_auto_revert_detected"], true);
    }

    #[test]
    fn client_can_update_runtime_device_info() {
        let mut client = LinkClient::new(test_config()).unwrap();

        let info = DeviceInfo {
            serial_number: "updated-runtime-device".to_string(),
            firmware: FirmwareMetadata {
                uuid: "updated-fw".to_string(),
                version: "3.0.0".to_string(),
                platform: "rpi5".to_string(),
                architecture: "aarch64".to_string(),
                product: "updated-product".to_string(),
            },
            device_api_version: "2.3.0".to_string(),
            fwup_version: None,
            console_version: None,
            runtime_state: DeviceRuntimeState::default(),
            extra_join_params: BTreeMap::new(),
        };

        client.set_device_info(info).unwrap();

        let payload = client.join_payload();
        assert_eq!(client.serial(), "updated-runtime-device");
        assert_eq!(payload["nerves_fw_uuid"], "updated-fw");
        assert_eq!(payload["nerves_fw_platform"], "rpi5");
    }
}
