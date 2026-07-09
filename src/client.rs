use crate::alarms::{AlarmStore, FIRMWARE_REVERTED_ALARM};
use crate::config::{Config, ConfigError};
use crate::connection::{ConnectionLoop, ConnectionParts};
use crate::console::{ConsoleBackend, ConsoleError, PtyConsoleBackend};
use crate::deployment::{self, Deployment, DeploymentManager};
use crate::device::{DeviceInfo, DeviceInfoError, DeviceInfoProvider, DynDeviceInfoProvider};
use crate::extensions::{HealthReporter, SystemHealthReporter};
use crate::identify::{IdentifyAction, IdentifyController, IdentifyError};
use crate::reboot::{RebootController, RebootError, Rebooter};
use crate::scripts::{ScriptController, ScriptRunner};
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
    #[error("reboot error: {0}")]
    Reboot(#[from] RebootError),
    #[error("identify error: {0}")]
    Identify(#[from] IdentifyError),
    #[error("channel closed")]
    ChannelClosed,
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
    DeploymentAvailable(Box<Deployment>),
    FirmwareDownloaded(std::path::PathBuf),
    FirmwareApplied,
    RebootRequested,
    IdentifyRequested,
    ScriptRequested(String),
    ScriptCompleted(String),
    ScriptFailed { script_ref: String, reason: String },
    Disconnected(String),
}

impl ClientError {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ClientError::Config(_)
                | ClientError::DeviceInfo(_)
                | ClientError::DeviceInfoProvider(_)
        )
    }
}

/// A device protocol client with platform-agnostic device metadata.
pub struct LinkClient {
    config: Config,
    device_info: DeviceInfo,
    deployment_manager: DeploymentManager,
    health_reporter: Arc<dyn HealthReporter>,
    console_backend: Option<Arc<dyn ConsoleBackend>>,
    reboot_controller: RebootController,
    identify_controller: IdentifyController,
    script_controller: ScriptController,
    alarm_store: AlarmStore,
    device_info_provider: Option<Arc<dyn DynDeviceInfoProvider>>,
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
        let deployment_manager = DeploymentManager::from_config(&config);
        let reboot_controller = RebootController::from_config(config.reboot.as_ref());
        let identify_controller = IdentifyController::from_config(config.identify.as_ref());
        let script_controller = ScriptController::from_config(config.scripts.as_ref());
        let console_backend = config
            .console
            .as_ref()
            .filter(|console| console.enabled())
            .map(|console| {
                Arc::new(PtyConsoleBackend::from_config(console)) as Arc<dyn ConsoleBackend>
            });
        let device_info = with_console_version(device_info, console_backend.is_some());
        device_info.validate()?;
        let alarm_store = AlarmStore::default();
        sync_runtime_alarms(&alarm_store, &device_info);
        Ok(Self {
            config,
            device_info,
            deployment_manager,
            health_reporter: Arc::new(SystemHealthReporter),
            console_backend,
            reboot_controller,
            identify_controller,
            script_controller,
            alarm_store,
            device_info_provider: None,
        })
    }

    pub fn from_provider<P>(config: Config, provider: P) -> Result<Self, ClientError>
    where
        P: DeviceInfoProvider + 'static,
    {
        let provider: Arc<dyn DynDeviceInfoProvider> = Arc::new(provider);
        let device_info = load_provider_device_info(&provider)?;
        let mut client = Self::with_device_info(config, device_info)?;
        client.device_info_provider = Some(provider);
        Ok(client)
    }

    pub fn set_device_info(&mut self, device_info: DeviceInfo) -> Result<(), ClientError> {
        let device_info = with_console_version(device_info, self.console_backend.is_some());
        device_info.validate()?;
        sync_runtime_alarms(&self.alarm_store, &device_info);
        self.device_info = device_info;
        self.device_info_provider = None;
        Ok(())
    }

    pub fn set_device_info_provider<P>(&mut self, provider: P) -> Result<(), ClientError>
    where
        P: DeviceInfoProvider + 'static,
    {
        let provider: Arc<dyn DynDeviceInfoProvider> = Arc::new(provider);
        let device_info = with_console_version(
            load_provider_device_info(&provider)?,
            self.console_backend.is_some(),
        );
        device_info.validate()?;
        sync_runtime_alarms(&self.alarm_store, &device_info);
        self.device_info = device_info;
        self.device_info_provider = Some(provider);
        Ok(())
    }

    pub fn with_device_info_provider<P>(mut self, provider: P) -> Result<Self, ClientError>
    where
        P: DeviceInfoProvider + 'static,
    {
        self.set_device_info_provider(provider)?;
        Ok(self)
    }

    pub fn current_device_info(&self) -> Result<DeviceInfo, ClientError> {
        let device_info = match &self.device_info_provider {
            Some(provider) => load_provider_device_info(provider)?,
            None => self.device_info.clone(),
        };
        let device_info = with_console_version(device_info, self.console_backend.is_some());
        device_info.validate()?;
        sync_runtime_alarms(&self.alarm_store, &device_info);
        Ok(device_info)
    }

    pub fn current_join_payload(&self) -> Result<serde_json::Value, ClientError> {
        Ok(self.current_device_info()?.join_payload())
    }

    pub fn disable_device_info_provider(&mut self) {
        self.device_info_provider = None;
    }

    pub fn refresh_device_info(&mut self) -> Result<(), ClientError> {
        let Some(provider) = &self.device_info_provider else {
            return Ok(());
        };
        let device_info = with_console_version(
            load_provider_device_info(provider)?,
            self.console_backend.is_some(),
        );
        device_info.validate()?;
        sync_runtime_alarms(&self.alarm_store, &device_info);
        self.device_info = device_info;
        Ok(())
    }

    pub fn alarm_store(&self) -> AlarmStore {
        self.alarm_store.clone()
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

    pub fn set_rebooter<R>(&mut self, rebooter: R)
    where
        R: Rebooter + 'static,
    {
        self.reboot_controller.set_rebooter(rebooter);
    }

    pub fn with_rebooter<R>(mut self, rebooter: R) -> Self
    where
        R: Rebooter + 'static,
    {
        self.set_rebooter(rebooter);
        self
    }

    pub fn disable_reboot(&mut self) {
        self.reboot_controller.disable();
    }

    pub fn set_identify_action<A>(&mut self, action: A)
    where
        A: IdentifyAction + 'static,
    {
        self.identify_controller.set_action(action);
    }

    pub fn with_identify_action<A>(mut self, action: A) -> Self
    where
        A: IdentifyAction + 'static,
    {
        self.set_identify_action(action);
        self
    }

    pub fn disable_identify(&mut self) {
        self.identify_controller.disable();
    }

    pub fn set_script_runner<R>(&mut self, runner: R)
    where
        R: ScriptRunner + 'static,
    {
        self.script_controller.set_runner(runner);
    }

    pub fn with_script_runner<R>(mut self, runner: R) -> Self
    where
        R: ScriptRunner + 'static,
    {
        self.set_script_runner(runner);
        self
    }

    pub fn disable_scripts(&mut self) {
        self.script_controller.disable();
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
        let device_info = self.current_device_info()?;

        ConnectionLoop::new(ConnectionParts {
            config: self.config.clone(),
            serial: device_info.serial_number.clone(),
            join_payload: device_info.join_payload(),
            deployment_manager: self.deployment_manager.clone(),
            health_reporter: Arc::clone(&self.health_reporter),
            console_backend: self.console_backend.clone(),
            reboot_controller: self.reboot_controller.clone(),
            identify_controller: self.identify_controller.clone(),
            script_controller: self.script_controller.clone(),
            alarm_store: self.alarm_store.clone(),
        })
        .run(event_tx)
        .await
    }
}

fn load_provider_device_info(
    provider: &Arc<dyn DynDeviceInfoProvider>,
) -> Result<DeviceInfo, ClientError> {
    let device_info = provider
        .device_info()
        .map_err(|e| ClientError::DeviceInfoProvider(e.to_string()))?;
    device_info.validate()?;
    Ok(device_info)
}

fn with_console_version(mut device_info: DeviceInfo, console_enabled: bool) -> DeviceInfo {
    if console_enabled && device_info.console_version.is_none() {
        device_info.console_version = Some("2.0.0".to_string());
    }

    device_info
}

fn sync_runtime_alarms(alarm_store: &AlarmStore, device_info: &DeviceInfo) {
    if device_info
        .runtime_state
        .firmware_auto_revert_detected
        .unwrap_or(false)
    {
        alarm_store.set(FIRMWARE_REVERTED_ALARM, "firmware auto revert was detected");
    } else {
        alarm_store.clear(FIRMWARE_REVERTED_ALARM);
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn test_config() -> Config {
        Config {
            host: "example.com".to_string(),
            auth: AuthConfig::SharedSecret {
                key: "test-key".to_string(),
                secret: "test-secret".to_string(),
            },
            device_info: None,
            serial_number: Some("test-device-001".to_string()),
            fwup_devpath: None,
            fwup_task: None,
            fwup_public_keys: None,
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
            reboot: None,
            identify: None,
            scripts: None,
        }
    }

    fn provider_info(version: impl Into<String>) -> DeviceInfo {
        DeviceInfo {
            serial_number: "provider-device-001".to_string(),
            firmware: FirmwareMetadata {
                uuid: "provider-fw".to_string(),
                version: version.into(),
                platform: "x86_64".to_string(),
                architecture: "x86_64".to_string(),
                product: "provider-product".to_string(),
            },
            device_api_version: "2.3.0".to_string(),
            fwup_version: Some("1.13.0".to_string()),
            console_version: None,
            runtime_state: DeviceRuntimeState::default(),
            extra_join_params: BTreeMap::new(),
        }
    }

    #[test]
    fn client_creation() {
        let client = LinkClient::new(test_config()).unwrap();
        assert_eq!(client.serial(), "test-device-001");
    }

    #[test]
    fn client_alarm_store_sets_gets_lists_and_clears() {
        let client = LinkClient::new(test_config()).unwrap();
        let alarms = client.alarm_store();

        alarms.set("link.test", "test alarm");

        assert_eq!(alarms.get("link.test").as_deref(), Some("test alarm"));
        assert_eq!(
            alarms.list().get("link.test").map(String::as_str),
            Some("test alarm")
        );
        assert_eq!(
            client.alarm_store().get("link.test").as_deref(),
            Some("test alarm")
        );

        alarms.clear("link.test");

        assert_eq!(alarms.get("link.test"), None);
        assert!(!alarms.list().contains_key("link.test"));
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
    fn current_device_info_refreshes_provider_each_time() {
        let mut config = test_config();
        config.serial_number = None;
        config.firmware = None;

        let calls = Arc::new(AtomicUsize::new(0));
        let provider = {
            let calls = Arc::clone(&calls);
            move || {
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, DeviceInfoError>(provider_info(format!("2.0.{call}")))
            }
        };

        let client = LinkClient::from_provider(config, provider).unwrap();

        assert_eq!(client.join_payload()["nerves_fw_version"], "2.0.1");
        assert_eq!(
            client.current_device_info().unwrap().firmware.version,
            "2.0.2"
        );
        assert_eq!(
            client.current_join_payload().unwrap()["nerves_fw_version"],
            "2.0.3"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn refresh_device_info_updates_cached_payload_from_provider() {
        let mut config = test_config();
        config.serial_number = None;
        config.firmware = None;

        let calls = Arc::new(AtomicUsize::new(0));
        let provider = {
            let calls = Arc::clone(&calls);
            move || {
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok::<_, DeviceInfoError>(provider_info(format!("3.0.{call}")))
            }
        };

        let mut client = LinkClient::from_provider(config, provider).unwrap();
        client.refresh_device_info().unwrap();

        assert_eq!(client.join_payload()["nerves_fw_version"], "3.0.2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn set_device_info_replaces_provider_with_static_info() {
        let mut config = test_config();
        config.serial_number = None;
        config.firmware = None;

        let calls = Arc::new(AtomicUsize::new(0));
        let provider = {
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, DeviceInfoError>(provider_info("4.0.0"))
            }
        };

        let mut client = LinkClient::from_provider(config, provider).unwrap();
        client.set_device_info(provider_info("manual")).unwrap();

        assert_eq!(
            client.current_device_info().unwrap().firmware.version,
            "manual"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
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

    #[test]
    fn runtime_device_info_updates_firmware_reverted_alarm() {
        let mut client = LinkClient::new(test_config()).unwrap();
        assert!(!client.alarm_store().is_set(FIRMWARE_REVERTED_ALARM));

        let mut info = DeviceInfo {
            serial_number: "runtime-device-001".to_string(),
            firmware: FirmwareMetadata {
                uuid: "runtime-fw".to_string(),
                version: "2.0.0".to_string(),
                platform: "x86_64".to_string(),
                architecture: "x86_64".to_string(),
                product: "runtime-product".to_string(),
            },
            device_api_version: "2.3.0".to_string(),
            fwup_version: None,
            console_version: None,
            runtime_state: DeviceRuntimeState {
                firmware_auto_revert_detected: Some(true),
                ..DeviceRuntimeState::default()
            },
            extra_join_params: BTreeMap::new(),
        };

        client.set_device_info(info.clone()).unwrap();
        assert!(client.alarm_store().is_set(FIRMWARE_REVERTED_ALARM));

        info.runtime_state.firmware_auto_revert_detected = Some(false);
        client.set_device_info(info).unwrap();
        assert!(!client.alarm_store().is_set(FIRMWARE_REVERTED_ALARM));
    }
}
