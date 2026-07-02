use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeviceInfoError {
    #[error("missing required field: {0}")]
    Missing(&'static str),
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

#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub serial_number: String,
    pub firmware: FirmwareMetadata,
    pub device_api_version: String,
    pub fwup_version: Option<String>,
    pub console_version: Option<String>,
    pub runtime_state: DeviceRuntimeState,
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

pub trait DeviceInfoProvider {
    type Error: std::error::Error + Send + Sync + 'static;

    fn device_info(&self) -> Result<DeviceInfo, Self::Error>;
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
