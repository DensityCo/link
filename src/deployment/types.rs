use crate::deployment::DeploymentError;
use serde::Deserialize;
use serde_json::Value;

/// Parsed deployment request from the server's "update" event.
#[derive(Debug, Clone, Deserialize)]
pub struct Deployment {
    pub firmware_url: String,
    pub firmware_meta: FirmwareMeta,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct FirmwareMeta {
    pub uuid: String,
    pub version: String,
    pub platform: String,
    pub architecture: String,
    pub product: String,
}

impl Deployment {
    /// Parse a deployment message payload.
    pub fn from_payload(payload: &Value) -> Result<Self, DeploymentError> {
        serde_json::from_value(payload.clone())
            .map_err(|e| DeploymentError::InvalidMessage(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_deployment() {
        let payload = json!({
            "firmware_url": "https://s3.example.com/fw.fw?token=abc",
            "firmware_meta": {
                "uuid": "abc-123",
                "version": "1.1.0",
                "platform": "rpi4",
                "architecture": "arm",
                "product": "my-product"
            }
        });
        let deployment = Deployment::from_payload(&payload).unwrap();
        assert_eq!(
            deployment.firmware_url,
            "https://s3.example.com/fw.fw?token=abc"
        );
        assert_eq!(deployment.firmware_meta.uuid, "abc-123");
        assert_eq!(deployment.firmware_meta.version, "1.1.0");
        assert_eq!(deployment.firmware_meta.platform, "rpi4");
    }

    #[test]
    fn parse_invalid_deployment() {
        let payload = json!({"missing": "fields"});
        assert!(Deployment::from_payload(&payload).is_err());
    }
}
