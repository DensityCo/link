use crate::deployment::DeploymentError;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// Parsed deployment request from the server's "update" event.
#[derive(Debug, Clone, Deserialize)]
pub struct Deployment {
    #[serde(default = "default_update_available")]
    pub update_available: bool,
    pub firmware_url: String,
    pub firmware_meta: FirmwareMeta,
    pub size: Option<u64>,
    pub checksum: Option<String>,
    pub partials_checksums: Option<Vec<String>>,
    pub deployment_id: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FirmwareMeta {
    pub uuid: String,
    pub version: String,
    pub platform: String,
    pub architecture: String,
    pub product: String,
    pub author: Option<String>,
    pub description: Option<String>,
    pub fwup_version: Option<String>,
    pub vcs_identifier: Option<String>,
    pub misc: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Deployment {
    /// Parse a deployment message payload.
    pub fn from_payload(payload: &Value) -> Result<Self, DeploymentError> {
        if payload
            .get("update_available")
            .and_then(Value::as_bool)
            .is_some_and(|update_available| !update_available)
        {
            return Err(DeploymentError::NoUpdateAvailable);
        }

        serde_json::from_value(payload.clone())
            .map_err(|e| DeploymentError::InvalidMessage(e.to_string()))
    }
}

fn default_update_available() -> bool {
    true
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
                "product": "my-product",
                "author": "Jane",
                "description": "Release notes",
                "fwup_version": "1.12.0",
                "vcs_identifier": "abc123",
                "misc": "extra",
                "artifact_format": "tar.zst"
            },
            "update_available": true,
            "size": 1234,
            "checksum": "ABCDEF",
            "partials_checksums": ["ABCD", "EF01"],
            "deployment_id": 42,
            "artifact_kind": "bundle"
        });
        let deployment = Deployment::from_payload(&payload).unwrap();
        assert_eq!(
            deployment.firmware_url,
            "https://s3.example.com/fw.fw?token=abc"
        );
        assert!(deployment.update_available);
        assert_eq!(deployment.firmware_meta.uuid, "abc-123");
        assert_eq!(deployment.firmware_meta.version, "1.1.0");
        assert_eq!(deployment.firmware_meta.platform, "rpi4");
        assert_eq!(deployment.firmware_meta.author.as_deref(), Some("Jane"));
        assert_eq!(
            deployment.firmware_meta.description.as_deref(),
            Some("Release notes")
        );
        assert_eq!(
            deployment.firmware_meta.fwup_version.as_deref(),
            Some("1.12.0")
        );
        assert_eq!(
            deployment.firmware_meta.vcs_identifier.as_deref(),
            Some("abc123")
        );
        assert_eq!(deployment.firmware_meta.misc.as_deref(), Some("extra"));
        assert_eq!(deployment.firmware_meta.extra["artifact_format"], "tar.zst");
        assert_eq!(deployment.size, Some(1234));
        assert_eq!(deployment.checksum.as_deref(), Some("ABCDEF"));
        assert_eq!(
            deployment.partials_checksums.as_deref(),
            Some(&["ABCD".to_string(), "EF01".to_string()][..])
        );
        assert_eq!(deployment.deployment_id, Some(42));
        assert_eq!(deployment.extra["artifact_kind"], "bundle");
    }

    #[test]
    fn parse_invalid_deployment() {
        let payload = json!({"missing": "fields"});
        assert!(Deployment::from_payload(&payload).is_err());
    }

    #[test]
    fn parse_no_update_available() {
        let payload = json!({"update_available": false});
        let err = Deployment::from_payload(&payload).unwrap_err();

        assert!(matches!(err, DeploymentError::NoUpdateAvailable));
    }
}
