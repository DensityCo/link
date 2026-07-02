use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProgressStage {
    Downloading,
    Updating,
}

impl ProgressStage {
    pub fn as_str(self) -> &'static str {
        match self {
            ProgressStage::Downloading => "downloading",
            ProgressStage::Updating => "updating",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum UpdateStatus {
    Received,
    Started {
        downloader_network_interface: Option<String>,
    },
    Completed,
    Ignored {
        reason: String,
    },
    Rescheduled {
        delay_for: u64,
        reason: Option<String>,
    },
    Failed {
        reason: String,
    },
}

impl UpdateStatus {
    pub fn payload(&self) -> Value {
        match self {
            UpdateStatus::Received => json!({"status": "received"}),
            UpdateStatus::Started {
                downloader_network_interface: Some(interface),
            } => {
                json!({
                    "status": "started",
                    "downloader_network_interface": interface,
                })
            }
            UpdateStatus::Started {
                downloader_network_interface: None,
            } => json!({"status": "started"}),
            UpdateStatus::Completed => json!({"status": "completed"}),
            UpdateStatus::Ignored { reason } => {
                json!({"status": "ignored", "reason": reason})
            }
            UpdateStatus::Rescheduled {
                delay_for,
                reason: Some(reason),
            } => {
                json!({
                    "status": "rescheduled",
                    "delay_for": delay_for,
                    "reason": reason,
                })
            }
            UpdateStatus::Rescheduled {
                delay_for,
                reason: None,
            } => json!({"status": "rescheduled", "delay_for": delay_for}),
            UpdateStatus::Failed { reason } => {
                json!({"status": "failed", "reason": reason})
            }
        }
    }
}

pub fn progress_payload(stage: ProgressStage, value: u8) -> Value {
    json!({
        "stage": stage.as_str(),
        "value": value.min(100),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_payloads_match_wire_protocol() {
        assert_eq!(UpdateStatus::Received.payload()["status"], "received");
        assert_eq!(
            UpdateStatus::Failed {
                reason: "nope".to_string()
            }
            .payload()["reason"],
            "nope"
        );
        assert_eq!(
            UpdateStatus::Rescheduled {
                delay_for: 10,
                reason: None
            }
            .payload()["delay_for"],
            10
        );
    }

    #[test]
    fn progress_payload_clamps_percent() {
        let payload = progress_payload(ProgressStage::Downloading, 200);

        assert_eq!(payload["stage"], "downloading");
        assert_eq!(payload["value"], 100);
    }
}
