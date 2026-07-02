#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProtocolEvent {
    PhxJoin,
    PhxReply,
    PhxError,
    PhxClose,
    Heartbeat,
    ExtensionsGet,
    Update,
    Reboot,
    Rebooting,
    StatusUpdate,
    FwupProgress,
}

impl ProtocolEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            ProtocolEvent::PhxJoin => "phx_join",
            ProtocolEvent::PhxReply => "phx_reply",
            ProtocolEvent::PhxError => "phx_error",
            ProtocolEvent::PhxClose => "phx_close",
            ProtocolEvent::Heartbeat => "heartbeat",
            ProtocolEvent::ExtensionsGet => "extensions:get",
            ProtocolEvent::Update => "update",
            ProtocolEvent::Reboot => "reboot",
            ProtocolEvent::Rebooting => "rebooting",
            ProtocolEvent::StatusUpdate => "status_update",
            ProtocolEvent::FwupProgress => "fwup_progress",
        }
    }

    pub fn from_str(event: &str) -> Option<Self> {
        match event {
            "phx_join" => Some(ProtocolEvent::PhxJoin),
            "phx_reply" => Some(ProtocolEvent::PhxReply),
            "phx_error" => Some(ProtocolEvent::PhxError),
            "phx_close" => Some(ProtocolEvent::PhxClose),
            "heartbeat" => Some(ProtocolEvent::Heartbeat),
            "extensions:get" => Some(ProtocolEvent::ExtensionsGet),
            "update" => Some(ProtocolEvent::Update),
            "reboot" => Some(ProtocolEvent::Reboot),
            "rebooting" => Some(ProtocolEvent::Rebooting),
            "status_update" => Some(ProtocolEvent::StatusUpdate),
            "fwup_progress" => Some(ProtocolEvent::FwupProgress),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_events() {
        assert_eq!(
            ProtocolEvent::from_str("status_update"),
            Some(ProtocolEvent::StatusUpdate)
        );
        assert_eq!(ProtocolEvent::FwupProgress.as_str(), "fwup_progress");
        assert_eq!(ProtocolEvent::from_str("custom"), None);
    }
}
