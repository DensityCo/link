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
    Identify,
    ScriptsRun,
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
            ProtocolEvent::Identify => "identify",
            ProtocolEvent::ScriptsRun => "scripts/run",
            ProtocolEvent::Rebooting => "rebooting",
            ProtocolEvent::StatusUpdate => "status_update",
            ProtocolEvent::FwupProgress => "fwup_progress",
        }
    }
}

impl std::str::FromStr for ProtocolEvent {
    type Err = ();

    fn from_str(event: &str) -> Result<Self, Self::Err> {
        match event {
            "phx_join" => Ok(ProtocolEvent::PhxJoin),
            "phx_reply" => Ok(ProtocolEvent::PhxReply),
            "phx_error" => Ok(ProtocolEvent::PhxError),
            "phx_close" => Ok(ProtocolEvent::PhxClose),
            "heartbeat" => Ok(ProtocolEvent::Heartbeat),
            "extensions:get" => Ok(ProtocolEvent::ExtensionsGet),
            "update" => Ok(ProtocolEvent::Update),
            "reboot" => Ok(ProtocolEvent::Reboot),
            "identify" => Ok(ProtocolEvent::Identify),
            "scripts/run" => Ok(ProtocolEvent::ScriptsRun),
            "rebooting" => Ok(ProtocolEvent::Rebooting),
            "status_update" => Ok(ProtocolEvent::StatusUpdate),
            "fwup_progress" => Ok(ProtocolEvent::FwupProgress),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_events() {
        assert_eq!(
            "status_update".parse::<ProtocolEvent>(),
            Ok(ProtocolEvent::StatusUpdate)
        );
        assert_eq!(
            "scripts/run".parse::<ProtocolEvent>(),
            Ok(ProtocolEvent::ScriptsRun)
        );
        assert_eq!(ProtocolEvent::FwupProgress.as_str(), "fwup_progress");
        assert!("custom".parse::<ProtocolEvent>().is_err());
    }
}
