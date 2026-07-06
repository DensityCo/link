use crate::protocol::events::ProtocolEvent;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid message format")]
    InvalidFormat,
}

/// A Phoenix Channels message: [join_ref, ref, topic, event, payload]
#[derive(Debug, Clone)]
pub struct Message {
    pub join_ref: Option<String>,
    pub msg_ref: Option<String>,
    pub topic: String,
    pub event: String,
    pub payload: Value,
}

impl Message {
    pub fn from_json(text: &str) -> Result<Self, ChannelError> {
        let arr: Vec<Value> = serde_json::from_str(text)?;
        if arr.len() != 5 {
            return Err(ChannelError::InvalidFormat);
        }

        Ok(Message {
            join_ref: arr[0].as_str().map(String::from),
            msg_ref: arr[1].as_str().map(String::from),
            topic: arr[2]
                .as_str()
                .ok_or(ChannelError::InvalidFormat)?
                .to_string(),
            event: arr[3]
                .as_str()
                .ok_or(ChannelError::InvalidFormat)?
                .to_string(),
            payload: arr[4].clone(),
        })
    }

    pub fn to_json(&self) -> String {
        let arr = serde_json::json!([
            self.join_ref,
            self.msg_ref,
            self.topic,
            self.event,
            self.payload,
        ]);
        arr.to_string()
    }

    pub fn reply_status(&self) -> Option<&str> {
        if self.event() == Some(ProtocolEvent::PhxReply) {
            self.payload.get("status")?.as_str()
        } else {
            None
        }
    }

    pub fn event(&self) -> Option<ProtocolEvent> {
        ProtocolEvent::from_str(&self.event)
    }

    pub fn is_reply(&self) -> bool {
        self.event() == Some(ProtocolEvent::PhxReply)
    }

    pub fn reply_ok(&self) -> bool {
        self.reply_status() == Some("ok")
    }
}

/// Reference counter for Phoenix Channels messages.
#[derive(Clone)]
pub struct RefCounter {
    next: Arc<AtomicU64>,
}

impl RefCounter {
    pub fn new() -> Self {
        Self {
            next: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn next(&self) -> String {
        self.next.fetch_add(1, Ordering::Relaxed).to_string()
    }
}

/// Builds Phoenix Channels protocol messages.
#[derive(Clone)]
pub struct ChannelBuilder {
    pub topic: String,
    pub join_ref: String,
    refs: RefCounter,
}

impl ChannelBuilder {
    pub fn new(topic: String) -> Self {
        let refs = RefCounter::new();
        let join_ref = refs.next();
        Self {
            topic,
            join_ref,
            refs,
        }
    }

    /// Build a join message for the device channel.
    pub fn join(&self, payload: Value) -> Message {
        Message {
            join_ref: Some(self.join_ref.clone()),
            msg_ref: Some(self.join_ref.clone()),
            topic: self.topic.clone(),
            event: ProtocolEvent::PhxJoin.as_str().to_string(),
            payload,
        }
    }

    /// Build a heartbeat message.
    pub fn heartbeat(&self) -> Message {
        Message {
            join_ref: None,
            msg_ref: Some(self.refs.next()),
            topic: "phoenix".to_string(),
            event: ProtocolEvent::Heartbeat.as_str().to_string(),
            payload: serde_json::json!({}),
        }
    }

    /// Build a push message to the server.
    pub fn push(&self, event: ProtocolEvent, payload: Value) -> Message {
        self.push_custom(event.as_str(), payload)
    }

    /// Build a push message with a dynamic extension event name.
    pub fn push_custom(&self, event: &str, payload: Value) -> Message {
        Message {
            join_ref: Some(self.join_ref.clone()),
            msg_ref: Some(self.refs.next()),
            topic: self.topic.clone(),
            event: event.to_string(),
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_server_push() {
        let json = r#"[null,null,"device:dev-123","update",{"firmware_url":"https://example.com/fw.fw","firmware_meta":{"uuid":"abc"}}]"#;
        let msg = Message::from_json(json).unwrap();
        assert!(msg.join_ref.is_none());
        assert!(msg.msg_ref.is_none());
        assert_eq!(msg.topic, "device:dev-123");
        assert_eq!(msg.event, "update");
        assert_eq!(msg.payload["firmware_url"], "https://example.com/fw.fw");
    }

    #[test]
    fn parse_reply() {
        let json = r#"["1","1","device:dev-123","phx_reply",{"status":"ok","response":{}}]"#;
        let msg = Message::from_json(json).unwrap();
        assert_eq!(msg.join_ref.as_deref(), Some("1"));
        assert_eq!(msg.msg_ref.as_deref(), Some("1"));
        assert!(msg.is_reply());
        assert!(msg.reply_ok());
    }

    #[test]
    fn parse_error_reply() {
        let json = r#"["1","1","device:dev-123","phx_reply",{"status":"error","response":{"reason":"unauthorized"}}]"#;
        let msg = Message::from_json(json).unwrap();
        assert!(msg.is_reply());
        assert!(!msg.reply_ok());
        assert_eq!(msg.reply_status(), Some("error"));
    }

    #[test]
    fn build_join() {
        let ch = ChannelBuilder::new("device:dev-123".to_string());
        let msg = ch.join(json!({"nerves_fw_version": "1.0.0"}));
        assert_eq!(msg.event, "phx_join");
        assert_eq!(msg.topic, "device:dev-123");
        assert!(msg.join_ref.is_some());
        let parsed: Vec<Value> = serde_json::from_str(&msg.to_json()).unwrap();
        assert_eq!(parsed.len(), 5);
        assert_eq!(parsed[3], "phx_join");
    }

    #[test]
    fn build_heartbeat() {
        let ch = ChannelBuilder::new("device:dev-123".to_string());
        let msg = ch.heartbeat();
        assert_eq!(msg.topic, "phoenix");
        assert_eq!(msg.event, "heartbeat");
        assert!(msg.join_ref.is_none());
        assert!(msg.msg_ref.is_some());
    }

    #[test]
    fn build_push() {
        let ch = ChannelBuilder::new("device:dev-123".to_string());
        let msg = ch.push(ProtocolEvent::FwupProgress, json!({"value": 50}));
        assert_eq!(msg.event, "fwup_progress");
        assert_eq!(msg.topic, "device:dev-123");
        assert_eq!(msg.payload["value"], 50);
    }

    #[test]
    fn ref_counter_increments() {
        let ch = ChannelBuilder::new("device:x".to_string());
        // join_ref consumed ref 1
        let h1 = ch.heartbeat();
        let h2 = ch.heartbeat();
        let r1: u64 = h1.msg_ref.unwrap().parse().unwrap();
        let r2: u64 = h2.msg_ref.unwrap().parse().unwrap();
        assert_eq!(r2, r1 + 1);
    }

    #[test]
    fn roundtrip_json() {
        let ch = ChannelBuilder::new("device:dev-123".to_string());
        let original = ch.push(ProtocolEvent::StatusUpdate, json!({"status": "completed"}));
        let json_str = original.to_json();
        let parsed = Message::from_json(&json_str).unwrap();
        assert_eq!(parsed.topic, original.topic);
        assert_eq!(parsed.event, original.event);
        assert_eq!(parsed.payload, original.payload);
    }

    #[test]
    fn invalid_message_format() {
        assert!(Message::from_json("{}").is_err());
        assert!(Message::from_json("[1,2,3]").is_err());
        assert!(Message::from_json("not json").is_err());
    }
}
