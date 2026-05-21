//! Event message — spec §8 `event`.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{MessageReject, Result, RiftError};

/// Business event published to a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Event name (e.g. `chat.message.created`).
    pub event: String,
    /// Message id (ULID, UUIDv7, etc.).
    pub message_id: String,
    /// Schema id `{domain}.{name}@{major}.{minor}`.
    pub schema: String,
    /// Business payload.
    pub payload: serde_json::Value,
    /// Optional dedupe key.
    pub dedupe_key: Option<String>,
    /// Optional ordering key.
    pub ordering_key: Option<String>,
    /// Optional time-to-live in milliseconds.
    pub ttl_ms: Option<u32>,
}

impl Event {
    pub fn new(
        event: impl Into<String>,
        message_id: impl Into<String>,
        schema: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            event: event.into(),
            message_id: message_id.into(),
            schema: schema.into(),
            payload,
            dedupe_key: None,
            ordering_key: None,
            ttl_ms: None,
        }
    }

    /// Approximate encoded size, used for queue accounting.
    pub fn size_hint(&self) -> usize {
        self.event.len()
            + self.message_id.len()
            + self.schema.len()
            + serde_json::to_vec(&self.payload)
                .map(|v| v.len())
                .unwrap_or(0)
    }
}

/// Helper for converting an `Event` to/from a raw `Bytes` payload
/// (used for the on-wire `payload` field of a frame).
pub fn encode_event_body(e: &Event) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(e)?))
}

pub fn decode_event_body(bytes: &[u8]) -> Result<Event> {
    if bytes.is_empty() {
        return Err(RiftError::Message(MessageReject::Rejected(
            "empty event payload".into(),
        )));
    }
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let e = Event::new(
            "chat.message.created",
            "01HZZZZZZZZZZZZZZZZZZZZZZ",
            "chat.message.created@1.0",
            serde_json::json!({"text": "hi"}),
        );
        let bytes = encode_event_body(&e).unwrap();
        let back = decode_event_body(&bytes).unwrap();
        assert_eq!(back.event, e.event);
        assert_eq!(back.message_id, e.message_id);
    }

    #[test]
    fn decode_empty_fails() {
        let r = decode_event_body(&[]);
        assert!(r.is_err());
    }
}
