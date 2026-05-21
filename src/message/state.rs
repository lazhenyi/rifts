//! State messages and Presence — spec §14.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A state message — only the latest value for a `state_key` matters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    /// State key (e.g. `cursor`, `typing:user-42`).
    pub state_key: String,
    /// Optional human-readable name for the state.
    pub name: Option<String>,
    /// The current state payload.
    pub value: serde_json::Value,
    /// Optional TTL in milliseconds.
    pub ttl_ms: Option<u32>,
    /// Optional source subject (user/device/connection).
    pub subject: Option<String>,
    /// Update timestamp (ms since epoch).
    pub updated_at: i64,
}

/// Presence is a special kind of state (spec §14.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Presence {
    /// Subject this presence entry describes.
    pub subject: String,
    /// Status string (e.g. `online`, `away`, `busy`, `offline`).
    pub status: String,
    /// Owning session id.
    pub session_id: Option<String>,
    /// Owning connection id.
    pub connection_id: Option<String>,
    /// Presence TTL in milliseconds.
    pub ttl_ms: Option<u32>,
    /// Free-form metadata.
    pub metadata: Option<serde_json::Value>,
    /// Update timestamp.
    pub updated_at: i64,
}

pub fn encode_state(s: &State) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(s)?))
}

pub fn decode_state(bytes: &[u8]) -> Result<State> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn encode_presence(p: &Presence) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(p)?))
}

pub fn decode_presence(bytes: &[u8]) -> Result<Presence> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trip() {
        let s = State {
            state_key: "cursor".into(),
            name: Some("cursor position".into()),
            value: serde_json::json!({"x": 1, "y": 2}),
            ttl_ms: None,
            subject: Some("user-1".into()),
            updated_at: 1000,
        };
        let bytes = encode_state(&s).unwrap();
        let back = decode_state(&bytes).unwrap();
        assert_eq!(back.state_key, "cursor");
    }

    #[test]
    fn presence_round_trip() {
        let p = Presence {
            subject: "user-1".into(),
            status: "online".into(),
            session_id: Some("s-1".into()),
            connection_id: Some("c-1".into()),
            ttl_ms: Some(30_000),
            metadata: None,
            updated_at: 1000,
        };
        let bytes = encode_presence(&p).unwrap();
        let back = decode_presence(&bytes).unwrap();
        assert_eq!(back.status, "online");
    }
}
