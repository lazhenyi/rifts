//! Datagram — best-effort, low-latency, no delivery guarantee (spec §8).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A high-frequency, drop-tolerant message (e.g. mouse moves, typing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Datagram {
    /// Schema id.
    pub schema: String,
    /// Optional event name.
    pub event: Option<String>,
    /// Payload.
    pub payload: serde_json::Value,
}

pub fn encode_datagram(d: &Datagram) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(d)?))
}

pub fn decode_datagram(bytes: &[u8]) -> Result<Datagram> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let d = Datagram {
            schema: "game.position@1.0".into(),
            event: Some("move".into()),
            payload: serde_json::json!({"x": 10, "y": 20}),
        };
        let bytes = encode_datagram(&d).unwrap();
        let back = decode_datagram(&bytes).unwrap();
        assert_eq!(back.payload["x"], 10);
    }
}
