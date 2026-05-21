//! Snapshot — state snapshot used to re-initialize after resume
//! (spec §13.4).

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A snapshot of a topic's state at a given offset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Topic this snapshot belongs to.
    pub topic: String,
    /// Snapshot id.
    pub snapshot_id: String,
    /// Offset at which this snapshot was taken.
    pub base_offset: i64,
    /// Snapshot schema id.
    pub schema: String,
    /// Snapshot payload.
    pub payload: serde_json::Value,
    /// Creation timestamp (ms since epoch).
    pub created_at: i64,
    /// Expiration timestamp (ms since epoch).
    pub expires_at: Option<i64>,
    /// Optional checksum.
    pub checksum: Option<String>,
}

pub fn encode_snapshot(s: &Snapshot) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(s)?))
}

pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let s = Snapshot {
            topic: "room/1".into(),
            snapshot_id: "snap-1".into(),
            base_offset: 42,
            schema: "room.snapshot@1.0".into(),
            payload: serde_json::json!({"messages": []}),
            created_at: 1000,
            expires_at: Some(2000),
            checksum: None,
        };
        let bytes = encode_snapshot(&s).unwrap();
        let back = decode_snapshot(&bytes).unwrap();
        assert_eq!(back.base_offset, 42);
        assert_eq!(back.snapshot_id, "snap-1");
    }
}
