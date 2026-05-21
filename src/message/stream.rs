//! Stream segment — a continuous ordered data stream (spec §8).
//!
//! A stream is a sequence of `StreamSegment` items sharing a
//! `stream_id`; ordering is preserved within a single stream.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A segment of a continuous data stream (AI tokens, file chunks,
/// audio frames, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSegment {
    /// Stream id (all segments of a stream share this).
    pub stream_id: String,
    /// Monotonic sequence within the stream.
    pub seq: u64,
    /// Whether this is the final segment.
    pub final_segment: bool,
    /// Schema id.
    pub schema: String,
    /// Segment payload.
    pub payload: serde_json::Value,
}

pub fn encode_stream(s: &StreamSegment) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(s)?))
}

pub fn decode_stream(bytes: &[u8]) -> Result<StreamSegment> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let s = StreamSegment {
            stream_id: "s1".into(),
            seq: 3,
            final_segment: false,
            schema: "ai.token@1.0".into(),
            payload: serde_json::json!({"text": "hello"}),
        };
        let bytes = encode_stream(&s).unwrap();
        let back = decode_stream(&bytes).unwrap();
        assert_eq!(back.seq, 3);
        assert!(!back.final_segment);
    }
}
