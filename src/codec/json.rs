//! JSON codec — spec §7 lists JSON as the debug/development codec.

use bytes::Bytes;

use crate::codec::codec::Codec;
use crate::error::Result;
use crate::frame::Codec as FrameCodec;

/// JSON debug codec.
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn frame_codec(&self) -> FrameCodec {
        FrameCodec::Json
    }

    fn encode_value(&self, value: &serde_json::Value) -> Result<Bytes> {
        Ok(Bytes::from(serde_json::to_vec(value)?))
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<serde_json::Value> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::codec::codec::CodecExt;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Sample {
        name: String,
        count: u32,
    }

    #[test]
    fn round_trip() {
        let c = JsonCodec;
        let s = Sample {
            name: "rift".to_string(),
            count: 42,
        };
        let bytes = c.encode(&s).unwrap();
        let back: Sample = c.decode(&bytes).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn frame_codec_tag() {
        assert_eq!(JsonCodec.frame_codec(), FrameCodec::Json);
    }
}
