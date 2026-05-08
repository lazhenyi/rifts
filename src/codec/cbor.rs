//! CBOR codec — spec §7 lists CBOR as the default binary codec.

use bytes::Bytes;

use crate::codec::codec::Codec;
use crate::error::Result;
use crate::frame::Codec as FrameCodec;

/// Default binary codec.
#[derive(Debug, Default, Clone, Copy)]
pub struct CborCodec;

impl Codec for CborCodec {
    fn frame_codec(&self) -> FrameCodec {
        FrameCodec::Cbor
    }

    fn encode_value(&self, value: &serde_json::Value) -> Result<Bytes> {
        let mut buf = Vec::new();
        ciborium::into_writer(value, &mut buf)?;
        Ok(Bytes::from(buf))
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<serde_json::Value> {
        Ok(ciborium::from_reader(bytes)?)
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
        let c = CborCodec;
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
        assert_eq!(CborCodec.frame_codec(), FrameCodec::Cbor);
    }
}
