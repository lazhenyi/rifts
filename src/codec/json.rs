//! # JSON Codec
//!
//! Implements the [`Codec`](crate::codec::Codec) trait for the JSON
//! wire format.
//!
//! The protocol specification (section 7) lists JSON as the
//! debug and development codec. JSON payloads are human-readable,
//! making them ideal for troubleshooting, interactive testing, and
//! environments where binary tooling is unavailable. For production
//! use, prefer the CBOR codec ([`CborCodec`](crate::codec::CborCodec)).
//!
//! ## Implementation Details
//!
//! * Encoding uses [`serde_json::to_vec`] to serialize a
//!   [`serde_json::Value`] into a UTF-8 JSON byte vector.
//! * Decoding uses [`serde_json::from_slice`] to parse JSON bytes back
//!   into a [`serde_json::Value`].
//! * The codec is stateless and zero-cost -- [`JsonCodec`] is a unit struct
//!   that can be freely copied.

use bytes::Bytes;

use crate::codec::codec::Codec;
use crate::error::Result;
use crate::frame::Codec as FrameCodec;

/// JSON text codec for debugging and development.
///
/// This struct implements the [`Codec`] trait and carries the
/// [`FrameCodec::Json`] tag used during Hello-phase codec negotiation.
/// JSON encoding produces larger payloads than CBOR but is trivially
/// inspectable with standard tools (`curl`, `jq`, browser devtools, etc.).
///
/// # Examples
///
/// ```rust,no_run
/// use rifts::codec::{JsonCodec, Codec, CodecExt};
///
/// let codec = JsonCodec;
/// let bytes = codec.encode(&"hello").unwrap();
/// let value: String = codec.decode(&bytes).unwrap();
/// assert_eq!(value, "hello");
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    /// Returns [`FrameCodec::Json`], identifying this codec during
    /// Hello-phase negotiation.
    fn frame_codec(&self) -> FrameCodec {
        FrameCodec::Json
    }

    /// Encode a [`serde_json::Value`] into JSON text bytes.
    ///
    /// The output is compact (no pretty-printing or indentation)
    /// to minimize wire overhead.
    ///
    /// # Errors
    ///
    /// Returns an error if `serde_json::to_vec` encounters a
    /// serialization failure (extremely rare for valid `Value` inputs).
    fn encode_value(&self, value: &serde_json::Value) -> Result<Bytes> {
        Ok(Bytes::from(serde_json::to_vec(value)?))
    }

    /// Decode JSON text bytes into a [`serde_json::Value`].
    ///
    /// # Errors
    ///
    /// Returns an error if the input is not valid JSON or if
    /// `serde_json::from_slice` encounters a deserialization failure.
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
