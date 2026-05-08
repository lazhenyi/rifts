//! The `Codec` trait and negotiation helper.

use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;

use crate::error::Result;
use crate::frame::Codec as FrameCodec;

/// Trait for a single named encoding.
///
/// Codecs are stateless; they implement `encode_value` / `decode_value`
/// (non-generic, so the trait is dyn-compatible) and carry a
/// `FrameCodec` tag so the server can negotiate which one to use.
pub trait Codec: Send + Sync {
    /// Returns the `FrameCodec` enum value associated with this codec.
    fn frame_codec(&self) -> FrameCodec;

    /// Encode a JSON value to bytes.
    fn encode_value(&self, value: &serde_json::Value) -> Result<Bytes>;

    /// Decode bytes to a JSON value.
    fn decode_value(&self, bytes: &[u8]) -> Result<serde_json::Value>;
}

impl<T> CodecExt for T
where
    T: Codec + ?Sized,
{
    fn encode<T2: Serialize + ?Sized>(&self, value: &T2) -> Result<Bytes> {
        let v = serde_json::to_value(value)?;
        self.encode_value(&v)
    }

    fn decode<T2: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T2> {
        let v = self.decode_value(bytes)?;
        Ok(serde_json::from_value(v)?)
    }
}

/// Extension methods available on every `Codec` — generic helpers
/// built on top of the non-generic `encode_value` / `decode_value`.
pub trait CodecExt: Codec {
    fn encode<T: Serialize + ?Sized>(&self, value: &T) -> Result<Bytes>;
    fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> Result<T>;
}

/// Negotiate a codec given a list of client preferences.
pub fn negotiate(server: &[Arc<dyn Codec>], client: &[FrameCodec]) -> Result<Arc<dyn Codec>> {
    for want in client {
        if let Some(c) = server.iter().find(|c| c.frame_codec() == *want) {
            return Ok(c.clone());
        }
    }
    Err(crate::error::RiftError::Frame(
        crate::error::FrameReject::CodecUnsupported(format!("client offered {:?}", client)),
    ))
}
