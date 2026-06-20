//! Codec layer — pluggable payload encoders/decoders.
//!
//! A `Codec` takes the application-level data and turns it into bytes
//! (for transmission) or vice versa. Spec §7 lists the supported
//! encodings.

pub mod cbor;
#[allow(clippy::module_inception)]
pub mod codec;
pub mod json;

pub use cbor::CborCodec;
pub use codec::{Codec, negotiate};
pub use json::JsonCodec;
