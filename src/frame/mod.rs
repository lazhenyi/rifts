//! Frame envelope — the wire-level message container for Rift/1.
//!
//! See spec §6. Every piece of information exchanged on a Rift/1
//! connection (control, data, ack, flow, error) is a `Frame`.

pub mod envelope;
pub mod types;

pub use envelope::Frame;
pub use types::{Codec, FrameFlags, FrameType, Priority};
