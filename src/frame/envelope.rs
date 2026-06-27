//! The `Frame` envelope — the single container for everything sent on
//! a Rift/1 wire.
//!
//! Fields mirror spec §6.1.

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::frame::{Codec, FrameFlags, FrameType, Priority};

/// A single frame exchanged between client and server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    /// Protocol version. Spec §6.1 requires this to be present.
    pub version: u16,
    /// Monotonically increasing frame id within the current connection.
    pub frame_id: u64,
    /// Frame category (control, data, ack, flow, error).
    pub frame_type: FrameType,
    /// Bit flags.
    pub flags: FrameFlags,
    /// Payload encoding.
    pub codec: Codec,
    /// Current logical session.
    pub session_id: Option<String>,
    /// Stream identifier (spec §6.1).
    pub stream_id: Option<String>,
    /// Topic.
    pub topic: Option<String>,
    /// Event name.
    pub event: Option<String>,
    /// Globally unique message id.
    pub message_id: Option<String>,
    /// Request/response correlation id.
    pub correlation_id: Option<String>,
    /// Distributed-trace id.
    pub trace_id: Option<String>,
    /// Sender timestamp (ms since epoch).
    pub timestamp: i64,
    /// Time-to-live in milliseconds.
    pub ttl_ms: Option<u32>,
    /// Priority.
    pub priority: Option<Priority>,
    /// Business or control payload.
    pub payload: Option<Bytes>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            version: 0,
            frame_id: 0,
            frame_type: FrameType::Control,
            flags: FrameFlags::empty(),
            codec: Codec::Json,
            session_id: None,
            stream_id: None,
            topic: None,
            event: None,
            message_id: None,
            correlation_id: None,
            trace_id: None,
            timestamp: 0,
            ttl_ms: None,
            priority: None,
            payload: None,
        }
    }
}

impl Frame {
    /// Construct a minimal control frame.
    pub fn control() -> Self {
        Self {
            frame_type: FrameType::Control,
            ..Self::default()
        }
    }

    /// Construct a minimal data frame.
    pub fn data() -> Self {
        Self {
            frame_type: FrameType::Data,
            ..Self::default()
        }
    }

    /// Construct a minimal ack frame.
    pub fn ack() -> Self {
        Self {
            frame_type: FrameType::Ack,
            ..Self::default()
        }
    }

    /// Construct a minimal flow frame.
    pub fn flow() -> Self {
        Self {
            frame_type: FrameType::Flow,
            ..Self::default()
        }
    }

    /// Construct a minimal error frame.
    pub fn error() -> Self {
        Self {
            frame_type: FrameType::Error,
            ..Self::default()
        }
    }

    /// Returns true if the frame requires acknowledgement.
    pub fn requires_ack(&self) -> bool {
        self.flags.contains(FrameFlags::REQUIRES_ACK)
    }

    /// Returns true if the frame is a replay (spec §13.1).
    pub fn is_replay(&self) -> bool {
        self.flags.contains(FrameFlags::REPLAYED)
    }

    /// Mark the frame as a replay.
    pub fn mark_replay(&mut self) {
        self.flags.set(FrameFlags::REPLAYED);
    }

    /// Returns true if the frame represents a snapshot (spec §6.3).
    pub fn is_snapshot(&self) -> bool {
        self.flags.contains(FrameFlags::SNAPSHOT)
    }

    /// Mark the frame as a snapshot.
    pub fn mark_snapshot(&mut self) {
        self.flags.set(FrameFlags::SNAPSHOT);
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Frame[id={} type={} codec={} topic={} event={} msg_id={} corr_id={} flags={} payload={}B]",
            self.frame_id,
            self.frame_type,
            self.codec,
            self.topic.as_deref().unwrap_or("-"),
            self.event.as_deref().unwrap_or("-"),
            self.message_id.as_deref().unwrap_or("-"),
            self.correlation_id.as_deref().unwrap_or("-"),
            self.flags,
            self.payload.as_ref().map(|p| p.len()).unwrap_or(0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_round_trip() {
        let mut f = FrameFlags::empty();
        f.set(FrameFlags::COMPRESSED);
        f.set(FrameFlags::REQUIRES_ACK);
        assert!(f.contains(FrameFlags::COMPRESSED));
        assert!(f.contains(FrameFlags::REQUIRES_ACK));
        assert!(!f.contains(FrameFlags::ENCRYPTED));
        assert_eq!(f.bits(), FrameFlags::COMPRESSED | FrameFlags::REQUIRES_ACK);
    }

    #[test]
    fn priority_default() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn frame_type_tag() {
        for t in [
            FrameType::Control,
            FrameType::Data,
            FrameType::Ack,
            FrameType::Flow,
            FrameType::Error,
        ] {
            assert_eq!(FrameType::from_tag(t.tag()), Some(t));
        }
        assert_eq!(FrameType::from_tag(b'X'), None);
    }

    #[test]
    fn codec_tag() {
        assert_eq!(Codec::from_tag(Codec::Json.tag()), Some(Codec::Json));
        assert_eq!(Codec::from_tag(Codec::Cbor.tag()), Some(Codec::Cbor));
        assert_eq!(Codec::from_tag(b'?'), None);
    }

    #[test]
    fn frame_constructors() {
        assert_eq!(Frame::control().frame_type, FrameType::Control);
        assert_eq!(Frame::data().frame_type, FrameType::Data);
        assert_eq!(Frame::ack().frame_type, FrameType::Ack);
        assert_eq!(Frame::flow().frame_type, FrameType::Flow);
        assert_eq!(Frame::error().frame_type, FrameType::Error);
    }
}
