//! Frame-level primitives — `FrameType`, `Codec`, `Priority`, `FrameFlags`.

use std::fmt;

/// Frame type (spec §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameType {
    /// hello, welcome, ready, ping, pong, subscribe, unsubscribe, resume…
    Control,
    /// event, state, command, reply, datagram…
    Data,
    /// acknowledgement of a previously sent frame.
    Ack,
    /// flow-control / backpressure / window / degradation notice.
    Flow,
    /// protocol, permission, business, or system error.
    Error,
}

impl FrameType {
    /// Short single-letter tag used in compact encodings.
    pub fn tag(self) -> u8 {
        match self {
            FrameType::Control => b'C',
            FrameType::Data => b'D',
            FrameType::Ack => b'A',
            FrameType::Flow => b'F',
            FrameType::Error => b'E',
        }
    }

    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            b'C' => Some(FrameType::Control),
            b'D' => Some(FrameType::Data),
            b'A' => Some(FrameType::Ack),
            b'F' => Some(FrameType::Flow),
            b'E' => Some(FrameType::Error),
            _ => None,
        }
    }
}

impl fmt::Display for FrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            FrameType::Control => "control",
            FrameType::Data => "data",
            FrameType::Ack => "ack",
            FrameType::Flow => "flow",
            FrameType::Error => "error",
        })
    }
}

/// Encoding codec for the frame payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Codec {
    /// JSON — debug / development only (spec §7).
    Json,
    /// CBOR — default binary codec (spec §7).
    Cbor,
}

impl Codec {
    pub fn tag(self) -> u8 {
        match self {
            Codec::Json => b'J',
            Codec::Cbor => b'B',
        }
    }

    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            b'J' => Some(Codec::Json),
            b'B' => Some(Codec::Cbor),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Codec::Json => "json",
            Codec::Cbor => "cbor",
        }
    }
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Message priority (spec §18.3). Used to order transmission and to
/// decide what gets dropped under backpressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(u8)]
pub enum Priority {
    Background = 0,
    Volatile = 1,
    Low = 2,
    #[default]
    Normal = 3,
    High = 4,
    Critical = 5,
}

impl Priority {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Priority::Background,
            1 => Priority::Volatile,
            2 => Priority::Low,
            3 => Priority::Normal,
            4 => Priority::High,
            5 => Priority::Critical,
            _ => return None,
        })
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Priority::Critical => "critical",
            Priority::High => "high",
            Priority::Normal => "normal",
            Priority::Low => "low",
            Priority::Volatile => "volatile",
            Priority::Background => "background",
        })
    }
}

/// Bit flags on a frame (spec §6.3). Internally stored as a `u16`
/// bitset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct FrameFlags(u16);

impl FrameFlags {
    pub const COMPRESSED: u16 = 1 << 0;
    pub const ENCRYPTED: u16 = 1 << 1;
    pub const FRAGMENTED: u16 = 1 << 2;
    pub const FINAL_FRAGMENT: u16 = 1 << 3;
    pub const REQUIRES_ACK: u16 = 1 << 4;
    pub const REPLAYED: u16 = 1 << 5;
    pub const SNAPSHOT: u16 = 1 << 6;
    pub const DEGRADED: u16 = 1 << 7;
    pub const DUPLICATE: u16 = 1 << 8;
    pub const TRACE: u16 = 1 << 9;

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub fn contains(self, flag: u16) -> bool {
        self.0 & flag == flag
    }

    pub fn set(&mut self, flag: u16) {
        self.0 |= flag;
    }

    pub fn clear(&mut self, flag: u16) {
        self.0 &= !flag;
    }

    pub fn with(mut self, flag: u16) -> Self {
        self.set(flag);
        self
    }

    pub fn without(mut self, flag: u16) -> Self {
        self.clear(flag);
        self
    }
}

impl fmt::Display for FrameFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        let mut emit = |name: &str, flag: u16| -> fmt::Result {
            if self.contains(flag) {
                if !first {
                    f.write_str("|")?;
                }
                first = false;
                f.write_str(name)?;
            }
            Ok(())
        };
        emit("compressed", FrameFlags::COMPRESSED)?;
        emit("encrypted", FrameFlags::ENCRYPTED)?;
        emit("fragmented", FrameFlags::FRAGMENTED)?;
        emit("final_fragment", FrameFlags::FINAL_FRAGMENT)?;
        emit("requires_ack", FrameFlags::REQUIRES_ACK)?;
        emit("replayed", FrameFlags::REPLAYED)?;
        emit("snapshot", FrameFlags::SNAPSHOT)?;
        emit("degraded", FrameFlags::DEGRADED)?;
        emit("duplicate", FrameFlags::DUPLICATE)?;
        emit("trace", FrameFlags::TRACE)?;
        if first {
            f.write_str("none")?;
        }
        Ok(())
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
}
