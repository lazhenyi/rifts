//! Protocol-level constants — version, name, negotiation rules.
//!
//! Spec §5.2 (hello) and §25 (versioning).

use std::ops::RangeInclusive;

/// Protocol name on the wire — spec §5.2 (`hello.protocol`).
pub const PROTOCOL_NAME: &str = "rift";

/// Current major version. Bumped on backwards-incompatible changes.
pub const PROTOCOL_MAJOR: u16 = 1;

/// Current minor version. Bumped on backwards-compatible changes.
pub const PROTOCOL_MINOR: u16 = 0;

/// Supported major-version range offered to clients during hello.
pub const SUPPORTED_MAJOR: RangeInclusive<u16> = 1..=1;

/// Encoded protocol version (major << 8 | minor). This is the value
/// used in the `version` field of every frame.
pub const fn encoded_version() -> u16 {
    (PROTOCOL_MAJOR << 8) | PROTOCOL_MINOR
}

/// Negotiate the highest mutually-supported major version.
///
/// For Rift/1 this is always `PROTOCOL_MAJOR`; this helper exists so
/// that a future Rift/2 server can still accept Rift/1 clients.
pub fn negotiate_major(client_major: u16) -> Option<u16> {
    if SUPPORTED_MAJOR.contains(&client_major) {
        Some(client_major)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_is_major_minor() {
        assert_eq!(encoded_version(), 0x0100);
    }

    #[test]
    fn negotiate_accepts_self() {
        assert_eq!(negotiate_major(1), Some(1));
    }

    #[test]
    fn negotiate_rejects_other() {
        assert_eq!(negotiate_major(0), None);
        assert_eq!(negotiate_major(2), None);
        assert_eq!(negotiate_major(99), None);
    }
}
