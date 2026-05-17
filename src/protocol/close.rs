//! Close codes — spec §20.

use std::fmt;

/// Close codes used when ending a Rift/1 connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum CloseCode {
    Normal = 1000,
    Draining = 1001,
    ProtocolError = 1002,
    UnsupportedCodec = 1003,
    AuthFailed = 1004,
    AuthExpired = 1005,
    PermissionRevoked = 1006,
    SessionConflict = 1007,
    RateLimited = 1008,
    PayloadTooLarge = 1009,
    SlowConsumer = 1010,
    ServerOverloaded = 1011,
    ShardMoved = 1012,
    IdleTimeout = 1013,
    ClientUpgradeRequired = 1014,
    PolicyViolation = 1015,
}

impl CloseCode {
    pub fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            1000 => CloseCode::Normal,
            1001 => CloseCode::Draining,
            1002 => CloseCode::ProtocolError,
            1003 => CloseCode::UnsupportedCodec,
            1004 => CloseCode::AuthFailed,
            1005 => CloseCode::AuthExpired,
            1006 => CloseCode::PermissionRevoked,
            1007 => CloseCode::SessionConflict,
            1008 => CloseCode::RateLimited,
            1009 => CloseCode::PayloadTooLarge,
            1010 => CloseCode::SlowConsumer,
            1011 => CloseCode::ServerOverloaded,
            1012 => CloseCode::ShardMoved,
            1013 => CloseCode::IdleTimeout,
            1014 => CloseCode::ClientUpgradeRequired,
            1015 => CloseCode::PolicyViolation,
            _ => return None,
        })
    }

    pub fn as_u16(self) -> u16 {
        self as u16
    }

    pub fn name(self) -> &'static str {
        match self {
            CloseCode::Normal => "normal",
            CloseCode::Draining => "draining",
            CloseCode::ProtocolError => "protocol_error",
            CloseCode::UnsupportedCodec => "unsupported_codec",
            CloseCode::AuthFailed => "auth_failed",
            CloseCode::AuthExpired => "auth_expired",
            CloseCode::PermissionRevoked => "permission_revoked",
            CloseCode::SessionConflict => "session_conflict",
            CloseCode::RateLimited => "rate_limited",
            CloseCode::PayloadTooLarge => "payload_too_large",
            CloseCode::SlowConsumer => "slow_consumer",
            CloseCode::ServerOverloaded => "server_overloaded",
            CloseCode::ShardMoved => "shard_moved",
            CloseCode::IdleTimeout => "idle_timeout",
            CloseCode::ClientUpgradeRequired => "client_upgrade_required",
            CloseCode::PolicyViolation => "policy_violation",
        }
    }
}

impl fmt::Display for CloseCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.name(), self.as_u16())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for c in [
            CloseCode::Normal,
            CloseCode::Draining,
            CloseCode::ProtocolError,
            CloseCode::UnsupportedCodec,
            CloseCode::AuthFailed,
            CloseCode::AuthExpired,
            CloseCode::PermissionRevoked,
            CloseCode::SessionConflict,
            CloseCode::RateLimited,
            CloseCode::PayloadTooLarge,
            CloseCode::SlowConsumer,
            CloseCode::ServerOverloaded,
            CloseCode::ShardMoved,
            CloseCode::IdleTimeout,
            CloseCode::ClientUpgradeRequired,
            CloseCode::PolicyViolation,
        ] {
            assert_eq!(CloseCode::from_u16(c.as_u16()), Some(c));
        }
    }

    #[test]
    fn unknown() {
        assert_eq!(CloseCode::from_u16(0), None);
        assert_eq!(CloseCode::from_u16(9999), None);
    }
}
