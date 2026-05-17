//! Structured error codes — spec §19.1.

use std::fmt;

/// Stable, machine-readable error code returned in a `Frame::error()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    // Protocol (§19.1)
    ProtocolVersionUnsupported,
    ProtocolFrameInvalid,
    ProtocolCodecUnsupported,
    ProtocolPayloadTooLarge,
    ProtocolRequiredFieldMissing,
    ProtocolSchemaMismatch,
    ProtocolOrderViolation,

    // Auth & permission
    AuthRequired,
    AuthInvalid,
    AuthExpired,
    AuthRevoked,
    PermissionDenied,
    TopicForbidden,

    // Session & resume
    SessionNotFound,
    SessionExpired,
    SessionConflict,
    ResumeRejected,
    ReplayOffsetExpired,
    SnapshotRequired,

    // Topic
    TopicNotFound,
    TopicClosed,
    TopicOverloaded,
    TopicSubscriberLimit,
    TopicPublisherLimit,
    TopicRateLimited,

    // Message
    MessageDuplicate,
    MessageExpired,
    MessageRejected,
    MessageTooLarge,
    MessageAckTimeout,
    MessageDeliveryFailed,

    // System
    SystemOverloaded,
    SystemMaintenance,
    SystemShardMoved,
    SystemRegionUnavailable,
    SystemInternal,
}

impl ErrorCode {
    /// Stable string identifier sent on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            // Protocol
            ErrorCode::ProtocolVersionUnsupported => "RIFT_PROTOCOL_VERSION_UNSUPPORTED",
            ErrorCode::ProtocolFrameInvalid => "RIFT_PROTOCOL_FRAME_INVALID",
            ErrorCode::ProtocolCodecUnsupported => "RIFT_PROTOCOL_CODEC_UNSUPPORTED",
            ErrorCode::ProtocolPayloadTooLarge => "RIFT_PROTOCOL_PAYLOAD_TOO_LARGE",
            ErrorCode::ProtocolRequiredFieldMissing => "RIFT_PROTOCOL_REQUIRED_FIELD_MISSING",
            ErrorCode::ProtocolSchemaMismatch => "RIFT_PROTOCOL_SCHEMA_MISMATCH",
            ErrorCode::ProtocolOrderViolation => "RIFT_PROTOCOL_ORDER_VIOLATION",

            // Auth
            ErrorCode::AuthRequired => "RIFT_AUTH_REQUIRED",
            ErrorCode::AuthInvalid => "RIFT_AUTH_INVALID",
            ErrorCode::AuthExpired => "RIFT_AUTH_EXPIRED",
            ErrorCode::AuthRevoked => "RIFT_AUTH_REVOKED",
            ErrorCode::PermissionDenied => "RIFT_PERMISSION_DENIED",
            ErrorCode::TopicForbidden => "RIFT_TOPIC_FORBIDDEN",

            // Session
            ErrorCode::SessionNotFound => "RIFT_SESSION_NOT_FOUND",
            ErrorCode::SessionExpired => "RIFT_SESSION_EXPIRED",
            ErrorCode::SessionConflict => "RIFT_SESSION_CONFLICT",
            ErrorCode::ResumeRejected => "RIFT_RESUME_REJECTED",
            ErrorCode::ReplayOffsetExpired => "RIFT_REPLAY_OFFSET_EXPIRED",
            ErrorCode::SnapshotRequired => "RIFT_SNAPSHOT_REQUIRED",

            // Topic
            ErrorCode::TopicNotFound => "RIFT_TOPIC_NOT_FOUND",
            ErrorCode::TopicClosed => "RIFT_TOPIC_CLOSED",
            ErrorCode::TopicOverloaded => "RIFT_TOPIC_OVERLOADED",
            ErrorCode::TopicSubscriberLimit => "RIFT_TOPIC_SUBSCRIBER_LIMIT",
            ErrorCode::TopicPublisherLimit => "RIFT_TOPIC_PUBLISHER_LIMIT",
            ErrorCode::TopicRateLimited => "RIFT_TOPIC_RATE_LIMITED",

            // Message
            ErrorCode::MessageDuplicate => "RIFT_MESSAGE_DUPLICATE",
            ErrorCode::MessageExpired => "RIFT_MESSAGE_EXPIRED",
            ErrorCode::MessageRejected => "RIFT_MESSAGE_REJECTED",
            ErrorCode::MessageTooLarge => "RIFT_MESSAGE_TOO_LARGE",
            ErrorCode::MessageAckTimeout => "RIFT_MESSAGE_ACK_TIMEOUT",
            ErrorCode::MessageDeliveryFailed => "RIFT_MESSAGE_DELIVERY_FAILED",

            // System
            ErrorCode::SystemOverloaded => "RIFT_SYSTEM_OVERLOADED",
            ErrorCode::SystemMaintenance => "RIFT_SYSTEM_MAINTENANCE",
            ErrorCode::SystemShardMoved => "RIFT_SYSTEM_SHARD_MOVED",
            ErrorCode::SystemRegionUnavailable => "RIFT_SYSTEM_REGION_UNAVAILABLE",
            ErrorCode::SystemInternal => "RIFT_SYSTEM_INTERNAL",
        }
    }

    /// Whether the error is generally safe to retry.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::SystemOverloaded
                | ErrorCode::SystemShardMoved
                | ErrorCode::SystemRegionUnavailable
                | ErrorCode::TopicOverloaded
                | ErrorCode::TopicRateLimited
                | ErrorCode::ReplayOffsetExpired
                | ErrorCode::MessageDeliveryFailed
                | ErrorCode::MessageAckTimeout
        )
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_round_trip() {
        for c in [
            ErrorCode::ProtocolVersionUnsupported,
            ErrorCode::AuthInvalid,
            ErrorCode::SessionConflict,
            ErrorCode::TopicNotFound,
            ErrorCode::MessageDuplicate,
            ErrorCode::SystemInternal,
        ] {
            assert!(!c.as_str().is_empty());
            assert!(c.as_str().starts_with("RIFT_"));
        }
    }

    #[test]
    fn retryable() {
        assert!(ErrorCode::SystemOverloaded.is_retryable());
        assert!(!ErrorCode::AuthInvalid.is_retryable());
        assert!(!ErrorCode::ProtocolFrameInvalid.is_retryable());
    }
}
