//! Top-level error type for the Rift/1 server.

use thiserror::Error;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, RiftError>;

/// Reasons a frame may be rejected during decode/validation.
#[derive(Debug, Error)]
pub enum FrameReject {
    #[error("protocol version unsupported: client={client}, server={server}")]
    ProtocolVersionUnsupported { client: u16, server: u16 },

    #[error("frame is malformed: {0}")]
    FrameInvalid(String),

    #[error("codec unsupported: {0}")]
    CodecUnsupported(String),

    #[error("payload too large: {actual} > {max}")]
    PayloadTooLarge { actual: usize, max: usize },

    #[error("required field missing: {0}")]
    RequiredFieldMissing(&'static str),

    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("order violation: {0}")]
    OrderViolation(String),
}

/// Reasons a session or resume attempt may fail.
#[derive(Debug, Error)]
pub enum SessionReject {
    #[error("session not found: {0}")]
    NotFound(String),

    #[error("session expired")]
    Expired,

    #[error("session epoch conflict: incoming={incoming}, current={current}")]
    Conflict { incoming: u32, current: u32 },

    #[error("resume rejected: {0}")]
    ResumeRejected(String),

    #[error("replay offset expired: topic={topic}, requested={requested}")]
    ReplayOffsetExpired { topic: String, requested: i64 },

    #[error("snapshot required for topic: {0}")]
    SnapshotRequired(String),
}

/// Reasons a topic operation may fail.
#[derive(Debug, Error)]
pub enum TopicReject {
    #[error("topic not found: {0}")]
    NotFound(String),

    #[error("topic closed: {0}")]
    Closed(String),

    #[error("topic overloaded: {0}")]
    Overloaded(String),

    #[error("topic subscriber limit reached: {0}")]
    SubscriberLimit(String),

    #[error("topic publisher limit reached: {0}")]
    PublisherLimit(String),

    #[error("topic forbidden: {0}")]
    Forbidden(String),

    #[error("topic rate limited: {0}")]
    RateLimited(String),

    #[error("invalid topic name: {0}")]
    InvalidName(String),
}

/// Reasons a message may be rejected.
#[derive(Debug, Error)]
pub enum MessageReject {
    #[error("duplicate message: id={0}")]
    Duplicate(String),

    #[error("message expired")]
    Expired,

    #[error("message rejected: {0}")]
    Rejected(String),

    #[error("message too large: {actual} > {max}")]
    TooLarge { actual: usize, max: usize },

    #[error("ack timeout: id={0}")]
    AckTimeout(String),

    #[error("delivery failed: {0}")]
    DeliveryFailed(String),
}

/// Authentication / authorization failures.
#[derive(Debug, Error)]
pub enum AuthReject {
    #[error("authentication required")]
    Required,

    #[error("authentication invalid: {0}")]
    Invalid(String),

    #[error("authentication expired")]
    Expired,

    #[error("authentication revoked")]
    Revoked,

    #[error("permission denied: {0}")]
    Denied(String),
}

/// System-level failures.
#[derive(Debug, Error)]
pub enum SystemReject {
    #[error("system overloaded")]
    Overloaded,

    #[error("system maintenance")]
    Maintenance,

    #[error("shard moved: topic={0}")]
    ShardMoved(String),

    #[error("region unavailable: {0}")]
    RegionUnavailable(String),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Top-level error for all Rift/1 server operations.
///
/// The categories live as dedicated enums (`FrameReject`,
/// `SessionReject`, `TopicReject`, `MessageReject`, `AuthReject`,
/// `SystemReject`); this top-level type composes them so call-sites
/// can match on a single result.
#[derive(Debug, Error)]
pub enum RiftError {
    #[error(transparent)]
    Frame(#[from] FrameReject),

    #[error(transparent)]
    Session(#[from] SessionReject),

    #[error(transparent)]
    Topic(#[from] TopicReject),

    #[error(transparent)]
    Message(#[from] MessageReject),

    #[error(transparent)]
    Auth(#[from] AuthReject),

    #[error(transparent)]
    System(#[from] SystemReject),

    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde_json error: {0}")]
    SerdeJson(BoxedStdError),

    #[error("ciborium error: {0}")]
    Cbor(BoxedStdError),

    #[error("ciborium de error: {0}")]
    CborDe(BoxedStdError),

    #[error("websocket error: {0}")]
    WebSocket(BoxedStdError),

    #[error("other: {0}")]
    Other(BoxedStdError),
}

impl From<serde_json::Error> for RiftError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeJson(BoxedStdError(Box::new(e)))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for RiftError {
    fn from(e: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::Cbor(BoxedStdError(Box::new(e)))
    }
}

impl From<ciborium::de::Error<std::io::Error>> for RiftError {
    fn from(e: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDe(BoxedStdError(Box::new(e)))
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for RiftError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(BoxedStdError(Box::new(e)))
    }
}

/// Convenience wrapper so arbitrary `std::error::Error` can be lifted
/// into `RiftError::Other` without forcing a new variant for every
/// source.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct BoxedStdError(pub Box<dyn std::error::Error + Send + Sync>);

impl RiftError {
    pub fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Other(BoxedStdError(Box::new(e)))
    }

    pub fn from_serde_json(e: serde_json::Error) -> Self {
        Self::SerdeJson(BoxedStdError(Box::new(e)))
    }

    pub fn from_cbor_ser<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Cbor(BoxedStdError(Box::new(e)))
    }

    pub fn from_cbor_de<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::CborDe(BoxedStdError(Box::new(e)))
    }
}

/// Invalid server configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid value for {field}: {message}")]
    Invalid {
        field: &'static str,
        message: String,
    },
}
