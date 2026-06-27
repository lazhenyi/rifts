//! Top-level error type for the Rift/1 server.

use thiserror::Error;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, RiftError>;

/// Reasons a frame may be rejected during decode/validation.
#[derive(Debug, Error)]
pub enum FrameReject {
    /// The client's protocol version is not supported by this server.
    #[error("protocol version unsupported: client={client}, server={server}")]
    ProtocolVersionUnsupported { client: u16, server: u16 },

    /// The frame is structurally invalid or contains illegal values.
    #[error("frame is malformed: {0}")]
    FrameInvalid(String),

    /// The client requested a codec the server does not support.
    #[error("codec unsupported: {0}")]
    CodecUnsupported(String),

    /// The payload exceeds the server-configured maximum.
    #[error("payload too large: {actual} > {max}")]
    PayloadTooLarge { actual: usize, max: usize },

    /// A required protocol field was absent.
    #[error("required field missing: {0}")]
    RequiredFieldMissing(&'static str),

    /// The frame schema does not match the expected schema.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// The frame violates the message ordering contract.
    #[error("order violation: {0}")]
    OrderViolation(String),
}

/// Reasons a session or resume attempt may fail.
#[derive(Debug, Error)]
pub enum SessionReject {
    /// The requested session does not exist.
    #[error("session not found: {0}")]
    NotFound(String),

    /// The session has expired.
    #[error("session expired")]
    Expired,

    /// The client's epoch does not match the server's.
    #[error("session epoch conflict: incoming={incoming}, current={current}")]
    Conflict { incoming: u32, current: u32 },

    /// The resume attempt was rejected.
    #[error("resume rejected: {0}")]
    ResumeRejected(String),

    /// The requested replay offset is no longer available.
    #[error("replay offset expired: topic={topic}, requested={requested}")]
    ReplayOffsetExpired { topic: String, requested: i64 },

    /// A full snapshot is required before continuing.
    #[error("snapshot required for topic: {0}")]
    SnapshotRequired(String),
}

/// Reasons a topic operation may fail.
#[derive(Debug, Error)]
pub enum TopicReject {
    /// The topic does not exist.
    #[error("topic not found: {0}")]
    NotFound(String),

    /// The topic has been closed and no longer accepts messages.
    #[error("topic closed: {0}")]
    Closed(String),

    /// The topic is currently overloaded.
    #[error("topic overloaded: {0}")]
    Overloaded(String),

    /// The topic has reached its maximum number of subscribers.
    #[error("topic subscriber limit reached: {0}")]
    SubscriberLimit(String),

    /// The topic has reached its maximum number of publishers.
    #[error("topic publisher limit reached: {0}")]
    PublisherLimit(String),

    /// The caller does not have permission to access this topic.
    #[error("topic forbidden: {0}")]
    Forbidden(String),

    /// The topic is rate-limited.
    #[error("topic rate limited: {0}")]
    RateLimited(String),

    /// The topic name is invalid.
    #[error("invalid topic name: {0}")]
    InvalidName(String),
}

/// Reasons a message may be rejected.
#[derive(Debug, Error)]
pub enum MessageReject {
    /// A message with this dedupe key was already processed.
    #[error("duplicate message: id={0}")]
    Duplicate(String),

    /// The message has expired.
    #[error("message expired")]
    Expired,

    /// The message was rejected by application logic.
    #[error("message rejected: {0}")]
    Rejected(String),

    /// The message exceeds the maximum size.
    #[error("message too large: {actual} > {max}")]
    TooLarge { actual: usize, max: usize },

    /// The acknowledgement for this message timed out.
    #[error("ack timeout: id={0}")]
    AckTimeout(String),

    /// Delivery to at least one subscriber failed.
    #[error("delivery failed: {0}")]
    DeliveryFailed(String),
}

/// Authentication / authorization failures.
#[derive(Debug, Error)]
pub enum AuthReject {
    /// Authentication is required but was not provided.
    #[error("authentication required")]
    Required,

    /// The provided authentication is invalid.
    #[error("authentication invalid: {0}")]
    Invalid(String),

    /// The authentication has expired.
    #[error("authentication expired")]
    Expired,

    /// The authentication has been revoked.
    #[error("authentication revoked")]
    Revoked,

    /// The authenticated principal lacks permission.
    #[error("permission denied: {0}")]
    Denied(String),
}

/// System-level failures.
#[derive(Debug, Error)]
pub enum SystemReject {
    /// The system is currently overloaded.
    #[error("system overloaded")]
    Overloaded,

    /// The system is under maintenance.
    #[error("system maintenance")]
    Maintenance,

    /// The responsible shard has moved to a different node.
    #[error("shard moved: topic={0}")]
    ShardMoved(String),

    /// This region is currently unavailable.
    #[error("region unavailable: {0}")]
    RegionUnavailable(String),

    /// An internal error occurred.
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
    /// Frame-level error.
    #[error(transparent)]
    Frame(#[from] FrameReject),

    /// Session-level error.
    #[error(transparent)]
    Session(#[from] SessionReject),

    /// Topic-level error.
    #[error(transparent)]
    Topic(#[from] TopicReject),

    /// Message-level error.
    #[error(transparent)]
    Message(#[from] MessageReject),

    /// Authentication error.
    #[error(transparent)]
    Auth(#[from] AuthReject),

    /// System-level error.
    #[error(transparent)]
    System(#[from] SystemReject),

    /// Configuration error.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    /// I/O error from the operating system or transport.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// serde_json serialization/deserialization error.
    #[error("serde_json error: {0}")]
    SerdeJson(BoxedStdError),

    /// CBOR serialization error.
    #[error("ciborium error: {0}")]
    Cbor(BoxedStdError),

    /// CBOR deserialization error.
    #[error("ciborium de error: {0}")]
    CborDe(BoxedStdError),

    /// WebSocket transport error.
    #[error("websocket error: {0}")]
    WebSocket(BoxedStdError),

    /// Catch-all for other error types.
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

#[cfg(feature = "websocket")]
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
    /// Wrap an arbitrary error into `RiftError::Other`.
    pub fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Other(BoxedStdError(Box::new(e)))
    }

    /// Construct from a `serde_json::Error` without the `From` impl
    /// (useful when the compiler cannot infer the type).
    pub fn from_serde_json(e: serde_json::Error) -> Self {
        Self::SerdeJson(BoxedStdError(Box::new(e)))
    }

    /// Construct from a CBOR serialization error.
    pub fn from_cbor_ser<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Cbor(BoxedStdError(Box::new(e)))
    }

    /// Construct from a CBOR deserialization error.
    pub fn from_cbor_de<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::CborDe(BoxedStdError(Box::new(e)))
    }
}

/// Invalid server configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A configuration field has an invalid value.
    #[error("invalid value for {field}: {message}")]
    Invalid {
        /// The name of the invalid field.
        field: &'static str,
        /// A human-readable explanation.
        message: String,
    },
}
