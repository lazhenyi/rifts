//! Server configuration. Defaults follow the recommended values in
//! spec §27.1 ("普通 Web 应用" — ordinary Web application).

use std::time::Duration;

use crate::protocol::heartbeat::HeartbeatPolicy;

/// Server-side configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Maximum payload size in bytes (spec §27.1 default: 65536).
    pub max_payload_bytes: usize,

    /// Maximum number of topic subscriptions per connection
    /// (spec §27.1 default: 128).
    pub max_topics_per_connection: usize,

    /// Maximum per-connection outbound send queue size in bytes
    /// (spec §27.1 default: 1 MiB).
    pub max_send_queue_bytes: usize,

    /// Heartbeat policy.
    pub heartbeat: HeartbeatPolicy,

    /// Connection idle timeout — closed if no traffic within this window.
    pub idle_timeout: Duration,

    /// Base reconnect interval for client guidance.
    pub reconnect_base_ms: u32,

    /// Maximum reconnect interval for client guidance.
    pub reconnect_max_ms: u32,

    /// Replay window — how long offsets are kept for replay (spec §27.1
    /// default: 300 s).
    pub replay_window: Duration,

    /// Dedupe window — how long a dedupe_key is remembered.
    pub dedupe_window: Duration,

    /// Maximum number of failed authentication attempts before the
    /// connection is closed.
    pub max_auth_failures: u32,

    /// Optional list of allowed codec names for negotiation.
    /// If empty, all supported codecs are offered.
    pub codec_offer: Vec<CodecOffer>,

    /// Default topic profile applied when a topic is auto-created on
    /// first subscribe.
    pub default_topic_profile: DefaultTopicProfile,
}

/// Codecs offered to the client during hello negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecOffer {
    Json,
    Cbor,
}

/// Default topic profile applied on auto-create.
#[derive(Debug, Clone)]
pub struct DefaultTopicProfile {
    pub retention: crate::topic::retention::RetentionPolicy,
    pub ordering: crate::topic::ordering::OrderingPolicy,
    pub max_subscribers: usize,
    pub max_publishers: usize,
    pub replay_enabled: bool,
    pub snapshot_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 65_536,
            max_topics_per_connection: 128,
            max_send_queue_bytes: 1_048_576,
            heartbeat: HeartbeatPolicy::default(),
            idle_timeout: Duration::from_secs(300),
            reconnect_base_ms: 500,
            reconnect_max_ms: 15_000,
            replay_window: Duration::from_secs(300),
            dedupe_window: Duration::from_secs(60),
            max_auth_failures: 3,
            codec_offer: Vec::new(),
            default_topic_profile: DefaultTopicProfile::default(),
        }
    }
}

impl Default for DefaultTopicProfile {
    fn default() -> Self {
        Self {
            retention: crate::topic::retention::RetentionPolicy::Latest,
            ordering: crate::topic::ordering::OrderingPolicy::Topic,
            max_subscribers: 10_000,
            max_publishers: 10_000,
            replay_enabled: true,
            snapshot_enabled: true,
        }
    }
}
