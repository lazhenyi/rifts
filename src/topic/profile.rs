//! Topic profile — spec §9.2.

use std::time::Duration;

use crate::topic::ordering::OrderingPolicy;
use crate::topic::retention::RetentionPolicy;

/// A topic profile defines the policy for a single topic.
#[derive(Debug, Clone)]
pub struct TopicProfile {
    /// Profile name (human-readable).
    pub name: String,
    /// Retention policy.
    pub retention: RetentionPolicy,
    /// Ordering policy.
    pub ordering: OrderingPolicy,
    /// Maximum number of subscribers.
    pub max_subscribers: usize,
    /// Maximum number of concurrent publishers.
    pub max_publishers: usize,
    /// Per-publisher rate limit (messages per second). `None` = no limit.
    pub rate_limit_per_publisher: Option<u32>,
    /// Per-topic total rate limit (messages per second). `None` = no limit.
    pub rate_limit_total: Option<u32>,
    /// Whether replay is supported.
    pub replay_enabled: bool,
    /// Whether snapshots are supported.
    pub snapshot_enabled: bool,
    /// Replay window — how long messages remain replayable.
    pub replay_window: Duration,
}

impl Default for TopicProfile {
    fn default() -> Self {
        Self {
            name: "default".into(),
            retention: RetentionPolicy::Latest,
            ordering: OrderingPolicy::Topic,
            max_subscribers: 10_000,
            max_publishers: 10_000,
            rate_limit_per_publisher: None,
            rate_limit_total: None,
            replay_enabled: true,
            snapshot_enabled: true,
            replay_window: Duration::from_secs(300),
        }
    }
}
