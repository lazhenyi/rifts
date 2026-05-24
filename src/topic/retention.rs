//! Retention policy (spec §9.3).

use std::time::Duration;

/// How long messages on a topic are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// No retention — messages are not kept after fanout.
    None,
    /// Retain for at most this duration.
    Ttl(Duration),
    /// Retain at most this many messages.
    Count(usize),
    /// Retain at most this many bytes.
    Size(usize),
    /// External durable storage controls retention.
    Durable,
    /// Only the latest value per state key is kept.
    Latest,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        RetentionPolicy::Latest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_latest() {
        assert_eq!(RetentionPolicy::default(), RetentionPolicy::Latest);
    }
}
