//! Ordering policy (spec §9.4).

use std::fmt;

/// How messages within a topic are ordered.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OrderingPolicy {
    /// No ordering guarantee.
    None,
    /// Ordered within a single connection.
    Connection,
    /// Ordered within a single publisher.
    Publisher,
    /// Globally ordered within a topic.
    #[default]
    Topic,
    /// Ordered by `ordering_key`.
    Key,
    /// Causally ordered (requires vector metadata).
    Causal,
}

impl fmt::Display for OrderingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OrderingPolicy::None => "none",
            OrderingPolicy::Connection => "connection",
            OrderingPolicy::Publisher => "publisher",
            OrderingPolicy::Topic => "topic",
            OrderingPolicy::Key => "key",
            OrderingPolicy::Causal => "causal",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_topic() {
        assert_eq!(OrderingPolicy::default(), OrderingPolicy::Topic);
    }
}
