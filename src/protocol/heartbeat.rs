//! Heartbeat policy — spec §21.

use std::time::Duration;

/// Heartbeat configuration sent to the client during `ready`.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatPolicy {
    /// Server expects a ping at least this often.
    pub ping_interval: Duration,
    /// If a pong is not received within this window, the connection is
    /// considered degraded.
    pub pong_timeout: Duration,
    /// How many consecutive missed pongs before the connection is
    /// closed.
    pub max_missed_pongs: u32,
    /// Idle connection timeout.
    pub idle_timeout: Duration,
    /// Client-side jitter for heartbeat distribution.
    pub jitter: Duration,
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        // spec §27.1 default for ordinary Web apps.
        Self {
            ping_interval: Duration::from_millis(25_000),
            pong_timeout: Duration::from_millis(10_000),
            max_missed_pongs: 2,
            idle_timeout: Duration::from_secs(300),
            jitter: Duration::from_millis(2_500),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let h = HeartbeatPolicy::default();
        assert_eq!(h.ping_interval, Duration::from_millis(25_000));
        assert_eq!(h.pong_timeout, Duration::from_millis(10_000));
    }
}
