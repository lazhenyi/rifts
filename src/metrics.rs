//! Process-local metrics counters (spec §23.2).
//!
//! These are simple in-memory counters; in production they'd be
//! exported to Prometheus / OTLP. The structure mirrors the spec so
//! that mapping to a real exporter is straightforward.

use std::sync::atomic::{AtomicU64, Ordering};

/// All process-wide metrics.
#[derive(Default)]
pub struct Metrics {
    // Connection metrics
    pub active_connections: AtomicU64,
    pub connection_open_total: AtomicU64,
    pub connection_close_total: AtomicU64,
    pub reconnect_total: AtomicU64,
    pub resume_success_total: AtomicU64,
    pub resume_failed_total: AtomicU64,
    pub heartbeat_timeout_total: AtomicU64,

    // Message metrics
    pub messages_in_total: AtomicU64,
    pub messages_out_total: AtomicU64,
    pub messages_dropped_total: AtomicU64,
    pub messages_replayed_total: AtomicU64,
    pub messages_expired_total: AtomicU64,
    pub ack_timeout_total: AtomicU64,
    pub duplicate_total: AtomicU64,

    // Backpressure
    pub send_queue_depth: AtomicU64,
    pub recv_queue_depth: AtomicU64,
    pub slow_consumer_total: AtomicU64,
    pub flow_pause_total: AtomicU64,
    pub flow_resume_total: AtomicU64,
    pub volatile_drop_total: AtomicU64,
    pub state_coalesce_total: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field(
                "active_connections",
                &self.active_connections.load(Ordering::Relaxed),
            )
            .field(
                "connection_open_total",
                &self.connection_open_total.load(Ordering::Relaxed),
            )
            .field(
                "connection_close_total",
                &self.connection_close_total.load(Ordering::Relaxed),
            )
            .field(
                "messages_in_total",
                &self.messages_in_total.load(Ordering::Relaxed),
            )
            .field(
                "messages_out_total",
                &self.messages_out_total.load(Ordering::Relaxed),
            )
            .field(
                "messages_dropped_total",
                &self.messages_dropped_total.load(Ordering::Relaxed),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_and_add() {
        let m = Metrics::new();
        m.inc(&m.connection_open_total);
        m.inc(&m.connection_open_total);
        m.add(&m.messages_in_total, 7);
        assert_eq!(m.connection_open_total.load(Ordering::Relaxed), 2);
        assert_eq!(m.messages_in_total.load(Ordering::Relaxed), 7);
    }
}
