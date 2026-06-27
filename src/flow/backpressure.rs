//! Per-connection backpressure — spec §18.1.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::frame::Priority;

/// Backpressure strategy applied when the send queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureStrategy {
    /// Wait until the queue drains.
    #[default]
    Pause,
    /// Drop the lowest-priority messages first.
    DropVolatile,
    /// Collapse state messages by `state_key` (kept latest only).
    CoalesceState,
    /// Lower delivery frequency (defer, reduce rate).
    Downgrade,
    /// Disconnect the slow consumer.
    Disconnect,
    /// Switch to snapshot polling.
    SnapshotLater,
}

/// State of a single connection's outbound queue.
pub struct BackpressureController {
    /// Max bytes the queue may hold.
    max_bytes: usize,
    /// Current queued byte count (AcqRel for accurate accounting).
    current_bytes: AtomicUsize,
    /// Strategy in effect.
    strategy: parking_lot::Mutex<BackpressureStrategy>,
    /// How many times we've applied backpressure.
    applied: AtomicUsize,
    /// How many messages have been dropped due to backpressure.
    dropped: AtomicUsize,
    /// How many slow-consumer disconnects have occurred.
    slow_consumer: AtomicUsize,
    /// How many flow-pause events have been emitted.
    flow_pause: AtomicUsize,
    /// How many flow-resume events have been emitted.
    flow_resume: AtomicUsize,
    /// How many volatile drops have occurred.
    volatile_drop: AtomicUsize,
    /// How many state coalescings have occurred.
    state_coalesce: AtomicUsize,
}

impl BackpressureController {
    /// Create a new controller with the given max queue capacity in
    /// bytes.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: AtomicUsize::new(0),
            strategy: parking_lot::Mutex::new(BackpressureStrategy::default()),
            applied: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            slow_consumer: AtomicUsize::new(0),
            flow_pause: AtomicUsize::new(0),
            flow_resume: AtomicUsize::new(0),
            volatile_drop: AtomicUsize::new(0),
            state_coalesce: AtomicUsize::new(0),
        }
    }

    /// Returns the max bytes capacity.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Returns the current byte count in the queue.
    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Acquire)
    }

    /// Returns the current strategy.
    pub fn strategy(&self) -> BackpressureStrategy {
        *self.strategy.lock()
    }

    /// Set the backpressure strategy.
    pub fn set_strategy(&self, s: BackpressureStrategy) {
        *self.strategy.lock() = s;
    }

    /// How much room is left in the queue.
    pub fn available(&self) -> usize {
        self.max_bytes.saturating_sub(self.current_bytes())
    }

    /// Returns `true` if the connection is currently over the high
    /// water mark (90%).
    pub fn is_overloaded(&self) -> bool {
        self.current_bytes() >= (self.max_bytes * 9) / 10
    }

    /// Try to enqueue a payload. Returns the action the caller should
    /// take, given the current strategy.
    ///
    /// Uses an atomic compare-and-swap loop to avoid the TOCTOU race
    /// between the capacity check and the increment.
    pub fn try_enqueue(&self, payload_bytes: usize) -> BackpressureAction {
        let mut prev = self.current_bytes.load(Ordering::Acquire);
        loop {
            if prev + payload_bytes <= self.max_bytes {
                match self.current_bytes.compare_exchange_weak(
                    prev,
                    prev + payload_bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return BackpressureAction::Accept,
                    Err(current) => prev = current,
                }
            } else {
                break;
            }
        }
        self.record_applied();
        match self.strategy() {
            BackpressureStrategy::Pause => {
                self.flow_pause.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::Pause
            }
            BackpressureStrategy::DropVolatile => {
                self.volatile_drop.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::DropVolatile
            }
            BackpressureStrategy::CoalesceState => {
                self.state_coalesce.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::CoalesceState
            }
            BackpressureStrategy::Downgrade => BackpressureAction::Downgrade,
            BackpressureStrategy::Disconnect => {
                self.slow_consumer.fetch_add(1, Ordering::Relaxed);
                BackpressureAction::Disconnect
            }
            BackpressureStrategy::SnapshotLater => BackpressureAction::SnapshotLater,
        }
    }

    /// Decrement the queue size after a message has been written.
    pub fn release(&self, bytes: usize) {
        let prev = self.current_bytes.fetch_sub(bytes, Ordering::AcqRel);
        // If we just crossed below the high water mark, record a
        // flow-resume event.
        if prev >= (self.max_bytes * 9) / 10
            && prev.saturating_sub(bytes) < (self.max_bytes * 9) / 10
        {
            self.flow_resume.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record that a backpressure decision was applied.
    pub fn record_applied(&self) {
        self.applied.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a message was dropped.
    pub fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of times backpressure was applied.
    pub fn applied(&self) -> usize {
        self.applied.load(Ordering::Relaxed)
    }

    /// Number of messages dropped.
    pub fn dropped(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of slow-consumer disconnects.
    pub fn slow_consumer_count(&self) -> usize {
        self.slow_consumer.load(Ordering::Relaxed)
    }

    /// Number of flow-pause events.
    pub fn flow_pause_count(&self) -> usize {
        self.flow_pause.load(Ordering::Relaxed)
    }

    /// Number of flow-resume events.
    pub fn flow_resume_count(&self) -> usize {
        self.flow_resume.load(Ordering::Relaxed)
    }

    /// Number of volatile drops.
    pub fn volatile_drop_count(&self) -> usize {
        self.volatile_drop.load(Ordering::Relaxed)
    }

    /// Number of state coalescing events.
    pub fn state_coalesce_count(&self) -> usize {
        self.state_coalesce.load(Ordering::Relaxed)
    }
}

/// What the caller should do when the queue cannot accept a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    /// The message was accepted into the queue.
    Accept,
    /// Caller should pause until capacity is available.
    Pause,
    /// Caller should drop messages marked as `Volatile` or `Background`.
    DropVolatile,
    /// Caller should coalesce state messages by key.
    CoalesceState,
    /// Caller should downgrade the connection.
    Downgrade,
    /// Caller should disconnect the client.
    Disconnect,
    /// Caller should switch to snapshot polling.
    SnapshotLater,
}

/// Whether a message's priority is eligible to be dropped under
/// `DropVolatile`.
pub fn is_volatile(p: Option<Priority>) -> bool {
    matches!(p, Some(Priority::Volatile) | Some(Priority::Background))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_under_limit() {
        let bp = BackpressureController::new(100);
        assert_eq!(bp.try_enqueue(50), BackpressureAction::Accept);
        assert_eq!(bp.try_enqueue(40), BackpressureAction::Accept);
        assert_eq!(bp.current_bytes(), 90);
    }

    #[test]
    fn pause_when_over_limit() {
        let bp = BackpressureController::new(100);
        bp.set_strategy(BackpressureStrategy::Pause);
        bp.try_enqueue(80);
        assert_eq!(bp.try_enqueue(50), BackpressureAction::Pause);
    }

    #[test]
    fn disconnect_strategy() {
        let bp = BackpressureController::new(100);
        bp.set_strategy(BackpressureStrategy::Disconnect);
        bp.try_enqueue(80);
        assert_eq!(bp.try_enqueue(50), BackpressureAction::Disconnect);
    }

    #[test]
    fn release_decrements() {
        let bp = BackpressureController::new(100);
        bp.try_enqueue(50);
        bp.release(30);
        assert_eq!(bp.current_bytes(), 20);
    }

    #[test]
    fn overloaded_detection() {
        let bp = BackpressureController::new(100);
        bp.try_enqueue(95);
        assert!(bp.is_overloaded());
        bp.release(10);
        assert!(!bp.is_overloaded());
    }

    #[test]
    fn volatile_filter() {
        assert!(is_volatile(Some(Priority::Volatile)));
        assert!(is_volatile(Some(Priority::Background)));
        assert!(!is_volatile(Some(Priority::Normal)));
        assert!(!is_volatile(None));
    }
}
