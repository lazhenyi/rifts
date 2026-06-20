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
    /// Current queued byte count.
    current_bytes: AtomicUsize,
    /// Strategy in effect.
    strategy: parking_lot::Mutex<BackpressureStrategy>,
    /// How many times we've applied backpressure.
    applied: AtomicUsize,
    /// How many messages have been dropped due to backpressure.
    dropped: AtomicUsize,
}

impl BackpressureController {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: AtomicUsize::new(0),
            strategy: parking_lot::Mutex::new(BackpressureStrategy::default()),
            applied: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        }
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::SeqCst)
    }

    pub fn strategy(&self) -> BackpressureStrategy {
        *self.strategy.lock()
    }

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
    pub fn try_enqueue(&self, payload_bytes: usize) -> BackpressureAction {
        if self.current_bytes() + payload_bytes <= self.max_bytes {
            self.current_bytes
                .fetch_add(payload_bytes, Ordering::SeqCst);
            return BackpressureAction::Accept;
        }
        match self.strategy() {
            BackpressureStrategy::Pause => BackpressureAction::Pause,
            BackpressureStrategy::DropVolatile => BackpressureAction::DropVolatile,
            BackpressureStrategy::CoalesceState => BackpressureAction::CoalesceState,
            BackpressureStrategy::Downgrade => BackpressureAction::Downgrade,
            BackpressureStrategy::Disconnect => BackpressureAction::Disconnect,
            BackpressureStrategy::SnapshotLater => BackpressureAction::SnapshotLater,
        }
    }

    /// Decrement the queue size after a message has been written.
    pub fn release(&self, bytes: usize) {
        self.current_bytes.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// Record that a backpressure decision was applied.
    pub fn record_applied(&self) {
        self.applied.fetch_add(1, Ordering::SeqCst);
    }

    /// Record that a message was dropped.
    pub fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }

    /// Counters.
    pub fn applied(&self) -> usize {
        self.applied.load(Ordering::SeqCst)
    }

    pub fn dropped(&self) -> usize {
        self.dropped.load(Ordering::SeqCst)
    }
}

/// What the caller should do when the queue cannot accept a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureAction {
    Accept,
    Pause,
    DropVolatile,
    CoalesceState,
    Downgrade,
    Disconnect,
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
