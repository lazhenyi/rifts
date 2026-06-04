//! Per-topic monotonic offset cursor (spec §13.1).
//!
//! In a single-process deployment this is the same value the
//! `TopicEntry` holds in its `next_offset` atomic; the `OffsetStore`
//! exists as a separate component so that a distributed broker can
//! back it with shared storage.

use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;

#[derive(Debug, Default)]
pub struct OffsetStore {
    inner: DashMap<String, AtomicI64>,
}

impl OffsetStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate the next offset for `topic` and return it.
    pub fn alloc(&self, topic: &str) -> i64 {
        let cell = self
            .inner
            .entry(topic.to_string())
            .or_insert_with(|| AtomicI64::new(1));
        cell.fetch_add(1, Ordering::SeqCst)
    }

    /// Return the highest allocated offset for `topic` (0 if none).
    pub fn head(&self, topic: &str) -> i64 {
        self.inner
            .get(topic)
            .map(|c| c.load(Ordering::SeqCst) - 1)
            .unwrap_or(0)
    }

    /// Drop a topic.
    pub fn remove(&self, topic: &str) {
        self.inner.remove(topic);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_monotonic() {
        let s = OffsetStore::new();
        assert_eq!(s.alloc("t"), 1);
        assert_eq!(s.alloc("t"), 2);
        assert_eq!(s.alloc("t"), 3);
        assert_eq!(s.head("t"), 3);
    }

    #[test]
    fn per_topic() {
        let s = OffsetStore::new();
        s.alloc("a");
        s.alloc("a");
        s.alloc("b");
        assert_eq!(s.head("a"), 2);
        assert_eq!(s.head("b"), 1);
    }
}
