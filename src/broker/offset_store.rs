//! Per-topic monotonic offset cursor — spec section 13.1.
//!
//! This module provides the [`OffsetStore`] struct, which allocates
//! monotonically increasing offsets for messages published to each
//! topic. Offsets start at 1 for the first message in a topic and
//! increment by 1 for each subsequent message.
//!
//! # Design
//!
//! In a single-process deployment, this store is the same value the
//! [`TopicEntry`](crate::topic::TopicEntry) holds in its `next_offset`
//! atomic. The [`OffsetStore`] exists as a separate component so that
//! a distributed broker can replace it with a shared-storage
//! implementation (e.g. a distributed counter or a database sequence)
//! without changing the broker logic.
//!
//! # Thread safety
//!
//! Each topic's offset counter is an [`AtomicI64`], making concurrent
//! allocation from multiple threads safe without external locking. The
//! topic-to-counter map uses a [`DashMap`] for concurrent shard-level
//! access.
//!
//! # Offset semantics
//!
//! The [`alloc`](OffsetStore::alloc) method atomically increments the
//! counter and returns the *previous* value, so the first call for a
//! new topic returns 1. The [`head`](OffsetStore::head) method returns
//! the highest allocated offset (counter minus 1), or 0 if no
//! allocations have been made for the topic.

use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;

/// Per-topic monotonic offset allocator.
///
/// Tracks a separate atomic counter for each topic. Offsets are
/// allocated via [`alloc`](OffsetStore::alloc) and queried via
/// [`head`](OffsetStore::head). The store is thread-safe and can be
/// shared across async tasks.
///
/// # Examples
///
/// ```ignore
/// use rifts::broker::OffsetStore;
///
/// let store = OffsetStore::new();
/// assert_eq!(store.alloc("orders"), 1);
/// assert_eq!(store.alloc("orders"), 2);
/// assert_eq!(store.alloc("orders"), 3);
/// assert_eq!(store.head("orders"), 3);
/// ```
#[derive(Debug, Default)]
pub struct OffsetStore {
    /// Concurrent map from topic name to an atomic offset counter.
    /// The counter stores the *next* offset to be allocated (i.e.
    /// one more than the last allocated offset). A counter value of
    /// 1 means no offsets have been allocated yet.
    inner: DashMap<String, AtomicI64>,
}

impl OffsetStore {
    /// Create an empty offset store with no tracked topics.
    ///
    /// Topics are registered lazily when `alloc` is first called for
    /// them.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate the next offset for the given topic and return it.
    ///
    /// If the topic has not been seen before, a new counter is
    /// initialized starting at 1. The counter is atomically
    /// incremented and the *previous* value is returned, so the first
    /// call always returns 1.
    ///
    /// This method is safe to call concurrently from multiple threads
    /// for the same topic; each call will receive a unique,
    /// monotonically increasing offset.
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic name for which to allocate an offset.
    pub fn alloc(&self, topic: &str) -> i64 {
        let cell = self
            .inner
            .entry(topic.to_string())
            .or_insert_with(|| AtomicI64::new(1));
        cell.fetch_add(1, Ordering::SeqCst)
    }

    /// Return the highest allocated offset for the given topic.
    ///
    /// Returns the offset of the most recently allocated message, or
    /// `0` if no messages have been allocated for the topic yet. This
    /// is equivalent to reading the atomic counter and subtracting 1.
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic name to query.
    pub fn head(&self, topic: &str) -> i64 {
        self.inner
            .get(topic)
            .map(|c| c.load(Ordering::SeqCst) - 1)
            .unwrap_or(0)
    }

    /// Remove the offset counter for a topic.
    ///
    /// Drops the entry from the internal map, freeing the associated
    /// memory. Subsequent calls to [`alloc`](OffsetStore::alloc) for
    /// the same topic will start from 1 again.
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic name to remove.
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
