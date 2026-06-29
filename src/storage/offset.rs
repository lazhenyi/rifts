//! # Per-Topic Monotonic Offset Store
//!
//! This module provides the [`OffsetStore`] trait and its implementations for
//! allocating monotonically increasing offsets on a per-topic basis. This
//! corresponds to **specification section 13.1**.
//!
//! ## How Offsets Work
//!
//! Each topic maintains a single integer "head" counter. When a new message
//! arrives, [`OffsetStore::alloc`] atomically increments the counter and
//! returns the new value. The first call for any topic returns `1`, the
//! second returns `2`, and so on. Because the counter is monotonically
//! increasing, offsets are guaranteed to be unique within a topic.
//!
//! [`OffsetStore::head`] returns the highest allocated offset without
//! incrementing it, or `0` if no offset has been allocated for the topic.
//!
//! ## Implementations
//!
//! - [`MemoryOffsetStore`] -- an in-memory store backed by a
//!   `DashMap<String, AtomicI64>`. The atomic counter ensures lock-free
//!   concurrent allocation. Suitable for development and single-process
//!   deployments.
//! - [`SledOffsetStore`] -- a durable store backed by
//!   [`SledEngine`](crate::storage::SledEngine). Requires the `sled`
//!   Cargo feature. Uses an in-memory cache to avoid reading from sled
//!   on every allocation, while persisting the head value to disk so that
//!   offsets are not reset across broker restarts.

use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;

/// Trait for per-topic monotonic offset allocation.
///
/// Implementations guarantee that each call to [`alloc`](OffsetStore::alloc)
/// returns a value strictly greater than all previously returned values for
/// the same topic, starting from `1`.
pub trait OffsetStore: Send + Sync {
    /// Allocate the next offset for `topic` and return it.
    ///
    /// The first call for any topic returns `1`. Subsequent calls return
    /// `2`, `3`, etc. The returned offset is guaranteed to be unique and
    /// monotonically increasing within the topic.
    fn alloc(&self, topic: &str) -> i64;

    /// Return the highest allocated offset for `topic`, or `0` if no
    /// offset has been allocated yet.
    ///
    /// This does **not** allocate a new offset; it is a read-only
    /// operation.
    fn head(&self, topic: &str) -> i64;

    /// Remove all offset state for the given topic.
    ///
    /// After this call, the next [`alloc`](OffsetStore::alloc) for the
    /// topic will start again from `1`. This is typically called when a
    /// topic is deleted.
    fn remove(&self, topic: &str);
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory offset store backed by a concurrent `DashMap`.
///
/// Each topic's head counter is stored as an `AtomicI64` initialized to
/// `1` on first access. Allocation uses `fetch_add` with `SeqCst`
/// ordering, making it safe for concurrent use without external locking.
///
/// # Thread Safety
///
/// The atomic counter guarantees that concurrent calls to
/// [`OffsetStore::alloc`] for the same topic will each receive a unique
/// offset with no gaps or duplicates.
#[derive(Debug, Default)]
pub struct MemoryOffsetStore {
    /// Maps topic name to its current atomic offset counter.
    /// The stored value is the *next* offset to hand out (i.e. one more
    /// than the highest allocated offset).
    inner: DashMap<String, AtomicI64>,
}

impl MemoryOffsetStore {
    /// Create a new, empty in-memory offset store.
    ///
    /// Topics are created lazily on the first call to
    /// [`OffsetStore::alloc`] for that topic.
    pub fn new() -> Self {
        Self::default()
    }
}

impl OffsetStore for MemoryOffsetStore {
    fn alloc(&self, topic: &str) -> i64 {
        self.inner
            .entry(topic.to_string())
            .or_insert_with(|| AtomicI64::new(1))
            .fetch_add(1, Ordering::SeqCst)
    }

    fn head(&self, topic: &str) -> i64 {
        self.inner
            .get(topic)
            .map(|c| c.load(Ordering::SeqCst) - 1)
            .unwrap_or(0)
    }

    fn remove(&self, topic: &str) {
        self.inner.remove(topic);
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    //! Sled-backed offset store implementation.
    //!
    //! This sub-module provides [`SledOffsetStore`], a durable offset
    //! store that persists head values to disk via a
    //! [`SledEngine`](crate::storage::SledEngine). An in-memory
    //! `HashMap` cache avoids sled reads on every allocation call,
    //! while the persisted value ensures offsets survive broker restarts.

    use super::*;
    use crate::storage::encode;
    use crate::storage::engine::SledEngine;
    use crate::storage::engine::StorageEngine;

    /// Sled-backed offset store. One key per topic: `<topic>\x00head`.
    ///
    /// The head value is persisted as an 8-byte big-endian `i64`. An
    /// in-memory `HashMap` cache mirrors the persisted values to avoid
    /// a sled read on every [`alloc`](OffsetStore::alloc) call. The cache
    /// is warmed from existing sled data at construction time.
    ///
    /// # Persistence Guarantee
    ///
    /// Because the head is written to sled on every allocation, the
    /// offset sequence will not reset to `1` if the broker restarts.
    /// However, there is no fsync per write; callers requiring strict
    /// durability should call [`SledEngine::flush`](crate::storage::SledEngine::flush)
    /// periodically.
    pub struct SledOffsetStore {
        /// The underlying byte-oriented storage engine.
        engine: SledEngine,
        /// In-memory cache to avoid sled reads on every `alloc`.
        cache: parking_lot::Mutex<std::collections::HashMap<String, i64>>,
    }

    impl SledOffsetStore {
        /// Create a new sled-backed offset store from the given engine.
        ///
        /// The constructor warms the in-memory cache by scanning for all
        /// existing `head` entries in the engine. This ensures that topics
        /// written by a previous process instance continue their offset
        /// sequence without resetting.
        ///
        /// # Parameters
        ///
        /// - `engine` -- a [`SledEngine`] instance (typically a dedicated
        ///   tree for offset entries).
        pub fn new(engine: SledEngine) -> Self {
            let mut cache = std::collections::HashMap::new();
            // Warm cache from existing data.
            for (key, value) in engine
                .scan_prefix(&[])
                .iter()
                .filter(|(k, _)| k.ends_with(b"head"))
            {
                let topic = String::from_utf8_lossy(&key[..key.len() - 5]).to_string();
                if value.len() >= 8 {
                    let head = i64::from_be_bytes(value[..8].try_into().unwrap_or([0; 8]));
                    cache.insert(topic, head);
                }
            }
            Self {
                engine,
                cache: parking_lot::Mutex::new(cache),
            }
        }
    }

    impl OffsetStore for SledOffsetStore {
        /// Allocate the next offset for `topic`.
        ///
        /// Checks the in-memory cache first. On a cache miss (e.g. a
        /// topic written by a previous process instance), reads the
        /// persisted head from the engine to avoid resetting the offset
        /// sequence. The new head is then written to both the cache and
        /// the engine.
        fn alloc(&self, topic: &str) -> i64 {
            let mut cache = self.cache.lock();
            let next = if let Some(&h) = cache.get(topic) {
                h + 1
            } else {
                // Cache miss: fall back to the engine so a topic that
                // was written by a previous process instance does not
                // have its offset sequence reset to 1 (which would
                // violate monotonicity).
                let key = encode::offset_key(topic);
                let real_head = self
                    .engine
                    .get(&key)
                    .and_then(|v| {
                        if v.len() >= 8 {
                            Some(i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8])))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                real_head + 1
            };
            cache.insert(topic.to_string(), next);
            let key = encode::offset_key(topic);
            self.engine.put(&key, &next.to_be_bytes());
            next
        }

        /// Return the highest allocated offset for `topic`.
        ///
        /// Checks the in-memory cache first; on a miss, reads the
        /// persisted value from the engine. Returns `0` if the topic
        /// has never been allocated.
        fn head(&self, topic: &str) -> i64 {
            self.cache.lock().get(topic).copied().unwrap_or_else(|| {
                let key = encode::offset_key(topic);
                self.engine
                    .get(&key)
                    .and_then(|v| {
                        if v.len() >= 8 {
                            Some(i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8])))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0)
            })
        }

        /// Remove the offset state for `topic` from both the cache and
        /// the engine.
        fn remove(&self, topic: &str) {
            self.cache.lock().remove(topic);
            self.engine.delete(&encode::offset_key(topic));
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_impl::SledOffsetStore;

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_alloc_and_head(store: &dyn OffsetStore) {
        assert_eq!(store.head("t"), 0);
        assert_eq!(store.alloc("t"), 1);
        assert_eq!(store.alloc("t"), 2);
        assert_eq!(store.head("t"), 2);
        assert_eq!(store.alloc("u"), 1);
        assert_eq!(store.head("u"), 1);
    }

    fn test_remove(store: &dyn OffsetStore) {
        store.alloc("t");
        store.alloc("t");
        store.remove("t");
        assert_eq!(store.head("t"), 0);
    }

    #[test]
    fn memory_alloc_and_head() {
        test_alloc_and_head(&MemoryOffsetStore::new());
    }

    #[test]
    fn memory_remove() {
        test_remove(&MemoryOffsetStore::new());
    }
}
