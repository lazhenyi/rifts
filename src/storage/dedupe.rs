//! # Message Deduplication Store
//!
//! This module implements message deduplication, which detects and discards
//! duplicate messages within a configurable time window. This corresponds to
//! **specification section 11.2**.
//!
//! ## How Deduplication Works
//!
//! Every message carries a `dedupe_key` (typically the message ID). The
//! deduplication store records each key the first time it is seen along with an
//! expiry timestamp computed from `now + window`. When the same key arrives
//! again within the window, [`DedupeStore::check_and_record`] returns `false`,
//! signaling the caller to skip the message. If the recorded entry has already
//! expired, the message is treated as fresh and the window is extended.
//!
//! ## Implementations
//!
//! - [`MemoryDedupeStore`] -- an in-memory deduplication table backed by
//!   `DashMap`. Uses the `entry()` API for atomic insert-or-update, making it
//!   safe for concurrent access without external locking. Suitable for
//!   single-process deployments.
//! - [`SledDedupeStore`] -- a durable deduplication store backed by
//!   [`SledEngine`](crate::storage::SledEngine). Requires the `sled` Cargo
//!   feature. Suitable when deduplication state must survive broker restarts.
//!
//! ## Expiry Sweeping
//!
//! Both implementations expose a [`DedupeStore::sweep`] method that removes all
//! entries whose expiry timestamp has passed. Callers should invoke `sweep`
//! periodically (e.g. via a timer) to prevent unbounded growth of the
//! deduplication table in memory or on disk.

use std::time::Duration;

use dashmap::DashMap;

use crate::now_ms;

/// Trait defining the contract for message deduplication stores.
///
/// Implementations record which message keys have been seen recently and
/// reject duplicates that arrive within a configurable time window. This
/// trait is the core abstraction that enables both in-memory and durable
/// deduplication strategies to be used interchangeably by the broker.
pub trait DedupeStore: Send + Sync {
    /// Check whether `key` under `topic` is fresh and, if so, record it.
    ///
    /// Returns `true` if the key has not been seen within `window` (or its
    /// previous record has expired), meaning the message should be processed.
    /// Returns `false` if the key is a duplicate within the active window,
    /// meaning the message should be skipped.
    ///
    /// The operation is atomic: the check and the record happen in a single
    /// step so that concurrent callers racing on the same key will never both
    /// see a "fresh" result.
    ///
    /// # Parameters
    ///
    /// - `topic` -- the topic namespace for the key.
    /// - `key` -- the deduplication key (typically a message ID).
    /// - `window` -- the duration for which a seen key remains valid.
    fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool;

    /// Remove all entries whose expiry timestamp has passed.
    ///
    /// Returns the number of entries that were removed. Call this
    /// periodically (e.g. from a background timer) to reclaim memory or
    /// disk space consumed by stale deduplication records.
    fn sweep(&self) -> usize;
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory deduplication store backed by a concurrent `DashMap`.
///
/// Each entry maps a `(topic, key)` pair to an absolute expiry timestamp
/// in milliseconds. The `DashMap::entry()` API guarantees atomicity: when
/// two threads race on the same key, exactly one closure executes, avoiding
/// the check-then-act race that a two-step `get_mut` + `insert` would have.
///
/// # Examples
///
/// ```ignore
/// use std::time::Duration;
/// use rifts::storage::{DedupeStore, MemoryDedupeStore};
///
/// let store = MemoryDedupeStore::new();
/// let window = Duration::from_secs(60);
///
/// assert!(store.check_and_record("orders", "msg-42", window));   // fresh
/// assert!(!store.check_and_record("orders", "msg-42", window));  // duplicate
/// ```
#[derive(Debug, Default)]
pub struct MemoryDedupeStore {
    /// Maps `(topic, key)` to an absolute expiry timestamp in milliseconds.
    inner: DashMap<(String, String), i64>,
}

impl MemoryDedupeStore {
    /// Create a new, empty in-memory deduplication store.
    ///
    /// The store starts with no entries and will grow as messages are
    /// recorded. Use [`DedupeStore::sweep`] periodically to remove
    /// expired entries and reclaim memory.
    pub fn new() -> Self {
        Self::default()
    }
}

impl DedupeStore for MemoryDedupeStore {
    fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let now = now_ms();
        let expires = now + window.as_millis() as i64;
        let k = (topic.to_string(), key.to_string());
        // DashMap 6.x entry() is atomic: the and_modify and or_insert_with
        // closures are mutually exclusive -- only one closure executes per
        // key. This eliminates the race window between a two-step
        // get_mut -> insert approach.
        let mut is_fresh = false;
        self.inner
            .entry(k)
            .and_modify(|v| {
                if *v <= now {
                    *v = expires;
                    is_fresh = true;
                }
            })
            .or_insert_with(|| {
                is_fresh = true;
                expires
            });
        is_fresh
    }

    fn sweep(&self) -> usize {
        let now = now_ms();
        let expired: Vec<(String, String)> = self
            .inner
            .iter()
            .filter(|kv| *kv.value() <= now)
            .map(|kv| (kv.key().0.clone(), kv.key().1.clone()))
            .collect();
        let mut removed = 0;
        for k in expired {
            if self.inner.remove(&k).is_some() {
                removed += 1;
            }
        }
        removed
    }
}

// ── Sled-backed ──────────────────────────────────────────────

#[cfg(feature = "sled")]
mod sled_impl {
    //! Sled-backed deduplication store implementation.
    //!
    //! This sub-module provides [`SledDedupeStore`], a durable
    //! deduplication store that persists entries to disk via a
    //! [`SledEngine`](crate::storage::SledEngine). Because the state
    //! survives broker restarts, it is suitable for production deployments
    //! where duplicate detection must continue across process lifetimes.

    use super::*;
    use crate::storage::encode;
    use crate::storage::engine::SledEngine;
    use crate::storage::engine::StorageEngine;

    /// Durable deduplication store backed by a [`SledEngine`].
    ///
    /// Each deduplication entry is stored as a key-value pair where the key
    /// is produced by [`encode::dedupe_key`] and the value is the 8-byte
    /// big-endian expiry timestamp. Because sled persists data to disk, the
    /// deduplication state survives broker restarts.
    ///
    /// # Key Layout
    ///
    /// Key: `<topic>\x00<message_id>\x00`
    /// Value: `<expiry_timestamp: i64, big-endian>`
    pub struct SledDedupeStore {
        /// The underlying byte-oriented storage engine.
        engine: SledEngine,
    }

    impl SledDedupeStore {
        /// Create a new sled-backed deduplication store from the given engine.
        ///
        /// The engine should be a dedicated tree for deduplication entries,
        /// opened from the same `sled::Db` instance used by other stores.
        ///
        /// # Parameters
        ///
        /// - `engine` -- a [`SledEngine`] instance (typically a dedicated tree
        ///   for deduplication entries).
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    impl DedupeStore for SledDedupeStore {
        /// Atomically check whether `key` under `topic` is fresh and, if so,
        /// record it in the sled tree.
        ///
        /// Uses sled's `compare_and_swap` so that two concurrent callers
        /// racing on the same key will never both observe a "fresh"
        /// result. The CAS loop writes the new expiry only if the current
        /// value is either absent or already expired, matching the
        /// `DedupeStore` trait contract.
        fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
            let now = now_ms();
            let expires = now + window.as_millis() as i64;
            let k = encode::dedupe_key(topic, key);
            let new_bytes = expires.to_be_bytes();

            // Loop: if the entry is absent (None) or expired (<= now), try
            // to atomically replace it with the new expiry. If the existing
            // value is fresh (> now), the CAS fails and we return false.
            loop {
                let expected: Option<Vec<u8>> = match self.engine.get(&k) {
                    None => None,
                    Some(v) if v.len() >= 8 => {
                        let prev = i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8]));
                        if prev > now {
                            return false;
                        }
                        Some(v)
                    }
                    // Corrupt/short value: treat as absent.
                    Some(_) => None,
                };
                match self.engine.cas(k.clone(), expected, new_bytes.to_vec()) {
                    Ok(Ok(())) => return true,
                    Ok(Err(_)) => {
                        // Another writer raced us; re-read and retry.
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "sled CAS failed in dedupe");
                        return false;
                    }
                }
            }
        }

        /// Scan deduplication entries for the given topics and remove those
        /// whose expiry timestamp has passed.
        ///
        /// Topics with no entries are skipped to keep the scan cost
        /// proportional to live data, not to engine size.
        fn sweep(&self) -> usize {
            let now = now_ms();
            let mut total = 0;
            // Iterate per-topic using `dedupe_prefix` so we don't scan
            // unrelated engine trees. The trait does not surface the topic
            // list, so we collect distinct topics from a one-time full scan
            // of the prefix-namespace, then sweep each topic's slice.
            //
            // Because sled trees are per-store (this engine is shared with
            // other dedupe users in principle), we read distinct topic
            // names by scanning the engine prefix and grouping by the
            // bytes before the first separator.
            let all = self.engine.scan_prefix(&[]);
            let mut topics: std::collections::HashSet<String> = std::collections::HashSet::new();
            for (k, _) in &all {
                if let Some(sep_pos) = k.iter().position(|&b| b == encode::SEP) {
                    if let Ok(t) = std::str::from_utf8(&k[..sep_pos]) {
                        topics.insert(t.to_string());
                    }
                }
            }
            for topic in topics {
                let prefix = encode::dedupe_prefix(&topic);
                let expired: Vec<Vec<u8>> = self
                    .engine
                    .scan_prefix(&prefix)
                    .into_iter()
                    .filter(|(_, v)| {
                        v.len() >= 8
                            && i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8])) <= now
                    })
                    .map(|(k, _)| k)
                    .collect();
                for k in &expired {
                    self.engine.delete(k);
                }
                total += expired.len();
            }
            total
        }
    }
}

#[cfg(feature = "sled")]
pub use sled_impl::SledDedupeStore;

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_fresh_then_duplicate(store: &dyn DedupeStore) {
        let w = Duration::from_secs(60);
        assert!(store.check_and_record("t", "k", w));
        assert!(!store.check_and_record("t", "k", w));
    }

    fn test_different_topics(store: &dyn DedupeStore) {
        let w = Duration::from_secs(60);
        assert!(store.check_and_record("t1", "k", w));
        assert!(store.check_and_record("t2", "k", w));
    }

    #[test]
    fn memory_fresh_then_duplicate() {
        test_fresh_then_duplicate(&MemoryDedupeStore::new());
    }

    #[test]
    fn memory_different_topics() {
        test_different_topics(&MemoryDedupeStore::new());
    }

    #[test]
    fn memory_sweep_removes_expired() {
        let d = MemoryDedupeStore::new();
        d.check_and_record("t", "k", Duration::from_millis(0));
        // Force expire
        d.inner.insert(("t".into(), "k".into()), 0);
        let removed = d.sweep();
        assert_eq!(removed, 1);
    }

    #[test]
    fn concurrent_dedup_returns_one_fresh() {
        use std::sync::Arc;
        use std::thread;
        let store = Arc::new(MemoryDedupeStore::new());
        let w = Duration::from_secs(60);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let s = store.clone();
                thread::spawn(move || s.check_and_record("t", "k", w))
            })
            .collect();
        let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let fresh_count = results.iter().filter(|&&b| b).count();
        assert_eq!(fresh_count, 1, "exactly one thread should see fresh");
    }
}
