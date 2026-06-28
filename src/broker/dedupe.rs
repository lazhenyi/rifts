//! Deduplication store — spec section 11.2.
//!
//! This module implements a time-window-based message deduplication
//! mechanism. When a publisher sends a message with a deduplication key
//! (typically the `message_id` field), the dedupe store checks whether
//! that key has already been seen within a configurable time window for
//! the same topic. If it has, the message is marked as a duplicate and
//! is not fanned out to subscribers, even though it is still persisted
//! and assigned an offset.
//!
//! # Key design
//!
//! Entries are keyed by `(topic, dedupe_key)` tuples, allowing the same
//! deduplication key to be used independently across different topics.
//! Each entry stores the epoch millisecond at which it expires. The
//! [`DedupeStore::check_and_record`] method atomically checks and inserts/
//! updates the entry using [`DashMap`]'s `entry()` API, which guarantees
//! that at most one closure executes per key under concurrent access.
//!
//! # Expiry and sweep
//!
//! Expired entries are not removed automatically on access. Instead, a
//! background task should periodically call [`DedupeStore::sweep`] to
//! evict entries whose expiry timestamp has passed, bounding memory
//! usage. The sweep operation collects expired keys and removes them
//! in a separate pass to avoid holding map shards for too long.

use std::time::Duration;

use dashmap::DashMap;

use crate::now_ms;

/// In-memory deduplication store.
///
/// Each entry is keyed by `(topic, dedupe_key)` and stores the epoch
/// millisecond at which it expires. [`check_and_record`](DedupeStore::check_and_record)
/// is the main entry point: it atomically checks and records, returning
/// `true` if the message should be processed (fresh) or `false` if it
/// is a duplicate within the time window.
///
/// The store is thread-safe and can be shared across async tasks via
/// `Arc<DedupeStore>`. Internally it uses a [`DashMap`] for concurrent
/// shard-level locking.
///
/// # Examples
///
/// ```ignore
/// use std::time::Duration;
/// use rifts::broker::DedupeStore;
///
/// let store = DedupeStore::new();
/// let window = Duration::from_secs(60);
///
/// // First occurrence — fresh.
/// assert!(store.check_and_record("orders", "msg-001", window));
///
/// // Second occurrence within window — duplicate.
/// assert!(!store.check_and_record("orders", "msg-001", window));
///
/// // Same key in a different topic — fresh.
/// assert!(store.check_and_record("payments", "msg-001", window));
/// ```
#[derive(Debug, Default)]
pub struct DedupeStore {
    /// Concurrent hash map from `(topic, dedupe_key)` to the epoch
    /// millisecond at which the entry expires.
    inner: DashMap<(String, String), i64>,
}

impl DedupeStore {
    /// Create an empty dedupe store.
    ///
    /// The store starts with no tracked keys. Entries are added
    /// lazily via [`check_and_record`](DedupeStore::check_and_record).
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically check whether a deduplication key is fresh and record
    /// it if so.
    ///
    /// Returns `true` if the key is fresh (i.e. the message should be
    /// processed and fanned out to subscribers); `false` if it has been
    /// seen within the deduplication window and the message is a
    /// duplicate.
    ///
    /// If the key exists but its previous entry has already expired
    /// (its stored expiry is less than or equal to the current time),
    /// the entry is renewed with a new expiry and the message is
    /// treated as fresh.
    ///
    /// # Arguments
    ///
    /// * `topic` — The topic name, used as part of the composite key
    ///   so that deduplication is scoped per-topic.
    /// * `key` — The deduplication key, typically the message ID.
    /// * `window` — The deduplication time window. A new entry (or a
    ///   renewal of an expired entry) will expire after this duration.
    ///
    /// # Thread safety
    ///
    /// Uses [`DashMap`]'s `entry()` API, which acquires the shard lock
    /// for the key and executes the modify/insert closure atomically.
    /// Under concurrent access with the same key, exactly one thread
    /// will observe the entry as fresh.
    pub fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let now = now_ms();
        let expires = now + window.as_millis() as i64;
        let k = (topic.to_string(), key.to_string());
        // DashMap 6.x entry() is atomic: and_modify and or_insert_with closures
        // are mutually exclusive — only one closure executes per key. This avoids
        // the race window between the previous get_mut -> insert two-step approach.
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

    /// Remove all expired entries from the store.
    ///
    /// Iterates over every entry and collects those whose expiry
    /// timestamp is less than or equal to the current time, then
    /// removes them in a second pass. Returns the number of entries
    /// that were removed.
    ///
    /// This method should be called periodically from a background
    /// task (e.g. every few seconds) to bound memory usage. Without
    /// periodic sweeps, the store will grow indefinitely as new keys
    /// are added but old ones are never reclaimed.
    ///
    /// # Performance
    ///
    /// The sweep is O(n) in the number of tracked keys. For very large
    /// stores, consider partitioning by topic or using a more
    /// sophisticated eviction strategy.
    pub fn sweep(&self) -> usize {
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

    /// Return the number of currently tracked (non-expired and expired)
    /// keys in the store.
    ///
    /// This includes entries that have logically expired but have not
    /// yet been reclaimed by [`sweep`](DedupeStore::sweep). For an
    /// accurate count of active entries, call `sweep()` first.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the store contains no tracked keys.
    ///
    /// Like [`len`](DedupeStore::len), this includes expired entries
    /// that have not yet been swept.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_then_duplicate() {
        let d = DedupeStore::new();
        assert!(d.check_and_record("t", "k", Duration::from_secs(60)));
        assert!(!d.check_and_record("t", "k", Duration::from_secs(60)));
    }

    #[test]
    fn per_topic() {
        let d = DedupeStore::new();
        assert!(d.check_and_record("t1", "k", Duration::from_secs(60)));
        assert!(d.check_and_record("t2", "k", Duration::from_secs(60)));
    }

    #[test]
    fn sweep_drops_expired() {
        let d = DedupeStore::new();
        // 0-ms window — entries are immediately expired.
        d.check_and_record("t", "k", Duration::from_millis(0));
        // Manually expire it.
        d.inner.insert(("t".into(), "k".into()), 0);
        assert_eq!(d.sweep(), 1);
    }

    #[test]
    fn concurrent_dedup_returns_one_fresh() {
        use std::sync::Arc;
        use std::thread;
        let store = Arc::new(DedupeStore::new());
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
