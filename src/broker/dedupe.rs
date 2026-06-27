//! Dedupe store — drops messages that have already been seen within
//! the dedupe window (spec §11.2).
//!
//! Keyed by `(topic, dedupe_key)`. A background sweep evicts old
//! entries; a read returns whether the key has been seen.

use std::time::Duration;

use dashmap::DashMap;

use crate::now_ms;

/// In-memory deduplication store.
///
/// Each entry is keyed by `(topic, dedupe_key)` and stores the epoch
/// millisecond at which it expires.  `check_and_record` is the main
/// entry point: it atomically checks and records, returning `true` if
/// the message should be processed (fresh) or `false` if it is a
/// duplicate.
#[derive(Debug, Default)]
pub struct DedupeStore {
    inner: DashMap<(String, String), i64>,
}

impl DedupeStore {
    /// Create an empty dedupe store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the key is fresh (i.e. the message should be
    /// processed); `false` if it has been seen within the window.
    ///
    /// If the key exists but its previous entry has already expired,
    /// the entry is renewed and the message is treated as fresh.
    pub fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let now = now_ms();
        let expires = now + window.as_millis() as i64;
        let k = (topic.to_string(), key.to_string());
        if let Some(mut entry) = self.inner.get_mut(&k) {
            if *entry.value() <= now {
                *entry.value_mut() = expires;
                true
            } else {
                false
            }
        } else {
            self.inner.insert(k, expires);
            true
        }
    }

    /// Drop expired entries. Returns number of removed entries.
    ///
    /// Should be called periodically from a background task to bound
    /// memory usage.
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

    /// Number of tracked keys.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the store is empty.
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
}
