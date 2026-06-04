//! Dedupe store — drops messages that have already been seen within
//! the dedupe window (spec §11.2).
//!
//! Keyed by `(topic, dedupe_key)`. A background sweep evicts old
//! entries; a read returns whether the key has been seen.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::Mutex;

#[derive(Debug, Default)]
pub struct DedupeStore {
    inner: DashMap<(String, String), i64>,
    sweep_cursor: Mutex<Vec<(String, String, i64)>>,
}

impl DedupeStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the key is fresh (i.e. the message should be
    /// processed); `false` if it has been seen within the window.
    pub fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let now = now_ms();
        let expires = now + window.as_millis() as i64;
        let k = (topic.to_string(), key.to_string());
        if let Some(mut entry) = self.inner.get_mut(&k) {
            if *entry.value() <= now {
                *entry.value_mut() = expires;
                self.sweep_cursor
                    .lock()
                    .push((k.0.clone(), k.1.clone(), expires));
                true
            } else {
                false
            }
        } else {
            self.inner.insert(k.clone(), expires);
            self.sweep_cursor.lock().push((k.0, k.1, expires));
            true
        }
    }

    /// Drop expired entries. Returns number of removed entries.
    pub fn sweep(&self) -> usize {
        let now = now_ms();
        let mut removed = 0;
        // Collect keys to remove (can't hold a DashMap iterator while mutating).
        let expired: Vec<(String, String)> = self
            .inner
            .iter()
            .filter(|kv| *kv.value() <= now)
            .map(|kv| (kv.key().0.clone(), kv.key().1.clone()))
            .collect();
        for k in expired {
            if self.inner.remove(&k).is_some() {
                removed += 1;
            }
        }
        self.sweep_cursor.lock().retain(|(_, _, exp)| *exp > now);
        removed
    }

    /// Number of tracked keys.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
