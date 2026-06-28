//! Dedupe store — drop messages already seen within a window (spec §11.2).

use std::time::Duration;

use dashmap::DashMap;

use crate::now_ms;

/// Trait for deduplication.
pub trait DedupeStore: Send + Sync {
    /// Returns `true` if the key is fresh (process the message);
    /// `false` if it has been seen within the window.
    fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool;

    /// Drop expired entries. Returns count removed.
    fn sweep(&self) -> usize;
}

// ── Memory-backed ────────────────────────────────────────────

/// In-memory dedupe store, backed by a `DashMap`.
#[derive(Debug, Default)]
pub struct MemoryDedupeStore {
    inner: DashMap<(String, String), i64>,
}

impl MemoryDedupeStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DedupeStore for MemoryDedupeStore {
    fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let now = now_ms();
        let expires = now + window.as_millis() as i64;
        let k = (topic.to_string(), key.to_string());
        // DashMap 6.x 的 entry() 是原子的：and_modify 与 or_insert_with 闭包
        // 互斥——同一 key 只有一个闭包会执行。这避免了原先 get_mut → insert
        // 两步之间的竞争窗口。
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
    use super::*;
    use crate::storage::encode;
    use crate::storage::engine::SledEngine;
    use crate::storage::engine::StorageEngine;

    /// Sled-backed dedupe store.
    pub struct SledDedupeStore {
        engine: SledEngine,
    }

    impl SledDedupeStore {
        pub fn new(engine: SledEngine) -> Self {
            Self { engine }
        }
    }

    impl DedupeStore for SledDedupeStore {
        fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
            let now = now_ms();
            let expires = now + window.as_millis() as i64;
            let k = encode::dedupe_key(topic, key);
            if let Some(existing) = self.engine.get(&k)
                && existing.len() >= 8
            {
                let prev = i64::from_be_bytes(existing[..8].try_into().unwrap_or([0; 8]));
                if prev > now {
                    return false;
                }
            }
            self.engine.put(&k, &expires.to_be_bytes());
            true
        }

        fn sweep(&self) -> usize {
            let now = now_ms();
            let expired: Vec<Vec<u8>> = self
                .engine
                .scan_prefix(&[])
                .into_iter()
                .filter(|(_, v)| {
                    if v.len() >= 8 {
                        i64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8])) <= now
                    } else {
                        false
                    }
                })
                .map(|(k, _)| k)
                .collect();
            let count = expired.len();
            for k in expired {
                self.engine.delete(&k);
            }
            count
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
