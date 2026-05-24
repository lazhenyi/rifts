//! In-memory topic store — spec §9.
//!
//! Topics are stored in a `DashMap` for concurrent access from many
//! connections. Each entry keeps the `TopicProfile`, the current
//! monotonic offset, a bounded replay log (when retention allows),
//! the latest snapshot (when supported), and a subscriber set.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::error::{Result, RiftError, TopicReject};
use crate::topic::profile::TopicProfile;
use crate::topic::retention::RetentionPolicy;

/// Validate a topic name per spec §9.1.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(RiftError::Topic(TopicReject::InvalidName(
            "empty topic".into(),
        )));
    }
    if name.len() > 256 {
        return Err(RiftError::Topic(TopicReject::InvalidName(format!(
            "name too long: {} > 256",
            name.len()
        ))));
    }
    if name.starts_with('$') {
        return Err(RiftError::Topic(TopicReject::InvalidName(format!(
            "name starts with reserved '$' prefix: {}",
            name
        ))));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(RiftError::Topic(TopicReject::InvalidName(
            "name contains control characters".into(),
        )));
    }
    if !name.is_ascii() && std::str::from_utf8(name.as_bytes()).is_err() {
        return Err(RiftError::Topic(TopicReject::InvalidName(
            "name is not valid UTF-8".into(),
        )));
    }
    Ok(())
}

/// A subscriber registration. The `Id` lets us cheaply remove the
/// subscriber without scanning a list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriberId(pub u64);

/// A replay log entry. Stores the offset and a serialized payload.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub offset: i64,
    pub publisher_session: Option<String>,
    pub message_id: String,
    pub class: String,
    pub event: Option<String>,
    pub payload: Bytes,
    pub timestamp: i64,
}

use bytes::Bytes;

/// Internal state of a single topic.
#[derive(Debug)]
pub struct TopicEntry {
    pub name: String,
    pub profile: RwLock<TopicProfile>,
    pub closed: parking_lot::Mutex<bool>,
    /// Monotonic offset cursor — next offset to assign.
    pub next_offset: AtomicI64,
    /// Replay log (sorted by offset). Bounded by `profile.retention`.
    pub log: RwLock<Vec<LogEntry>>,
    /// Current subscriber count.
    pub subscriber_count: AtomicU64,
    /// Publisher count.
    pub publisher_count: AtomicU64,
    /// Latest snapshot payload (if any).
    pub latest_snapshot: RwLock<Option<LogEntry>>,
}

impl TopicEntry {
    fn new(name: String, profile: TopicProfile) -> Self {
        Self {
            name,
            profile: RwLock::new(profile),
            closed: parking_lot::Mutex::new(false),
            next_offset: AtomicI64::new(1),
            log: RwLock::new(Vec::new()),
            subscriber_count: AtomicU64::new(0),
            publisher_count: AtomicU64::new(0),
            latest_snapshot: RwLock::new(None),
        }
    }

    /// Allocate the next offset for this topic.
    pub fn alloc_offset(&self) -> i64 {
        self.next_offset.fetch_add(1, Ordering::SeqCst)
    }

    /// Current highest offset (0 if no messages yet).
    pub fn head_offset(&self) -> i64 {
        self.next_offset.load(Ordering::SeqCst) - 1
    }

    /// Append a new log entry, enforcing retention policy.
    pub fn append(&self, entry: LogEntry) {
        let profile = self.profile.read().clone();
        let mut log = self.log.write();
        log.push(entry.clone());
        match profile.retention {
            RetentionPolicy::None => log.clear(),
            RetentionPolicy::Count(n) => {
                if log.len() > n {
                    let drop = log.len() - n;
                    log.drain(0..drop);
                }
            }
            RetentionPolicy::Size(max_bytes) => {
                let mut total: usize = log.iter().map(|e| e.payload.len()).sum();
                let mut idx = 0;
                while total > max_bytes && idx < log.len() {
                    total -= log[idx].payload.len();
                    idx += 1;
                }
                if idx > 0 {
                    log.drain(0..idx);
                }
            }
            RetentionPolicy::Ttl(ttl) => {
                let now = chrono_now_ms();
                log.retain(|e| now - e.timestamp <= ttl.as_millis() as i64);
            }
            RetentionPolicy::Latest => {
                log.retain(|e| e.offset == entry.offset);
            }
            RetentionPolicy::Durable => {
                // external store — keep all in-memory entries until evicted
            }
        }
        if profile.snapshot_enabled {
            *self.latest_snapshot.write() = Some(entry);
        }
    }

    /// Returns log entries whose offset is `>= from` and `<= to`.
    pub fn range(&self, from: i64, to: i64) -> Vec<LogEntry> {
        self.log
            .read()
            .iter()
            .filter(|e| e.offset >= from && e.offset <= to)
            .cloned()
            .collect()
    }

    /// Latest snapshot, if any.
    pub fn snapshot(&self) -> Option<LogEntry> {
        self.latest_snapshot.read().clone()
    }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Topic store — a process-wide map of topic name → entry.
///
/// Internally the `DashMap` is wrapped in an `Arc` so that cloning a
/// `TopicStore` (which the broker and the router both do) shares the
/// same underlying map.
#[derive(Clone, Debug, Default)]
pub struct TopicStore {
    inner: Arc<DashMap<String, Arc<TopicEntry>>>,
}

impl TopicStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up or auto-create a topic with the given default profile.
    pub fn get_or_create(
        &self,
        name: &str,
        default_profile: TopicProfile,
    ) -> Result<Arc<TopicEntry>> {
        validate_name(name)?;
        if let Some(e) = self.inner.get(name) {
            return Ok(e.clone());
        }
        let entry = Arc::new(TopicEntry::new(name.to_string(), default_profile));
        // Another thread may have raced — use entry API to avoid clobbering.
        if let Some(existing) = self.inner.get(name) {
            return Ok(existing.clone());
        }
        self.inner.insert(name.to_string(), entry.clone());
        Ok(entry)
    }

    /// Look up a topic by name; returns `None` if it does not exist.
    pub fn get(&self, name: &str) -> Option<Arc<TopicEntry>> {
        self.inner.get(name).map(|e| e.clone())
    }

    /// Returns true if a topic with this name exists.
    pub fn exists(&self, name: &str) -> bool {
        self.inner.contains_key(name)
    }

    /// Drop a topic.
    pub fn remove(&self, name: &str) -> Option<Arc<TopicEntry>> {
        self.inner.remove(name).map(|(_, e)| e)
    }

    /// Snapshot of all topic names.
    pub fn names(&self) -> Vec<String> {
        self.inner.iter().map(|kv| kv.key().clone()).collect()
    }

    /// Per-topic stats: name → (subscribers, publishers, head_offset).
    pub fn stats(&self) -> BTreeMap<String, (u64, u64, i64)> {
        self.inner
            .iter()
            .map(|kv| {
                let e = kv.value();
                (
                    kv.key().clone(),
                    (
                        e.subscriber_count.load(Ordering::SeqCst),
                        e.publisher_count.load(Ordering::SeqCst),
                        e.head_offset(),
                    ),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(offset: i64, payload: &[u8]) -> LogEntry {
        LogEntry {
            offset,
            publisher_session: None,
            message_id: format!("m-{offset}"),
            class: "event".into(),
            event: Some("e".into()),
            payload: Bytes::copy_from_slice(payload),
            timestamp: 0,
        }
    }

    #[test]
    fn name_validation() {
        assert!(super::validate_name("room/1").is_ok());
        assert!(super::validate_name("user/abc").is_ok());
        assert!(super::validate_name("").is_err());
        assert!(super::validate_name("$system").is_err());
        assert!(super::validate_name(&"x".repeat(257)).is_err());
    }

    #[test]
    fn get_or_create_idempotent() {
        let store = TopicStore::new();
        let a = store
            .get_or_create("room/1", TopicProfile::default())
            .unwrap();
        let b = store
            .get_or_create("room/1", TopicProfile::default())
            .unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn append_count_retention() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t1",
                TopicProfile {
                    retention: RetentionPolicy::Count(2),
                    ..TopicProfile::default()
                },
            )
            .unwrap();
        for i in 1..=5 {
            entry.append(sample_entry(i, b"x"));
        }
        let log = entry.log.read();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].offset, 4);
        assert_eq!(log[1].offset, 5);
    }

    #[test]
    fn append_latest_retention() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t1",
                TopicProfile {
                    retention: RetentionPolicy::Latest,
                    ..TopicProfile::default()
                },
            )
            .unwrap();
        entry.append(sample_entry(1, b"a"));
        entry.append(sample_entry(2, b"b"));
        let log = entry.log.read();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].offset, 2);
    }

    #[test]
    fn range_query() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t1",
                TopicProfile {
                    retention: RetentionPolicy::Count(100),
                    ..TopicProfile::default()
                },
            )
            .unwrap();
        for i in 1..=5 {
            entry.append(sample_entry(i, b"x"));
        }
        let got = entry.range(2, 4);
        assert_eq!(
            got.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    #[test]
    fn offset_monotonic() {
        let store = TopicStore::new();
        let entry = store.get_or_create("t1", TopicProfile::default()).unwrap();
        let a = entry.alloc_offset();
        let b = entry.alloc_offset();
        let c = entry.alloc_offset();
        assert_eq!((a, b, c), (1, 2, 3));
        assert_eq!(entry.head_offset(), 3);
    }

    #[test]
    fn snapshot_keeps_latest() {
        let store = TopicStore::new();
        let entry = store
            .get_or_create(
                "t1",
                TopicProfile {
                    retention: RetentionPolicy::Count(10),
                    snapshot_enabled: true,
                    ..TopicProfile::default()
                },
            )
            .unwrap();
        entry.append(sample_entry(1, b"a"));
        entry.append(sample_entry(2, b"b"));
        let s = entry.snapshot().unwrap();
        assert_eq!(s.offset, 2);
    }
}
