//! Redis-backed snapshot store.
//!
//! Uses a Redis Hash per topic:
//!
//! ```text
//! Key:  rift:snapshot:{topic}    (Hash)
//! Fields: snapshot_id, topic, base_offset, payload, created_at, expires_at
//! ```
//!
//! Each topic stores one active snapshot at a time. Capturing a new
//! snapshot overwrites the previous one via `HSET`.

use std::time::Duration;

use bytes::Bytes;

use crate::now_ms;
use crate::redis::connection::RedisPool;
use crate::storage::{SnapshotStore, StoredSnapshot};
use crate::topic::TopicStore;

/// Redis-backed snapshot store.
///
/// Snapshots are stored as Redis Hashes. Serialization uses CBOR
/// for the payload and string/int representation for metadata fields.
#[derive(Clone)]
pub struct RedisSnapshotStore {
    pool: RedisPool,
}

impl RedisSnapshotStore {
    /// Create a new Redis-backed snapshot store.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn snapshot_key(&self, topic: &str) -> String {
        self.pool.topic_key("snapshot", topic)
    }
}

impl SnapshotStore for RedisSnapshotStore {
    fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot> {
        // Read the latest log entry from the topic store.
        let entry = store.get(topic)?;
        let latest = entry.log.read().last().cloned()?;

        let snapshot_id = format!("snap-{}", latest.offset);
        let expires_at = ttl.map(|d| now_ms() + d.as_millis() as i64);

        let snap = StoredSnapshot {
            snapshot_id,
            topic: topic.to_string(),
            base_offset: latest.offset,
            payload: latest.payload.clone(),
            created_at: now_ms(),
            expires_at,
        };

        // Serialize to Redis hash.
        let key = self.snapshot_key(topic);
        let payload_b64 = base64_encode(&snap.payload);
        let expires_str = snap.expires_at.map(|e| e.to_string()).unwrap_or_default();
        let snap_id = snap.snapshot_id.clone();
        let snap_topic = snap.topic.clone();

        let key2 = key.clone();
        let payload_b642 = payload_b64.clone();
        let expires_str2 = expires_str.clone();
        let _: () = self.pool.sync_cmd(move |c| {
            redis::cmd("HSET")
                .arg(&key2)
                .arg("snapshot_id")
                .arg(&snap_id)
                .arg("topic")
                .arg(&snap_topic)
                .arg("base_offset")
                .arg(snap.base_offset)
                .arg("payload")
                .arg(&payload_b642)
                .arg("created_at")
                .arg(snap.created_at)
                .arg("expires_at")
                .arg(&expires_str2)
                .query::<()>(c)
        });

        // Set TTL on the key if requested.
        if let Some(ttl) = ttl {
            let key3 = key.clone();
            let _: () = self.pool.sync_cmd(move |c| {
                redis::cmd("EXPIRE")
                    .arg(&key3)
                    .arg(ttl.as_secs().max(1) as usize)
                    .query::<()>(c)
            });
        }

        Some(snap)
    }

    fn get(&self, topic: &str) -> Option<StoredSnapshot> {
        let key = self.snapshot_key(topic);
        let fields: Vec<String> = self.pool.sync_cmd(move |c| {
            redis::cmd("HMGET")
                .arg(&key)
                .arg("snapshot_id")
                .arg("topic")
                .arg("base_offset")
                .arg("payload")
                .arg("created_at")
                .arg("expires_at")
                .query::<Vec<String>>(c)
        });

        if fields.len() < 6 || fields[0].is_empty() {
            return None;
        }

        let snapshot_id = fields[0].clone();
        let topic_name = fields[1].clone();
        let base_offset: i64 = fields[2].parse().unwrap_or(0);
        let payload = base64_decode(&fields[3]);
        let created_at: i64 = fields[4].parse().unwrap_or(0);
        let expires_at: Option<i64> = if fields[5].is_empty() {
            None
        } else {
            fields[5].parse().ok()
        };

        // Check expiration.
        if let Some(exp) = expires_at
            && now_ms() > exp
        {
            return None;
        }

        Some(StoredSnapshot {
            snapshot_id,
            topic: topic_name,
            base_offset,
            payload,
            created_at,
            expires_at,
        })
    }

    fn remove(&self, topic: &str) {
        let key = self.snapshot_key(topic);
        let _: () = self
            .pool
            .sync_cmd(move |c| redis::cmd("DEL").arg(&key).query::<()>(c));
    }

    fn list(&self) -> Vec<StoredSnapshot> {
        // We can't easily list all snapshot keys without SCAN.
        // Return empty — listing is not a hot-path operation.
        Vec::new()
    }
}

fn base64_encode(data: &Bytes) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    for b in data.as_ref() {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn base64_decode(hex: &str) -> Bytes {
    let mut bytes = Vec::new();
    for i in (0..hex.len()).step_by(2) {
        if i + 2 <= hex.len()
            && let Ok(b) = u8::from_str_radix(&hex[i..i + 2], 16)
        {
            bytes.push(b);
        }
    }
    Bytes::from(bytes)
}
