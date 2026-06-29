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
use redis::AsyncCommands;

use crate::now_ms;
use crate::redis::connection::RedisPool;
use crate::storage::{SnapshotStore, StoredSnapshot};
use crate::topic::TopicStore;

/// Redis-backed snapshot store.
///
/// Snapshots are stored as Redis Hashes. Serialization uses hex
/// encoding for the payload and string/int representation for
/// metadata fields.
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

    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: std::future::Future,
    {
        tokio::runtime::Handle::current().block_on(f)
    }
}

impl SnapshotStore for RedisSnapshotStore {
    fn capture(
        &self,
        topic: &str,
        store: &TopicStore,
        ttl: Option<Duration>,
    ) -> Option<StoredSnapshot> {
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

        let key = self.snapshot_key(topic);
        let payload_hex = hex_encode(&snap.payload);
        let expires_str = snap.expires_at.map(|e| e.to_string()).unwrap_or_default();
        let snap_id = snap.snapshot_id.clone();
        let snap_topic = snap.topic.clone();
        let base_offset = snap.base_offset;
        let created_at = snap.created_at;

        self.block_on(async {
            let mut conn = self.pool.conn().clone();
            let _: Result<(), _> = redis::cmd("HSET")
                .arg(&key)
                .arg("snapshot_id")
                .arg(&snap_id)
                .arg("topic")
                .arg(&snap_topic)
                .arg("base_offset")
                .arg(base_offset)
                .arg("payload")
                .arg(&payload_hex)
                .arg("created_at")
                .arg(created_at)
                .arg("expires_at")
                .arg(&expires_str)
                .query_async(&mut conn)
                .await;

            if let Some(ttl) = ttl {
                let _: Result<(), _> = redis::cmd("EXPIRE")
                    .arg(&key)
                    .arg(ttl.as_secs().max(1) as usize)
                    .query_async(&mut conn)
                    .await;
            }
        });

        Some(snap)
    }

    fn get(&self, topic: &str) -> Option<StoredSnapshot> {
        let key = self.snapshot_key(topic);
        let fields: Vec<String> = self.block_on(async {
            let mut conn = self.pool.conn().clone();
            redis::cmd("HMGET")
                .arg(&key)
                .arg("snapshot_id")
                .arg("topic")
                .arg("base_offset")
                .arg("payload")
                .arg("created_at")
                .arg("expires_at")
                .query_async(&mut conn)
                .await
                .unwrap_or_default()
        });

        if fields.len() < 6 || fields[0].is_empty() {
            return None;
        }

        let snapshot_id = fields[0].clone();
        let topic_name = fields[1].clone();
        let base_offset: i64 = fields[2].parse().unwrap_or(0);
        let payload = hex_decode(&fields[3]);
        let created_at: i64 = fields[4].parse().unwrap_or(0);
        let expires_at: Option<i64> = if fields[5].is_empty() {
            None
        } else {
            fields[5].parse().ok()
        };

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
        let _: Result<(), _> = self.block_on(async {
            let mut conn = self.pool.conn().clone();
            redis::cmd("DEL").arg(&key).query_async(&mut conn).await
        });
    }

    fn list(&self) -> Vec<StoredSnapshot> {
        // Listing all snapshot keys requires SCAN which is not
        // implemented for this store. Return empty; listing is not
        // a hot-path operation.
        Vec::new()
    }
}

fn hex_encode(data: &Bytes) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    for b in data.as_ref() {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(hex: &str) -> Bytes {
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
