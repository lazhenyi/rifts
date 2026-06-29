//! Redis-backed monotonic offset allocation.
//!
//! Uses a Redis Hash with `HINCRBY` for atomic, distributed offset
//! allocation. Each topic maps to a field in a shared Redis hash:
//!
//! ```text
//! Key:   rift:offsets       (Hash)
//! Field: {topic_name}       (String)
//! Value: i64 head counter
//! ```

use crate::redis::connection::RedisPool;
use crate::storage::OffsetStore;

/// Redis-backed offset store.
///
/// Each topic's head counter is stored as a field in the Redis hash
/// `{prefix}:offsets`.  `HINCRBY` provides atomic, lock-free
/// increment that is safe across multiple `rifts` instances.
#[derive(Clone)]
pub struct RedisOffsetStore {
    pool: RedisPool,
}

impl RedisOffsetStore {
    /// Create a new Redis-backed offset store.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn hash_key(&self) -> String {
        self.pool.key("offsets")
    }
}

impl OffsetStore for RedisOffsetStore {
    fn alloc(&self, topic: &str) -> i64 {
        let key = self.hash_key();
        self.pool.sync_cmd(|c| {
            redis::cmd("HINCRBY")
                .arg(&key)
                .arg(topic)
                .arg(1)
                .query::<i64>(c)
        })
    }

    fn head(&self, topic: &str) -> i64 {
        let key = self.hash_key();
        self.pool
            .sync_cmd(|c| {
                redis::cmd("HGET")
                    .arg(&key)
                    .arg(topic)
                    .query::<Option<i64>>(c)
            })
            .unwrap_or(0)
    }

    fn remove(&self, topic: &str) {
        let key = self.hash_key();
        let _: () = self
            .pool
            .sync_cmd(|c| redis::cmd("HDEL").arg(&key).arg(topic).query::<()>(c));
    }
}
