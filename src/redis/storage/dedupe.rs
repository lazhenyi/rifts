//! Redis-backed message deduplication.
//!
//! Uses a Redis Set per topic with per-member TTL via separate keys:
//!
//! ```text
//! Key:    rift:dedupe:{topic}:{message_id}   (String)
//! Value:  "1"
//! TTL:    {dedupe_window} seconds
//! ```
//!
//! Per-member keys (instead of a Set with `EXPIRE` on the whole set)
//! ensure each message ID expires independently. `SET NX` returns
//! `true` when the key did not exist → fresh message.

use std::time::Duration;

use redis::AsyncCommands;

use crate::redis::connection::RedisPool;
use crate::storage::DedupeStore;

/// Redis-backed deduplication store.
///
/// Each message ID is stored as a separate key with its own TTL so
/// entries expire independently regardless of topic activity level.
#[derive(Clone)]
pub struct RedisDedupeStore {
    pool: RedisPool,
}

impl RedisDedupeStore {
    /// Create a new Redis-backed dedupe store.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn member_key(&self, topic: &str, message_id: &str) -> String {
        format!("{}:dedupe:{topic}:{message_id}", self.pool.prefix())
    }

    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: std::future::Future,
    {
        tokio::runtime::Handle::current().block_on(f)
    }
}

impl DedupeStore for RedisDedupeStore {
    fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let member_key = self.member_key(topic, key);
        let window_secs = window.as_secs().max(1) as usize;

        self.block_on(async {
            let mut conn = self.pool.conn().clone();
            // SET key "1" NX EX window_secs
            // Returns OK if set (fresh), nil if already exists (duplicate).
            let result: Option<String> = redis::cmd("SET")
                .arg(&member_key)
                .arg("1")
                .arg("NX")
                .arg("EX")
                .arg(window_secs)
                .query_async(&mut conn)
                .await
                .unwrap_or(None);
            result.is_some()
        })
    }

    fn sweep(&self) -> usize {
        // Redis TTL handles cleanup automatically; no explicit sweep needed.
        0
    }
}
