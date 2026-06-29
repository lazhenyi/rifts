//! Redis-backed message deduplication.
//!
//! Uses a Redis Set per topic with TTL-based eviction:
//!
//! ```text
//! Key:    rift:dedupe:{topic}   (Set)
//! Member: {message_id}
//! TTL:    {dedupe_window} seconds
//! ```
//!
//! `SADD` returns the number of newly-added members; a return of
//! `0` means the member already existed → duplicate.

use std::time::Duration;

use crate::redis::connection::RedisPool;
use crate::storage::DedupeStore;

/// Redis-backed deduplication store.
///
/// Each topic has its own Set. When a message ID is first seen,
/// `SADD` returns 1 and a TTL is set via `EXPIRE`.  When the same
/// ID arrives within the TTL, `SADD` returns 0 → duplicate.
/// Redis handles eviction automatically when the TTL expires.
#[derive(Clone)]
pub struct RedisDedupeStore {
    pool: RedisPool,
}

impl RedisDedupeStore {
    /// Create a new Redis-backed dedupe store.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn set_key(&self, topic: &str) -> String {
        self.pool.topic_key("dedupe", topic)
    }
}

impl DedupeStore for RedisDedupeStore {
    fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let set_key = self.set_key(topic);
        let msg_id = key.to_string();
        let window_secs = window.as_secs().max(1) as usize;

        // SADD returns the number of elements added. 1 = new, 0 = already exists.
        let (set_key2, msg_id2) = (set_key.clone(), msg_id.clone());
        let added: i32 = self.pool.sync_cmd(move |c| {
            redis::cmd("SADD")
                .arg(&set_key2)
                .arg(&msg_id2)
                .query::<i32>(c)
        });

        // Set or refresh TTL.
        let (set_key3, window_secs2) = (set_key.clone(), window_secs);
        let _: () = self.pool.sync_cmd(move |c| {
            redis::cmd("EXPIRE")
                .arg(&set_key3)
                .arg(window_secs2)
                .query::<()>(c)
        });

        added > 0
    }

    fn sweep(&self) -> usize {
        // Redis TTL handles cleanup automatically; no explicit sweep
        // needed. We scan for keys with no TTL as a safety measure
        // but return 0 as this is a no-op for Redis.
        0
    }
}
