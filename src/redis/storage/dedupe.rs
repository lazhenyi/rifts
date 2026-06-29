//! Redis-backed message deduplication using per-key TTL.
//! All methods are async.

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;

use crate::redis::connection::RedisPool;
use crate::storage::DedupeStore;

#[derive(Clone)]
pub struct RedisDedupeStore {
    pool: RedisPool,
}

impl RedisDedupeStore {
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn member_key(&self, topic: &str, message_id: &str) -> String {
        format!("{}:dedupe:{topic}:{message_id}", self.pool.prefix())
    }
}

#[async_trait]
impl DedupeStore for RedisDedupeStore {
    async fn check_and_record(&self, topic: &str, key: &str, window: Duration) -> bool {
        let member_key = self.member_key(topic, key);
        let window_secs = window.as_secs().max(1) as usize;
        let mut conn = self.pool.conn().clone();
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
    }

    async fn sweep(&self) -> usize {
        0
    }
}
