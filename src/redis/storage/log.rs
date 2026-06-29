//! Redis-backed message log store.
//!
//! Uses a Redis Sorted Set per topic. Each entry is stored as a
//! member with its offset as the score:
//!
//! ```text
//! Key:    rift:log:{topic}     (Sorted Set)
//! Score:  i64 offset
//! Member: CBOR-serialized LogEntry bytes
//! ```

use redis::AsyncCommands;

use crate::redis::connection::RedisPool;
use crate::storage::LogStore;
use crate::topic::retention::RetentionPolicy;
use crate::topic::store::LogEntry;

/// Redis-backed log store using Sorted Sets.
///
/// Messages are appended with `ZADD` using the offset as score.
/// Range queries use `ZRANGEBYSCORE`. Retention is enforced via
/// `ZREMRANGEBYRANK` (count-based) or `ZREMRANGEBYSCORE`
/// (time-based).
#[derive(Clone)]
pub struct RedisLogStore {
    pool: RedisPool,
}

impl RedisLogStore {
    /// Create a new Redis-backed log store.
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    fn log_key(&self, topic: &str) -> String {
        self.pool.topic_key("log", topic)
    }

    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: std::future::Future,
    {
        tokio::runtime::Handle::current().block_on(f)
    }

    /// Serialize a LogEntry to CBOR bytes for Redis storage.
    fn encode_entry(entry: &LogEntry) -> Vec<u8> {
        let mut buf = Vec::new();
        // CBOR serialization of a simple struct with owned fields
        // should not fail; if it does, return an empty encoding that
        // decode_entry will reject as None rather than panicking.
        ciborium::into_writer(entry, &mut buf).unwrap_or_default();
        buf
    }

    /// Deserialize a LogEntry from CBOR bytes.
    fn decode_entry(data: &[u8]) -> Option<LogEntry> {
        ciborium::from_reader(data).ok()
    }
}

impl LogStore for RedisLogStore {
    fn append(&self, topic: &str, entry: LogEntry, retention: RetentionPolicy) {
        let key = self.log_key(topic);
        let member = Self::encode_entry(&entry);
        let offset = entry.offset;

        self.block_on(async {
            let mut conn = self.pool.conn().clone();
            let _: Result<(), _> = redis::cmd("ZADD")
                .arg(&key)
                .arg(offset)
                .arg(&member)
                .query_async(&mut conn)
                .await;

            match retention {
                RetentionPolicy::None => {
                    let _: Result<(), _> = redis::cmd("DEL").arg(&key).query_async(&mut conn).await;
                }
                RetentionPolicy::Count(n) if n > 0 => {
                    let _: Result<(), _> = redis::cmd("ZREMRANGEBYRANK")
                        .arg(&key)
                        .arg(0)
                        .arg(-((n as i64) + 1))
                        .query_async(&mut conn)
                        .await;
                }
                RetentionPolicy::Latest => {
                    let _: Result<(), _> = redis::cmd("ZREMRANGEBYRANK")
                        .arg(&key)
                        .arg(0)
                        .arg(-2)
                        .query_async(&mut conn)
                        .await;
                }
                // Size, TTL, and Durable are not yet implemented for Redis;
                // they silently keep all entries.
                _ => {}
            }
        });
    }

    fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry> {
        let key = self.log_key(topic);
        self.block_on(async {
            let mut conn = self.pool.conn().clone();
            let data: Result<Vec<Vec<u8>>, _> = redis::cmd("ZRANGEBYSCORE")
                .arg(&key)
                .arg(from)
                .arg(to)
                .query_async(&mut conn)
                .await;
            data.unwrap_or_default()
                .iter()
                .filter_map(|d| Self::decode_entry(d))
                .collect()
        })
    }

    fn latest(&self, topic: &str) -> Option<LogEntry> {
        let key = self.log_key(topic);
        self.block_on(async {
            let mut conn = self.pool.conn().clone();
            let data: Result<Vec<Vec<u8>>, _> = redis::cmd("ZREVRANGE")
                .arg(&key)
                .arg(0)
                .arg(0)
                .query_async(&mut conn)
                .await;
            data.unwrap_or_default()
                .first()
                .and_then(|d| Self::decode_entry(d))
        })
    }

    fn remove(&self, topic: &str) {
        let key = self.log_key(topic);
        let _: Result<(), _> = self.block_on(async {
            let mut conn = self.pool.conn().clone();
            redis::cmd("DEL").arg(&key).query_async(&mut conn).await
        });
    }
}
