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

    /// Serialize a LogEntry to CBOR bytes for Redis storage.
    fn encode_entry(entry: &LogEntry) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(entry, &mut buf).expect("LogEntry CBOR serialization failed");
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

        // ZADD — score is the offset.
        let (key2, member2) = (key.clone(), member.clone());
        let _: () = self.pool.sync_cmd(move |c| {
            redis::cmd("ZADD")
                .arg(&key2)
                .arg(offset)
                .arg(&member2)
                .query::<()>(c)
        });

        // Enforce retention.
        match retention {
            RetentionPolicy::None => {
                let key3 = key.clone();
                let _: () = self
                    .pool
                    .sync_cmd(move |c| redis::cmd("DEL").arg(&key3).query::<()>(c));
            }
            RetentionPolicy::Count(n) if n > 0 => {
                let key3 = key.clone();
                // Keep the n entries with highest scores.
                let _: () = self.pool.sync_cmd(move |c| {
                    redis::cmd("ZREMRANGEBYRANK")
                        .arg(&key3)
                        .arg(0)
                        .arg(-((n as i64) + 1))
                        .query::<()>(c)
                });
            }
            RetentionPolicy::Latest => {
                let key3 = key.clone();
                // Keep only the entry with the highest score.
                let _: () = self.pool.sync_cmd(move |c| {
                    redis::cmd("ZREMRANGEBYRANK")
                        .arg(&key3)
                        .arg(0)
                        .arg(-2)
                        .query::<()>(c)
                });
            }
            // Size, TTL, and Durable are not yet implemented for Redis;
            // they silently keep all entries.
            _ => {}
        }
    }

    fn range(&self, topic: &str, from: i64, to: i64) -> Vec<LogEntry> {
        let key = self.log_key(topic);
        let data: Vec<Vec<u8>> = self.pool.sync_cmd(|c| {
            redis::cmd("ZRANGEBYSCORE")
                .arg(&key)
                .arg(from)
                .arg(to)
                .query::<Vec<Vec<u8>>>(c)
        });
        data.iter().filter_map(|d| Self::decode_entry(d)).collect()
    }

    fn latest(&self, topic: &str) -> Option<LogEntry> {
        let key = self.log_key(topic);
        let data: Vec<Vec<u8>> = self.pool.sync_cmd(|c| {
            redis::cmd("ZREVRANGE")
                .arg(&key)
                .arg(0)
                .arg(0)
                .query::<Vec<Vec<u8>>>(c)
        });
        data.first().and_then(|d| Self::decode_entry(d))
    }

    fn remove(&self, topic: &str) {
        let key = self.log_key(topic);
        let _: () = self
            .pool
            .sync_cmd(|c| redis::cmd("DEL").arg(&key).query::<()>(c));
    }
}
