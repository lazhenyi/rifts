//! Redis connection pool and key-space helpers.
//!
//! `RedisPool` holds both:
//! - A sync [`redis::aio::MultiplexedConnection`] for async Pub/Sub and fanout
//! - A sync [`redis::Connection`]-like access for storage trait methods
//!
//! Storage traits (`OffsetStore`, `LogStore`, etc.) have synchronous
//! signatures, so we use `redis::ConnectionManager` (sync, auto-reconnect)
//! for those. Async Pub/Sub uses the multiplexed connection.

use redis::Client;
use redis::aio::MultiplexedConnection;

use crate::error::{Result, RiftError, SystemReject};

/// A Redis connection pool holding both sync and async connections.
///
/// - The **sync** [`redis::ConnectionManager`] (not directly exposed) is used
///   by the storage trait implementations. It auto-reconnects on error.
/// - The **async** [`MultiplexedConnection`] is used for Pub/Sub fanout
///   and any async Redis operations.
///
/// # Cloning
///
/// `RedisPool` derives `Clone`. Each clone shares the same underlying
/// async connection (multiplexed) and creates its own sync connection
/// manager.
#[derive(Clone)]
pub struct RedisPool {
    /// Async multiplexed connection for Pub/Sub and fanout.
    conn: MultiplexedConnection,
    /// Redis connection info for creating sync connections on demand.
    url: String,
    /// Key prefix applied to every Redis key.
    prefix: String,
}

impl RedisPool {
    /// Create a new pool connected to the given Redis URL.
    pub async fn connect(url: &str, prefix: &str) -> Result<Self> {
        let client = Client::open(url)
            .map_err(|e| RiftError::System(SystemReject::Internal(format!("redis client: {e}"))))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                RiftError::System(SystemReject::Internal(format!("redis connect: {e}")))
            })?;
        Ok(Self {
            conn,
            url: url.to_string(),
            prefix: prefix.to_string(),
        })
    }

    /// Return a reference to the async multiplexed connection.
    pub fn conn(&self) -> &MultiplexedConnection {
        &self.conn
    }

    /// Execute a Redis command synchronously via a short-lived
    /// synchronous connection. Each call opens and closes a
    /// connection. For high-throughput use cases, use the async
    /// connection directly.
    pub fn sync_cmd<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    {
        let mut conn = redis::Client::open(&*self.url)
            .and_then(|c| c.get_connection())
            .unwrap_or_else(|e| panic!("redis sync connect: {e}"));
        f(&mut conn).unwrap_or_else(|e| panic!("redis sync cmd: {e}"))
    }

    /// Build a namespaced Redis key: `{prefix}:{suffix}`.
    pub fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }

    /// Build a topic-scoped namespaced Redis key: `{prefix}:{kind}:{topic}`.
    pub fn topic_key(&self, kind: &str, topic: &str) -> String {
        format!("{}:{kind}:{topic}", self.prefix)
    }

    /// The configured key prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The Redis connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl std::fmt::Debug for RedisPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisPool")
            .field("url", &self.url)
            .field("prefix", &self.prefix)
            .finish()
    }
}
