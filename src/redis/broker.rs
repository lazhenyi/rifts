//! Redis-backed broker — multi-instance message routing via Redis.
//!
//! [`RedisActorBroker`] implements the [`Broker`] trait using Redis as
//! the shared state and communication bus. Each instance runs its own
//! local [`FanoutEngine`](crate::broker::fanout::FanoutEngine) for
//! local subscriber delivery and synchronizes with other instances
//! via Redis Pub/Sub.
//!
//! ## Publish Flow
//!
//! 1. Validate the frame (topic, message_id required)
//! 2. Check deduplication in Redis
//! 3. Allocate offset from Redis
//! 4. Append to Redis log
//! 5. Deliver to local subscribers
//! 6. Publish to Redis Pub/Sub for cross-instance fanout
//!
//! ## Subscribe Flow
//!
//! 1. Register the sink in the local fanout engine
//! 2. Ensure the Redis Pub/Sub channel is subscribed (first subscriber)
//! 3. Return the subscription ID

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use redis::AsyncCommands;

use crate::broker::broker::{Broker, PublishOutcome};
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};

use crate::error::{MessageReject, Result, RiftError, SystemReject};
use crate::frame::Frame;
use crate::redis::connection::RedisPool;
use crate::redis::fanout::FanoutBridge;
use crate::storage::{DedupeStore, LogStore, OffsetStore, SnapshotStore};

/// A `Broker` implementation that uses Redis for shared state and
/// cross-instance message fanout.
///
/// # Architecture
///
/// - **Storage**: Delegates to Redis-backed [`OffsetStore`], [`LogStore`],
///   [`DedupeStore`], and [`SnapshotStore`] implementations.
/// - **Fanout**: Uses a local [`FanoutBridge`] that wraps a
///   [`FanoutEngine`](crate::broker::fanout::FanoutEngine) for local
///   delivery and subscribes to Redis Pub/Sub for cross-instance delivery.
/// - **Publish**: Writes to Redis storage, fans out locally, and
///   `PUBLISH`es to the Redis channel for the topic.
pub struct RedisActorBroker<O, L, D, S> {
    /// Redis connection pool for commands and Pub/Sub.
    pool: RedisPool,
    /// Redis-backed offset allocator.
    offsets: Arc<O>,
    /// Redis-backed message log.
    log: Arc<L>,
    /// Redis-backed deduplication store.
    dedupe: Arc<D>,
    /// Redis-backed snapshot store.
    snapshots: Arc<S>,
    /// Local fanout bridge managing cross-instance Pub/Sub delivery.
    bridge: Arc<FanoutBridge>,
    /// Maximum allowed payload size in bytes.
    max_payload_bytes: usize,
}

impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> RedisActorBroker<O, L, D, S>
{
    /// Create a new Redis-backed broker.
    pub fn new(
        pool: RedisPool,
        offsets: Arc<O>,
        log: Arc<L>,
        dedupe: Arc<D>,
        snapshots: Arc<S>,
        bridge: Arc<FanoutBridge>,
        max_payload_bytes: usize,
    ) -> Self {
        Self {
            pool,
            offsets,
            log,
            dedupe,
            snapshots,
            bridge,
            max_payload_bytes,
        }
    }

    /// Validate a frame before publishing.
    fn validate_publish<'a>(&self, frame: &'a Frame) -> Result<(&'a str, &'a str)> {
        let topic = frame.topic.as_deref().ok_or_else(|| {
            RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing("topic"))
        })?;
        let message_id = frame.message_id.as_deref().ok_or_else(|| {
            RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing(
                "message_id",
            ))
        })?;
        let max = self.max_payload_bytes;
        if let Some(payload) = frame.payload.as_ref()
            && payload.len() > max
        {
            return Err(RiftError::Message(MessageReject::TooLarge {
                actual: payload.len(),
                max,
            }));
        }
        Ok((topic, message_id))
    }
}

#[async_trait]
impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> Broker for RedisActorBroker<O, L, D, S>
{
    async fn publish(&self, frame: &Frame) -> Result<PublishOutcome> {
        let (topic, message_id) = self.validate_publish(frame)?;
        crate::topic::store::validate_name(topic)?;

        // Dedupe via Redis set.
        let dedupe_window = Duration::from_secs(60);
        let mut duplicate = false;
        if !self
            .dedupe
            .check_and_record(topic, message_id, dedupe_window)
        {
            duplicate = true;
        }

        // Allocate offset via Redis hash.
        let offset = self.offsets.alloc(topic);

        // Append to Redis log.
        let entry = crate::topic::store::LogEntry {
            offset,
            publisher_session: frame.session_id.clone(),
            message_id: message_id.to_string(),
            class: "event".to_string(),
            event: frame.event.clone(),
            payload: frame.payload.clone().unwrap_or_default(),
            timestamp: frame.timestamp,
            appended_at: None,
        };
        self.log.append(
            topic,
            entry,
            crate::topic::retention::RetentionPolicy::Durable,
        );

        // Fan out locally.
        if !duplicate {
            let serialized = crate::broker::broker::serialize_frame_for_fanout(frame, offset);
            self.bridge.fanout().deliver(topic, serialized);

            // Publish to Redis Pub/Sub for cross-instance delivery.
            let channel = self.pool.topic_key("fanout", topic);
            let payload_data = frame.payload.clone().unwrap_or_default();
            let mut conn = self.pool.conn().clone();
            let _: Result<()> = conn
                .publish(&channel, payload_data.as_ref())
                .await
                .map_err(|e| {
                    RiftError::System(SystemReject::Internal(format!("redis publish: {e}")))
                });
        }

        Ok(PublishOutcome { offset, duplicate })
    }

    async fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId> {
        crate::topic::store::validate_name(topic)?;
        Ok(self.bridge.subscribe(topic, intent, sink))
    }

    async fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        Ok(self.bridge.unsubscribe(id).is_some())
    }

    async fn drop_sink(&self, sink_id: u64) -> usize {
        self.bridge.drop_sink(sink_id).len()
    }

    async fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>> {
        Ok(self
            .log
            .range(topic, from, to)
            .into_iter()
            .map(|e| e.payload)
            .collect())
    }

    async fn snapshot(&self, topic: &str) -> Result<Option<crate::storage::StoredSnapshot>> {
        Ok(self.snapshots.get(topic))
    }

    async fn subscriber_count(&self, topic: &str) -> usize {
        self.bridge.topic_subscriber_count(topic)
    }

    async fn head_offset(&self, topic: &str) -> i64 {
        self.offsets.head(topic)
    }
}
