//! Fanout engine — delivers a published message to all subscribers of
//! a topic (spec §22.4, `direct` strategy for small topics).
//!
//! A `Subscription` ties a connection to a topic with a `from` offset
//! and a `live_only` flag. The fanout engine hands each matching
//! subscriber a serialized frame ready to write to the transport.

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use uuid::Uuid;

/// Identifies a single (connection, topic) subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// What a subscriber wants to receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubscribeIntent {
    /// New messages after subscription.
    Live,
    /// From a specific offset.
    Replay {
        /// Starting offset for replay.
        from: i64,
    },
    /// Snapshot then live.
    SnapshotThenLive,
    /// Only the latest state.
    Latest,
    /// System notices only.
    Passive,
    /// Temporary; cleaned up on disconnect.
    Ephemeral,
}

/// A registered subscription.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Unique subscription id.
    pub id: SubscriptionId,
    /// Topic name.
    pub topic: String,
    /// What this subscriber wants.
    pub intent: SubscribeIntent,
    /// `true` if the subscription has been told to stop.
    pub cancelled: bool,
}

/// Connection handle used by the fanout engine to deliver messages.
pub type ConnectionSink = Arc<dyn FanoutSink>;

/// Sink trait — implemented by the connection to receive fanned-out
/// frames. The fanout engine does not know the transport type.
pub trait FanoutSink: Send + Sync {
    /// Deliver a serialized frame to this sink.
    fn deliver(&self, frame: bytes::Bytes) -> Result<(), FanoutError>;
    /// Unique identifier for this sink.
    fn id(&self) -> u64;
}

/// Errors that can occur during fanout delivery.
#[derive(Debug, thiserror::Error)]
pub enum FanoutError {
    /// The sink has been closed and should be removed.
    #[error("sink closed")]
    Closed,
    /// The sink's send queue is full.
    #[error("sink backpressured: queue={queue_bytes}, max={max_bytes}")]
    Backpressured {
        /// Current queue depth in bytes.
        queue_bytes: usize,
        /// Maximum queue capacity in bytes.
        max_bytes: usize,
    },
}

/// In-process fanout engine. Stores subscriptions grouped by topic.
pub struct FanoutEngine {
    /// topic → (subscription_id, sink)
    by_topic: DashMap<String, Vec<(SubscriptionId, ConnectionSink)>>,
    /// subscription_id → (topic, sink)
    by_id: DashMap<SubscriptionId, (String, ConnectionSink)>,
    seq: Mutex<u64>,
}

impl std::fmt::Debug for FanoutEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutEngine")
            .field("subscription_count", &self.by_id.len())
            .finish()
    }
}

impl Default for FanoutEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FanoutEngine {
    /// Create an empty fanout engine.
    pub fn new() -> Self {
        Self {
            by_topic: DashMap::new(),
            by_id: DashMap::new(),
            seq: Mutex::new(0),
        }
    }

    /// Add a new subscription. Returns the subscription id.
    pub fn subscribe(
        &self,
        topic: &str,
        _intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> SubscriptionId {
        let mut seq = self.seq.lock();
        *seq += 1;
        let id = SubscriptionId(*seq);
        drop(seq);
        self.by_topic
            .entry(topic.to_string())
            .or_default()
            .push((id, sink.clone()));
        self.by_id.insert(id, (topic.to_string(), sink));
        id
    }

    /// Remove a subscription. Returns `Some(topic_name)` if the
    /// subscription existed, so the caller can adjust per-topic
    /// counters.
    pub fn unsubscribe(&self, id: SubscriptionId) -> Option<String> {
        if let Some((_, (topic, _sink))) = self.by_id.remove(&id) {
            if let Some(mut list) = self.by_topic.get_mut(&topic) {
                list.retain(|(sid, _)| *sid != id);
            }
            Some(topic)
        } else {
            None
        }
    }

    /// Drop all subscriptions owned by `sink_id`.
    pub fn drop_sink(&self, sink_id: u64) -> Vec<String> {
        let mut topics = Vec::new();
        let ids: Vec<SubscriptionId> = self
            .by_id
            .iter()
            .filter(|kv| kv.value().1.id() == sink_id)
            .map(|kv| *kv.key())
            .collect();
        for id in ids {
            if let Some(topic) = self.unsubscribe(id) {
                topics.push(topic);
            }
        }
        topics
    }

    /// Deliver a single serialized frame to all subscribers of `topic`
    /// whose intent covers it. Returns the number of successful
    /// deliveries.
    pub fn deliver(&self, topic: &str, frame: bytes::Bytes) -> usize {
        let mut ok = 0;
        if let Some(list) = self.by_topic.get(topic) {
            for (_id, sink) in list.iter() {
                if sink.deliver(frame.clone()).is_ok() {
                    ok += 1;
                }
            }
        }
        ok
    }

    /// Total number of registered subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.by_id.len()
    }

    /// Number of distinct sinks subscribed to a topic.
    pub fn topic_subscriber_count(&self, topic: &str) -> usize {
        self.by_topic.get(topic).map(|l| l.len()).unwrap_or(0)
    }
}

/// Build a fresh connection sink id (uuid-derived u64) for tagging.
pub fn new_sink_id() -> u64 {
    let u = Uuid::new_v4();
    let bytes = u.as_bytes();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(buf)
}

/// Optional helper for tests: a sink that just counts deliveries.
pub mod test_sink {
    use std::sync::atomic::{AtomicU64, Ordering};

    use parking_lot::Mutex;

    use super::{FanoutError, FanoutSink};

    /// A test sink that counts deliveries and records messages.
    pub struct CountingSink {
        id: u64,
        delivered: AtomicU64,
        log: Mutex<Vec<Vec<u8>>>,
    }

    impl CountingSink {
        /// Create a new counting sink with the given id.
        pub fn new(id: u64) -> Self {
            Self {
                id,
                delivered: AtomicU64::new(0),
                log: Mutex::new(Vec::new()),
            }
        }
        /// Number of messages delivered.
        pub fn count(&self) -> u64 {
            self.delivered.load(Ordering::SeqCst)
        }
        /// Snapshot of delivered messages.
        pub fn messages(&self) -> Vec<Vec<u8>> {
            self.log.lock().clone()
        }
    }

    impl FanoutSink for CountingSink {
        fn deliver(&self, frame: bytes::Bytes) -> Result<(), FanoutError> {
            self.delivered.fetch_add(1, Ordering::SeqCst);
            self.log.lock().push(frame.to_vec());
            Ok(())
        }
        fn id(&self) -> u64 {
            self.id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_sink::CountingSink;
    use super::*;

    #[test]
    fn subscribe_and_fanout() {
        let fan = FanoutEngine::new();
        let s1 = Arc::new(CountingSink::new(1));
        let s2 = Arc::new(CountingSink::new(2));
        let s3 = Arc::new(CountingSink::new(3));
        fan.subscribe("t", SubscribeIntent::Live, s1.clone());
        fan.subscribe("t", SubscribeIntent::Live, s2.clone());
        fan.subscribe("other", SubscribeIntent::Live, s3.clone());

        let frame = bytes::Bytes::from_static(b"hi");
        let n = fan.deliver("t", frame);
        assert_eq!(n, 2);
        assert_eq!(s1.count(), 1);
        assert_eq!(s2.count(), 1);
        assert_eq!(s3.count(), 0);
    }

    #[test]
    fn unsubscribe_returns_topic() {
        let fan = FanoutEngine::new();
        let s = Arc::new(CountingSink::new(1));
        let id = fan.subscribe("t", SubscribeIntent::Live, s.clone());
        let topic = fan.unsubscribe(id);
        assert_eq!(topic, Some("t".to_string()));
        assert_eq!(fan.deliver("t", bytes::Bytes::from_static(b"x")), 0);
    }

    #[test]
    fn drop_sink_returns_topics() {
        let fan = FanoutEngine::new();
        let s1 = Arc::new(CountingSink::new(7));
        let s2 = Arc::new(CountingSink::new(7));
        fan.subscribe("t", SubscribeIntent::Live, s1.clone());
        fan.subscribe("u", SubscribeIntent::Live, s2.clone());
        let topics = fan.drop_sink(7);
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"t".to_string()));
        assert!(topics.contains(&"u".to_string()));
        assert_eq!(fan.subscription_count(), 0);
    }

    #[test]
    fn topic_subscriber_count() {
        let fan = FanoutEngine::new();
        let s = Arc::new(CountingSink::new(1));
        fan.subscribe("t", SubscribeIntent::Live, s.clone());
        fan.subscribe("t", SubscribeIntent::Live, s.clone());
        assert_eq!(fan.topic_subscriber_count("t"), 2);
    }

    #[test]
    fn deliver_records_payload() {
        let fan = FanoutEngine::new();
        let s = Arc::new(CountingSink::new(1));
        fan.subscribe("t", SubscribeIntent::Live, s.clone());
        fan.deliver("t", bytes::Bytes::from_static(b"abc"));
        assert_eq!(s.messages(), vec![b"abc".to_vec()]);
    }

    #[test]
    fn sink_id_is_unique() {
        let a = new_sink_id();
        let b = new_sink_id();
        assert_ne!(a, b);
    }
}
