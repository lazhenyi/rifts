//! Broker trait and shared types — spec §22.
//!
//! Concrete implementations live in sibling modules:
//! - [`InMemoryBroker`](crate::broker::InMemoryBroker) — single-process, no persistence.
//! - Sled-backed (Phase 1).
//! - [`RemoteBroker`] (Phase 2a).
//! - [`ActorBroker`] (Phase 2b).

use std::sync::Arc;

use bytes::Bytes;

use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::error::Result;
use crate::frame::Frame;

/// The outcome of publishing a message.
#[derive(Debug, Clone)]
pub struct PublishOutcome {
    /// The offset assigned by the broker.
    pub offset: i64,
    /// True if the dedupe key had been seen within the window.
    pub duplicate: bool,
}

/// Broker trait.
///
/// All topic operations — publish, subscribe, replay, snapshot —
/// go through this trait.  Implementations range from the built-in
/// [`InMemoryBroker`](crate::broker::InMemoryBroker) to user-provided
/// remote or actor-based brokers.
pub trait Broker: Send + Sync {
    /// Publish a message to a topic.
    fn publish(&self, frame: &Frame) -> Result<PublishOutcome>;

    /// Subscribe a connection sink to a topic.
    fn subscribe(
        &self,
        topic: &str,
        intent: SubscribeIntent,
        sink: ConnectionSink,
    ) -> Result<SubscriptionId>;

    /// Cancel a subscription.
    fn unsubscribe(&self, id: SubscriptionId) -> Result<bool>;

    /// Remove all subscriptions belonging to a sink.
    fn drop_sink(&self, sink_id: u64) -> usize;

    /// Replay messages in `[from, to]` on `topic`. The returned
    /// frames are already serialized and ready to send.
    fn replay(&self, topic: &str, from: i64, to: i64) -> Result<Vec<Bytes>>;

    /// Fetch a snapshot for `topic`, if one is available.
    fn snapshot(&self, topic: &str) -> Result<Option<crate::storage::StoredSnapshot>>;

    /// Number of subscribers for a topic.
    fn subscriber_count(&self, topic: &str) -> usize;

    /// Current head offset for a topic.
    fn head_offset(&self, topic: &str) -> i64;
}

/// Helper: produce a serialized frame for fanout, stamping the
/// assigned offset.
pub fn serialize_frame_for_fanout(frame: &Frame, offset: i64) -> Bytes {
    let mut buf = Vec::with_capacity(16 + frame.payload.as_ref().map(|p| p.len()).unwrap_or(0));
    buf.extend_from_slice(b"OFF:");
    buf.extend_from_slice(&offset.to_be_bytes());
    if let Some(payload) = frame.payload.as_ref() {
        buf.extend_from_slice(payload);
    }
    Bytes::from(buf)
}

/// Subscription handle returned by the broker.
pub type BrokerSubscription = crate::broker::fanout::Subscription;
