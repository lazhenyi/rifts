//! Messages that a [`TopicActor`](crate::actor::TopicActor) can process.
//!
//! Each variant carries a `reply_to: oneshot::Sender<...>` for the response.
//! For cross-process use, see [`WireTopicMsg`] which replaces `reply_to` with
//! a `request_id: u32`.

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::broker::broker::PublishOutcome;
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::error::Result;
use crate::frame::Frame;
use crate::storage::StoredSnapshot;

/// Messages that a TopicActor can process.
pub enum TopicMsg {
    /// Publish a frame to the topic.
    Publish {
        frame: Frame,
        reply_to: oneshot::Sender<Result<PublishOutcome>>,
    },
    /// Subscribe a sink to the topic.
    Subscribe {
        sink: ConnectionSink,
        intent: SubscribeIntent,
        reply_to: oneshot::Sender<Result<SubscriptionId>>,
    },
    /// Unsubscribe by id.
    Unsubscribe {
        id: SubscriptionId,
        reply_to: oneshot::Sender<Result<bool>>,
    },
    /// Replay messages in a range.
    Replay {
        from: i64,
        to: i64,
        reply_to: oneshot::Sender<Result<Vec<Bytes>>>,
    },
    /// Fetch a snapshot.
    Snapshot {
        reply_to: oneshot::Sender<Result<Option<StoredSnapshot>>>,
    },
    /// Get the head offset.
    HeadOffset { reply_to: oneshot::Sender<i64> },
    /// Drop a sink.
    DropSink {
        sink_id: u64,
        reply_to: oneshot::Sender<usize>,
    },
    /// Graceful shutdown.
    Shutdown { reply_to: oneshot::Sender<()> },
}

impl std::fmt::Debug for TopicMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicMsg::Publish { .. } => f.write_str("Publish"),
            TopicMsg::Subscribe { .. } => f.write_str("Subscribe"),
            TopicMsg::Unsubscribe { .. } => f.write_str("Unsubscribe"),
            TopicMsg::Replay { .. } => f.write_str("Replay"),
            TopicMsg::Snapshot { .. } => f.write_str("Snapshot"),
            TopicMsg::HeadOffset { .. } => f.write_str("HeadOffset"),
            TopicMsg::DropSink { .. } => f.write_str("DropSink"),
            TopicMsg::Shutdown { .. } => f.write_str("Shutdown"),
        }
    }
}

/// A wire-friendly version of `TopicMsg` that replaces `oneshot::Sender`
/// with a `request_id: u32` for cross-process transport.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WireTopicMsg {
    Publish {
        request_id: u32,
        frame: Frame,
    },
    Subscribe {
        request_id: u32,
        intent: SubscribeIntent,
        sink_id: u64,
    },
    Unsubscribe {
        request_id: u32,
        id: u64,
    },
    Replay {
        request_id: u32,
        from: i64,
        to: i64,
    },
    Snapshot {
        request_id: u32,
    },
    HeadOffset {
        request_id: u32,
    },
    DropSink {
        request_id: u32,
        sink_id: u64,
    },
    Shutdown {
        request_id: u32,
    },
}
