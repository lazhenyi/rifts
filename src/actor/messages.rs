//! Messages that a [`TopicActor`](crate::actor::TopicActor) can process.
//!
//! This module defines the request/response protocol between callers and
//! topic actors.  There are two message enums:
//!
//! - [`TopicMsg`] -- the in-process message type exchanged over an
//!   `mpsc` channel.  Each variant carries a `oneshot::Sender` in its
//!   `reply_to` field so the caller can `await` the actor's response
//!   asynchronously.
//!
//! - [`WireTopicMsg`] -- a serializable counterpart of [`TopicMsg`] for
//!   cross-process transport.  It replaces the `oneshot::Sender` with a
//!   numeric `request_id: u32` that can be correlated on the wire.  The
//!   enum derives `serde::Serialize` and `serde::Deserialize` and is
//!   designed to be encoded with CBOR or any other Serde-compatible format.
//!
//! # Design rationale
//!
//! The request/response pattern using `oneshot` channels avoids the need
//! for callers to match on response types or manage correlation IDs
//! manually.  Each `TopicMsg` variant is self-contained: the caller
//! constructs the message, sends it through the [`LocalActorRef`](crate::actor::LocalActorRef),
//! and awaits the oneshot receiver for the result.

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::broker::broker::PublishOutcome;
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::error::Result;
use crate::frame::Frame;
use crate::storage::StoredSnapshot;

/// In-process message enum that a [`TopicActor`](crate::actor::TopicActor)
/// processes inside its `mpsc` loop.
///
/// Each variant represents a distinct operation on the topic and carries a
/// `reply_to: oneshot::Sender<...>` field so the caller can receive the
/// actor's response asynchronously.  The actor processes these messages
/// sequentially, guaranteeing that no concurrent mutation of the topic
/// state occurs.
///
/// # Variants
///
/// See the individual variant documentation for details on each operation.
pub enum TopicMsg {
    /// Publish a frame to the topic.
    ///
    /// The actor will:
    /// 1. Validate that the frame has a `topic` and `message_id`.
    /// 2. Check the deduplication store -- if the `message_id` was seen
    ///    within the configured window, the message is marked as a duplicate.
    /// 3. Allocate an offset from the offset store.
    /// 4. Append the entry to the log store.
    /// 5. Fan out the payload to all live subscribers (unless duplicate).
    ///
    /// # Fields
    ///
    /// * `frame` -- the frame to publish, containing topic, payload,
    ///   message ID, and metadata.
    /// * `reply_to` -- channel to send back the [`PublishOutcome`],
    ///   which includes the assigned offset and whether the message
    ///   was a duplicate.
    Publish {
        frame: Frame,
        reply_to: oneshot::Sender<Result<PublishOutcome>>,
    },

    /// Subscribe a connection sink to the topic.
    ///
    /// Registers a new subscriber in the actor's internal map and
    /// assigns a unique [`SubscriptionId`].  The subscriber will
    /// receive future messages via fanout (for [`SubscribeIntent::Live`])
    /// or can request a replay separately.
    ///
    /// # Fields
    ///
    /// * `sink` -- the [`ConnectionSink`] (an `Arc<dyn FanoutSink>`)
    ///   that delivers serialized payloads to the remote client.
    /// * `intent` -- whether the subscriber wants live messages only
    ///   or also a catch-up from a specific offset.
    /// * `reply_to` -- channel to send back the assigned
    ///   [`SubscriptionId`].
    Subscribe {
        sink: ConnectionSink,
        intent: SubscribeIntent,
        reply_to: oneshot::Sender<Result<SubscriptionId>>,
    },

    /// Unsubscribe a previously registered subscription by its ID.
    ///
    /// Removes the subscription from the actor's internal map.  Returns
    /// `true` if the subscription existed and was removed, `false` if
    /// the ID was not found (already unsubscribed or never registered).
    ///
    /// # Fields
    ///
    /// * `id` -- the [`SubscriptionId`] to remove.
    /// * `reply_to` -- channel to send back whether the unsubscription
    ///   succeeded.
    Unsubscribe {
        id: SubscriptionId,
        reply_to: oneshot::Sender<Result<bool>>,
    },

    /// Replay messages from the topic's log in a given offset range.
    ///
    /// Reads entries from the log store for offsets `[from, to)` and
    /// returns their payloads as raw `Bytes`.  This is used for
    /// catch-up when a subscriber joins with a historical offset.
    ///
    /// # Fields
    ///
    /// * `from` -- inclusive start offset (use `0` for the beginning).
    /// * `to` -- exclusive end offset.
    /// * `reply_to` -- channel to send back the vector of message
    ///   payloads.
    Replay {
        from: i64,
        to: i64,
        reply_to: oneshot::Sender<Result<Vec<Bytes>>>,
    },

    /// Fetch the latest snapshot for this topic.
    ///
    /// A snapshot is constructed from the most recent log entry.  If
    /// the log is empty, `None` is returned.  The snapshot includes a
    /// generated UUID, the current base offset, the payload, and
    /// timestamps.
    ///
    /// # Fields
    ///
    /// * `reply_to` -- channel to send back the optional
    ///   [`StoredSnapshot`].
    Snapshot {
        reply_to: oneshot::Sender<Result<Option<StoredSnapshot>>>,
    },

    /// Retrieve the current head (highest allocated) offset for the topic.
    ///
    /// The head offset is the next offset that *would* be allocated --
    /// i.e., one past the last written entry.  Returns `0` if no
    /// messages have been published yet.
    ///
    /// # Fields
    ///
    /// * `reply_to` -- channel to send back the head offset as `i64`.
    HeadOffset { reply_to: oneshot::Sender<i64> },

    /// Drop all subscriptions associated with a given sink ID.
    ///
    /// When a connection disconnects, the broker calls this to remove
    /// all subscriptions that belong to that connection's sink.  Returns
    /// the number of subscriptions that were removed.
    ///
    /// # Fields
    ///
    /// * `sink_id` -- the numeric sink identifier to match against.
    /// * `reply_to` -- channel to send back the count of removed
    ///   subscriptions.
    DropSink {
        sink_id: u64,
        reply_to: oneshot::Sender<usize>,
    },

    /// Request a graceful shutdown of the actor.
    ///
    /// The actor will acknowledge the shutdown via `reply_to` and then
    /// exit its `run` loop, causing the `mpsc` receiver to be dropped.
    /// [`TopicRegistry::get_or_spawn`](crate::actor::TopicRegistry::get_or_spawn)
    /// will detect the closed channel and spawn a fresh actor on the
    /// next request.
    ///
    /// # Fields
    ///
    /// * `reply_to` -- channel to send back `()` once the actor has
    ///   acknowledged the shutdown.
    Shutdown { reply_to: oneshot::Sender<()> },
}

impl std::fmt::Debug for TopicMsg {
    /// Format the message variant name for debugging.
    ///
    /// Prints only the variant name (e.g. `"Publish"`, `"Subscribe"`)
    /// without the fields, since `ConnectionSink` and `oneshot::Sender`
    /// do not implement `Debug` in a meaningful way. Useful scalar
    /// fields (topic / message_id / sink_id / offset range) are
    /// surfaced for operational visibility.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicMsg::Publish { frame, .. } => f
                .debug_struct("Publish")
                .field("topic", &frame.topic)
                .field("message_id", &frame.message_id)
                .finish(),
            TopicMsg::Subscribe { .. } => f.write_str("Subscribe"),
            TopicMsg::Unsubscribe { id, .. } => write!(f, "Unsubscribe {{ id: {:?} }}", id),
            TopicMsg::Replay { from, to, .. } => {
                write!(f, "Replay {{ from: {}, to: {} }}", from, to)
            }
            TopicMsg::Snapshot { .. } => f.write_str("Snapshot"),
            TopicMsg::HeadOffset { .. } => f.write_str("HeadOffset"),
            TopicMsg::DropSink { sink_id, .. } => {
                write!(f, "DropSink {{ sink_id: {} }}", sink_id)
            }
            TopicMsg::Shutdown { .. } => f.write_str("Shutdown"),
        }
    }
}

/// A wire-friendly version of [`TopicMsg`] for cross-process transport.
///
/// `WireTopicMsg` mirrors every variant of [`TopicMsg`] but replaces the
/// `oneshot::Sender` reply channel with a `request_id: u32` that can be
/// serialized and sent over a network connection.  The remote side
/// includes the same `request_id` in its response so the caller can
/// correlate replies with outstanding requests.
///
/// This enum derives `serde::Serialize` and `serde::Deserialize` and is
/// designed for use with CBOR, JSON, or any other Serde-compatible
/// wire format.
///
/// # Fields (common to all variants)
///
/// * `request_id` -- a caller-assigned numeric identifier used to
///   correlate the response with this request.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WireTopicMsg {
    /// Publish a frame to the topic (wire equivalent of
    /// [`TopicMsg::Publish`]).
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    /// * `frame` -- boxed frame to publish (boxed to reduce enum size).
    Publish { request_id: u32, frame: Box<Frame> },

    /// Subscribe a sink to the topic (wire equivalent of
    /// [`TopicMsg::Subscribe`]).
    ///
    /// Unlike the in-process variant, the `sink` is represented by a
    /// `sink_id: u64` rather than a `ConnectionSink` reference, since
    /// the actual sink lives on the remote side.
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    /// * `intent` -- the subscriber's intent (live-only or catch-up).
    /// * `sink_id` -- numeric identifier of the remote sink.
    Subscribe {
        request_id: u32,
        intent: SubscribeIntent,
        sink_id: u64,
    },

    /// Unsubscribe by subscription ID (wire equivalent of
    /// [`TopicMsg::Unsubscribe`]).
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    /// * `id` -- the subscription ID to remove (as a raw `u64`).
    Unsubscribe { request_id: u32, id: u64 },

    /// Replay messages in an offset range (wire equivalent of
    /// [`TopicMsg::Replay`]).
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    /// * `from` -- inclusive start offset.
    /// * `to` -- exclusive end offset.
    Replay { request_id: u32, from: i64, to: i64 },

    /// Fetch the latest snapshot (wire equivalent of
    /// [`TopicMsg::Snapshot`]).
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    Snapshot { request_id: u32 },

    /// Get the head offset (wire equivalent of
    /// [`TopicMsg::HeadOffset`]).
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    HeadOffset { request_id: u32 },

    /// Drop all subscriptions for a sink (wire equivalent of
    /// [`TopicMsg::DropSink`]).
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    /// * `sink_id` -- the numeric sink identifier to match.
    DropSink { request_id: u32, sink_id: u64 },

    /// Graceful shutdown request (wire equivalent of
    /// [`TopicMsg::Shutdown`]).
    ///
    /// # Fields
    ///
    /// * `request_id` -- correlation identifier for the response.
    Shutdown { request_id: u32 },
}
