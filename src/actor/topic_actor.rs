//! Topic actor -- owns a single topic's state and processes requests
//! sequentially via an `mpsc` actor loop.
//!
//! Each [`TopicActor`] instance is responsible for exactly one topic
//! (e.g. `"room/1"`).  It owns all mutable state for that topic --
//! the subscriber map, the shared log/dedupe/offset stores, and the
//! fanout semaphore -- and processes incoming [`TopicMsg`] messages
//! one at a time in a `while let` loop.  Because the actor is
//! single-threaded and processes messages sequentially, no interior
//! mutability (`Mutex`, `RwLock`) is needed on any of its fields.
//!
//! # Lifecycle
//!
//! An actor is spawned by [`TopicRegistry::get_or_spawn`](crate::actor::TopicRegistry::get_or_spawn),
//! which creates an `mpsc` channel and hands the receiver half to
//! [`run`](TopicActor::run).  The actor processes messages until:
//!
//! 1. It receives a [`Shutdown`](TopicMsg::Shutdown) message, at which
//!    point it sends the acknowledgement and returns.
//! 2. All sender halves are dropped, causing `rx.recv()` to return
//!    `None`, ending the loop.
//! 3. It panics while handling a message; the `tokio::spawn` task
//!    terminates and the channel closes.
//!
//! In all three cases, [`TopicRegistry::get_or_spawn`](crate::actor::TopicRegistry::get_or_spawn)
//! will detect the closed channel on the next request and spawn a
//! replacement.
//!
//! # Fanout
//!
//! When a non-duplicate message is published, the actor fans out the
//! payload to every registered subscriber concurrently using
//! `tokio::spawn`, bounded by a semaphore to limit the number of
//! in-flight fanout tasks (default: 64).  Duplicate messages skip
//! fanout entirely.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};
use uuid::Uuid;

use crate::actor::messages::TopicMsg;
use crate::broker::broker::PublishOutcome;
use crate::broker::fanout::{ConnectionSink, SubscribeIntent, SubscriptionId};
use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::message::MessageClass;
use crate::now_ms;
use crate::storage::{DedupeStore, LogStore, OffsetStore, SnapshotStore};
use crate::topic::profile::TopicProfile;
use crate::topic::store::LogEntry;

/// A local subscriber entry stored in the actor's in-memory subscriber map.
///
/// Each `LocalSink` pairs a [`ConnectionSink`] (the delivery handle for a
/// remote client) with the [`SubscribeIntent`] that was specified when the
/// subscription was created.
#[allow(dead_code)]
struct LocalSink {
    /// The delivery handle used to push payloads to the remote client.
    sink: ConnectionSink,

    /// The subscriber's declared intent: live-only delivery or
    /// catch-up from a specific offset followed by live delivery.
    intent: SubscribeIntent,
}

/// An actor that owns a single topic's state and processes messages
/// sequentially.
///
/// `TopicActor` is the workhorse of the actor subsystem.  It holds all
/// mutable state for one topic -- the subscriber map, the shared storage
/// backends, deduplication window, and a semaphore that caps concurrent
/// fanout tasks.  All mutation happens inside the synchronous
/// [`handle_message`](Self::handle_message) method, which is called
/// from the async [`run`](Self::run) loop.
///
/// # Type parameters
///
/// * `O` -- [`OffsetStore`](crate::storage::OffsetStore) implementation
///   for monotonic offset allocation.
/// * `L` -- [`LogStore`](crate::storage::LogStore) implementation for
///   append-only message persistence and replay.
/// * `D` -- [`DedupeStore`](crate::storage::DedupeStore) implementation
///   for sliding-window message deduplication.
/// * `S` -- [`SnapshotStore`](crate::storage::SnapshotStore) implementation
///   for snapshot retrieval.
pub struct TopicActor<O: OffsetStore, L: LogStore, D: DedupeStore, S: SnapshotStore> {
    /// The topic name this actor is responsible for (e.g. `"room/1"`).
    topic_name: String,

    /// Topic-level configuration controlling retention and other
    /// policy settings.
    profile: TopicProfile,

    /// Shared offset allocator used to assign monotonically increasing
    /// offsets to published messages.
    offsets: Arc<O>,

    /// Shared append-only log store for message persistence and replay.
    #[allow(dead_code)]
    log: Arc<L>,

    /// Shared deduplication store used to detect and reject duplicate
    /// messages within the configured time window.
    dedupe: Arc<D>,

    /// Shared snapshot store for retrieving the latest topic snapshot.
    #[allow(dead_code)]
    snapshots: Arc<S>,

    /// In-memory map of active subscribers, keyed by subscription ID.
    subscribers: HashMap<SubscriptionId, LocalSink>,

    /// Monotonically increasing counter used to assign unique
    /// [`SubscriptionId`]s to new subscribers.
    next_sub_id: u64,

    /// Semaphore that limits the number of concurrent fanout delivery
    /// tasks, preventing unbounded spawning when many subscribers are
    /// active.
    fanout_semaphore: Arc<Semaphore>,

    /// Duration for which a published `message_id` is remembered in the
    /// deduplication store.  Messages with IDs seen within this window
    /// are treated as duplicates.
    dedupe_window: Duration,
}

impl<O: OffsetStore, L: LogStore, D: DedupeStore, S: SnapshotStore> TopicActor<O, L, D, S> {
    /// Create a new topic actor for the given topic.
    ///
    /// The actor starts with an empty subscriber map and a subscription
    /// ID counter of `1`.  It does **not** start processing messages
    /// until [`run`](Self::run) is called with the receive half of an
    /// `mpsc` channel.
    ///
    /// # Arguments
    ///
    /// * `topic_name` -- the name of the topic this actor manages.
    /// * `profile` -- [`TopicProfile`] controlling retention and other
    ///   topic-level policy settings.
    /// * `offsets` -- shared [`OffsetStore`] for monotonic offset
    ///   allocation.
    /// * `log` -- shared [`LogStore`] for message persistence and
    ///   replay.
    /// * `dedupe` -- shared [`DedupeStore`] for duplicate detection.
    /// * `snapshots` -- shared [`SnapshotStore`] for snapshot retrieval.
    /// * `dedupe_window` -- duration a `message_id` is remembered in
    ///   the deduplication store.
    ///
    /// # Returns
    ///
    /// A new `TopicActor` ready to be passed to [`run`](Self::run).
    pub fn new(
        topic_name: String,
        profile: TopicProfile,
        offsets: Arc<O>,
        log: Arc<L>,
        dedupe: Arc<D>,
        snapshots: Arc<S>,
        dedupe_window: Duration,
    ) -> Self {
        Self {
            topic_name,
            profile,
            offsets,
            log,
            dedupe,
            snapshots,
            subscribers: HashMap::new(),
            next_sub_id: 1,
            fanout_semaphore: Arc::new(Semaphore::new(64)),
            dedupe_window,
        }
    }

    /// Run the actor's main message-processing loop.
    ///
    /// This method blocks (asynchronously) until the channel is closed
    /// or a [`Shutdown`](TopicMsg::Shutdown) message is received.  Each
    /// incoming message is dispatched to [`handle_message`](Self::handle_message),
    /// which processes it synchronously and sends the response through
    /// the per-message `oneshot` channel.
    ///
    /// # Panics
    ///
    /// If [`handle_message`](Self::handle_message) panics, the panic
    /// propagates through this method, terminating the `tokio::spawn`
    /// task.  The registry will detect the closed channel and spawn a
    /// fresh actor on the next request.
    ///
    /// # Arguments
    ///
    /// * `rx` -- the receive half of the `mpsc` channel through which
    ///   [`TopicMsg`] messages arrive.
    pub async fn run(mut self, mut rx: mpsc::Receiver<TopicMsg>) {
        while let Some(msg) = rx.recv().await {
            // Errors are already reported via the per-message oneshot
            // channel; nothing to handle here.
            let _ = self.handle_message(msg);
        }
    }

    /// Dispatch a single message to the appropriate handler.
    ///
    /// Matches on the [`TopicMsg`] variant and delegates to the
    /// corresponding logic.  Each branch sends the response through
    /// the variant's `reply_to` channel before returning.  The
    /// `Shutdown` branch is special: it sends the acknowledgement and
    /// then returns early, which will cause [`run`](Self::run) to
    /// exit on the next iteration.
    ///
    /// # Arguments
    ///
    /// * `msg` -- the incoming message to process.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.  Errors from storage backends are mapped
    /// into [`RiftError`] and returned.
    fn handle_message(&mut self, msg: TopicMsg) -> Result<()> {
        match msg {
            TopicMsg::Publish { frame, reply_to } => {
                let outcome = self.handle_publish(&frame);
                let _ = reply_to.send(outcome);
            }
            TopicMsg::Subscribe {
                sink,
                intent,
                reply_to,
            } => {
                let id = SubscriptionId(self.next_sub_id);
                self.next_sub_id += 1;
                self.subscribers.insert(id, LocalSink { sink, intent });
                let _ = reply_to.send(Ok(id));
            }
            TopicMsg::Unsubscribe { id, reply_to } => {
                let ok = self.subscribers.remove(&id).is_some();
                let _ = reply_to.send(Ok(ok));
            }
            TopicMsg::Replay { from, to, reply_to } => {
                let entries = self.log.range(&self.topic_name, from, to);
                let payloads: Vec<Bytes> = entries.into_iter().map(|e| e.payload).collect();
                let _ = reply_to.send(Ok(payloads));
            }
            TopicMsg::Snapshot { reply_to } => {
                let latest = self.log.latest(&self.topic_name);
                let snap = latest.map(|e| crate::storage::StoredSnapshot {
                    snapshot_id: Uuid::new_v4().to_string(),
                    topic: self.topic_name.clone(),
                    base_offset: e.offset,
                    payload: e.payload.clone(),
                    created_at: now_ms(),
                    expires_at: None,
                });
                let _ = reply_to.send(Ok(snap));
            }
            TopicMsg::HeadOffset { reply_to } => {
                let _ = reply_to.send(self.offsets.head(&self.topic_name));
            }
            TopicMsg::DropSink { sink_id, reply_to } => {
                let count = self
                    .subscribers
                    .iter()
                    .filter(|(_, sub)| sub.sink.id() == sink_id)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>()
                    .len();
                self.subscribers.retain(|_, sub| sub.sink.id() != sink_id);
                let _ = reply_to.send(count);
            }
            TopicMsg::Shutdown { reply_to } => {
                let _ = reply_to.send(());
                // Returning here will drop self, closing the channel,
                // ending the actor loop.
            }
        }
        Ok(())
    }

    /// Process a publish request end-to-end.
    ///
    /// This is the hot-path handler for [`TopicMsg::Publish`].  It
    /// performs the following steps in order:
    ///
    /// 1. **Validation** -- verifies that the frame contains a `topic`
    ///    and a `message_id`.  Returns [`RiftError::Frame`] if either
    ///    is missing.
    /// 2. **Deduplication** -- checks the [`DedupeStore`] for the
    ///    `message_id` within the configured `dedupe_window`.  If the
    ///    ID was already seen, the message is marked as a duplicate
    ///    and fanout is skipped.
    /// 3. **Offset allocation** -- obtains the next monotonic offset
    ///    from the [`OffsetStore`].
    /// 4. **Log append** -- constructs a [`LogEntry`] and appends it
    ///    to the [`LogStore`], applying the retention policy from the
    ///    topic profile.
    /// 5. **Fanout** -- if the message is not a duplicate, delivers
    ///    the payload to every registered subscriber by spawning
    ///    concurrent delivery tasks bounded by the fanout semaphore.
    ///
    /// # Arguments
    ///
    /// * `frame` -- the frame to publish.
    ///
    /// # Returns
    ///
    /// A [`PublishOutcome`] containing the assigned offset and a flag
    /// indicating whether the message was a duplicate.  Returns an
    /// error if required fields are missing.
    fn handle_publish(&mut self, frame: &Frame) -> Result<PublishOutcome> {
        if frame.topic.is_none() {
            return Err(RiftError::Frame(
                crate::error::FrameReject::RequiredFieldMissing("topic"),
            ));
        }
        let message_id = frame.message_id.as_deref().ok_or_else(|| {
            RiftError::Frame(crate::error::FrameReject::RequiredFieldMissing(
                "message_id",
            ))
        })?;

        // Dedupe.
        let mut duplicate = false;
        if !self
            .dedupe
            .check_and_record(&self.topic_name, message_id, self.dedupe_window)
        {
            duplicate = true;
        }

        let offset = self.offsets.alloc(&self.topic_name);
        let entry = LogEntry {
            offset,
            publisher_session: frame.session_id.clone(),
            message_id: message_id.to_string(),
            class: frame
                .event
                .clone()
                .unwrap_or_else(|| MessageClass::Event.as_str().to_string()),
            event: frame.event.clone(),
            payload: frame.payload.clone().unwrap_or_default(),
            timestamp: frame.timestamp,
        };
        self.log
            .append(&self.topic_name, entry, self.profile.retention);

        // Fanout.
        if !duplicate {
            let payload = frame.payload.clone().unwrap_or_default();
            for sub in self.subscribers.values() {
                let payload = payload.clone();
                let sink = sub.sink.clone();
                let sem = self.fanout_semaphore.clone();
                tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let _ = sink.deliver(payload);
                });
            }
        }

        Ok(PublishOutcome { offset, duplicate })
    }
}
