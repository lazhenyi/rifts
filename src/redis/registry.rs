//! Redis-aware topic registry (placeholder — Phase 3).
use std::sync::Arc;
use std::time::Duration;

use crate::redis::connection::RedisPool;
use crate::storage::{DedupeStore, LogStore, OffsetStore, SnapshotStore};
use crate::topic::profile::TopicProfile;

#[allow(dead_code)]
pub struct RedisTopicRegistry<O, L, D, S> {
    offsets: Arc<O>,
    log: Arc<L>,
    dedupe: Arc<D>,
    snapshots: Arc<S>,
    default_profile: TopicProfile,
    dedupe_window: Duration,
    pool: RedisPool,
}

impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> RedisTopicRegistry<O, L, D, S>
{
    pub fn new(
        offsets: Arc<O>,
        log: Arc<L>,
        dedupe: Arc<D>,
        snapshots: Arc<S>,
        default_profile: TopicProfile,
        dedupe_window: Duration,
        pool: RedisPool,
    ) -> Self {
        Self {
            offsets,
            log,
            dedupe,
            snapshots,
            default_profile,
            dedupe_window,
            pool,
        }
    }
}
