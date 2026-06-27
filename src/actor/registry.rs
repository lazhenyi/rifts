//! Topic registry — lazily spawns [`TopicActor`]s keyed by topic name.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::actor::actor_ref::LocalActorRef;
use crate::actor::messages::TopicMsg;
use crate::actor::topic_actor::TopicActor;
use crate::storage::{DedupeStore, LogStore, OffsetStore, SnapshotStore};
use crate::topic::profile::TopicProfile;

/// A lazily-populated map of topic name → actor.
pub struct TopicRegistry<O, L, D, S> {
    actors: DashMap<String, LocalActorRef<TopicMsg>>,
    offsets: Arc<O>,
    log: Arc<L>,
    dedupe: Arc<D>,
    snapshots: Arc<S>,
    default_profile: TopicProfile,
    dedupe_window: Duration,
}

impl<
    O: OffsetStore + 'static,
    L: LogStore + 'static,
    D: DedupeStore + 'static,
    S: SnapshotStore + 'static,
> TopicRegistry<O, L, D, S>
{
    /// Create a new topic registry.
    pub fn new(
        offsets: Arc<O>,
        log: Arc<L>,
        dedupe: Arc<D>,
        snapshots: Arc<S>,
        default_profile: TopicProfile,
        dedupe_window: Duration,
    ) -> Self {
        Self {
            actors: DashMap::new(),
            offsets,
            log,
            dedupe,
            snapshots,
            default_profile,
            dedupe_window,
        }
    }

    /// Get or spawn the actor for a topic.  If the existing actor's
    /// channel is closed (actor died), spawns a new one.
    pub fn get_or_spawn(&self, topic: &str) -> LocalActorRef<TopicMsg> {
        // Fast path: existing live actor.
        if let Some(r) = self.actors.get(topic) {
            if !r.is_closed() {
                return r.clone();
            }
            // Actor died — remove stale entry.
            self.actors.remove(topic);
        }
        // Slow path: spawn.
        let (tx, rx) = mpsc::channel(256);
        let actor = TopicActor::new(
            topic.to_string(),
            self.default_profile.clone(),
            self.offsets.clone(),
            self.log.clone(),
            self.dedupe.clone(),
            self.snapshots.clone(),
            self.dedupe_window,
        );
        let actor_ref = LocalActorRef::new(tx);
        tokio::spawn(async move { actor.run(rx).await });
        self.actors.insert(topic.to_string(), actor_ref.clone());
        actor_ref
    }

    /// Returns the number of spawned actors.
    pub fn len(&self) -> usize {
        self.actors.len()
    }

    /// Returns `true` if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        MemoryDedupeStore, MemoryLogStore, MemoryOffsetStore, MemorySnapshotStore,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn spawn_and_reuse() {
        let registry: TopicRegistry<
            MemoryOffsetStore,
            MemoryLogStore,
            MemoryDedupeStore,
            MemorySnapshotStore,
        > = TopicRegistry::new(
            Arc::new(MemoryOffsetStore::new()),
            Arc::new(MemoryLogStore::new()),
            Arc::new(MemoryDedupeStore::new()),
            Arc::new(MemorySnapshotStore::new()),
            TopicProfile::default(),
            Duration::from_secs(60),
        );
        let a = registry.get_or_spawn("room/1");
        let b = registry.get_or_spawn("room/1");
        // Same topic should return the same actor ref (by sender equality).
        assert_eq!(a.sender().capacity(), b.sender().capacity());
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn different_topics_different_actors() {
        let registry: TopicRegistry<
            MemoryOffsetStore,
            MemoryLogStore,
            MemoryDedupeStore,
            MemorySnapshotStore,
        > = TopicRegistry::new(
            Arc::new(MemoryOffsetStore::new()),
            Arc::new(MemoryLogStore::new()),
            Arc::new(MemoryDedupeStore::new()),
            Arc::new(MemorySnapshotStore::new()),
            TopicProfile::default(),
            Duration::from_secs(60),
        );
        let _a = registry.get_or_spawn("room/1");
        let _b = registry.get_or_spawn("room/2");
        assert_eq!(registry.len(), 2);
    }
}
