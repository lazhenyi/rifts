//! Topic router — resolves a topic name to the entries that should
//! handle it (spec §22.3).
//!
//! In a single-process deployment the router is a no-op: every topic
//! lives in the local `TopicStore`. The trait exists so a future
//! distributed deployment can plug in a hash-based or affinity-based
//! router without changing call-sites.

use std::sync::Arc;

use crate::topic::TopicEntry;
use crate::topic::TopicStore;

/// Routing decision.
#[derive(Debug, Clone)]
pub struct Route {
    /// The topic entry that owns the message.
    pub entry: Arc<TopicEntry>,
}

/// Router trait.
pub trait TopicRouter: Send + Sync {
    fn route(&self, topic: &str, routing_key: Option<&str>) -> Option<Route>;
}

/// Single-process router — looks up the topic in the local
/// `TopicStore`, creating it on demand with the supplied default
/// profile factory.
pub struct LocalRouter {
    pub store: TopicStore,
    pub default_profile_factory: Arc<dyn Fn() -> crate::topic::TopicProfile + Send + Sync>,
}

impl std::fmt::Debug for LocalRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalRouter")
            .field("store", &self.store)
            .field("default_profile_factory", &"<fn>")
            .finish()
    }
}

impl LocalRouter {
    pub fn new(
        store: TopicStore,
        default_profile_factory: Arc<dyn Fn() -> crate::topic::TopicProfile + Send + Sync>,
    ) -> Self {
        Self {
            store,
            default_profile_factory,
        }
    }
}

impl TopicRouter for LocalRouter {
    fn route(&self, topic: &str, _routing_key: Option<&str>) -> Option<Route> {
        let entry = self
            .store
            .get_or_create(topic, (self.default_profile_factory)())
            .ok()?;
        Some(Route { entry })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_router_creates_topic() {
        let store = TopicStore::new();
        let router = LocalRouter::new(store, Arc::new(crate::topic::TopicProfile::default));
        let route = router.route("room/1", None).unwrap();
        assert_eq!(route.entry.name, "room/1");
    }
}
