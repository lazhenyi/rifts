//! Actor module — power-user building blocks for custom broker topologies.
//!
//! # Overview
//!
//! The actor types let users build their own broker without touching the
//! crate internals:
//!
//! - [`TopicMsg`] — the message enum exchanged between actors and callers.
//! - [`TopicActor`] — owns a single topic's state, runs an mpsc loop.
//! - [`TopicRegistry`] — lazy `DashMap` of topic name → actor.
//! - [`LocalActorRef`] — type-safe `mpsc::Sender` wrapper.
//! - [`RemoteActorRef`] — stub for cross-process actors.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use rifts::actor::{TopicRegistry, LocalActorRef, TopicMsg};
//! use rifts::storage::{MemoryOffsetStore, MemoryLogStore, MemoryDedupeStore, MemorySnapshotStore};
//! use rifts::topic::TopicProfile;
//!
//! let registry = TopicRegistry::new(
//!     Arc::new(MemoryOffsetStore::new()),
//!     Arc::new(MemoryLogStore::new()),
//!     Arc::new(MemoryDedupeStore::new()),
//!     Arc::new(MemorySnapshotStore::new()),
//!     TopicProfile::default(),
//!     Duration::from_secs(60),
//! );
//!
//! // Get or spawn the actor for room/1.
//! let room_actor: LocalActorRef<TopicMsg> = registry.get_or_spawn("room/1");
//! ```

pub mod actor_ref;
pub mod messages;
pub mod registry;
pub mod topic_actor;

pub use actor_ref::{LocalActorRef, RemoteActorRef};
pub use messages::{TopicMsg, WireTopicMsg};
pub use registry::TopicRegistry;
pub use topic_actor::TopicActor;
