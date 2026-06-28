//! Actor subsystem -- power-user building blocks for custom broker topologies.
//!
//! This module provides a lightweight, channel-based actor model that lets
//! users construct their own broker layer without touching crate internals.
//! Each topic is backed by a dedicated [`TopicActor`] that owns its state
//! and processes requests sequentially through an `mpsc` loop, eliminating
//! the need for interior mutability or locks.
//!
//! # Architecture
//!
//! The actor subsystem is composed of four layers:
//!
//! 1. **Message types** ([`TopicMsg`], [`WireTopicMsg`]) -- define the
//!    request/response protocol between callers and actors.  Each
//!    [`TopicMsg`] variant carries a `oneshot::Sender` for the reply;
//!    [`WireTopicMsg`] replaces that channel with a numeric `request_id`
//!    suitable for cross-process transport.
//!
//! 2. **Actor references** ([`LocalActorRef`], [`RemoteActorRef`]) --
//!    type-safe handles that abstract over the transport.  A
//!    [`LocalActorRef`] wraps a `tokio::sync::mpsc::Sender` for
//!    in-process communication; [`RemoteActorRef`] is a stub for
//!    cross-process (TCP/CBOR) actors.
//!
//! 3. **Actor implementation** ([`TopicActor`]) -- owns a single topic's
//!    subscriber map, log store, dedupe state, and offset allocator.
//!    It runs an `async` loop that drains the `mpsc` receiver one
//!    message at a time, guaranteeing sequential access without locks.
//!
//! 4. **Registry** ([`TopicRegistry`]) -- a concurrent `DashMap` that
//!    lazily spawns one [`TopicActor`] per topic name and maintains
//!    reverse indices (`subscription -> topic`, `sink -> subscriptions`)
//!    so that lookups like "which topic owns this subscription?" are
//!    O(1) without broadcasting queries to every actor.
//!
//! # Key types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`TopicMsg`] | In-process message enum exchanged between callers and actors. |
//! | [`WireTopicMsg`] | Serializable wire counterpart of [`TopicMsg`] for cross-process transport. |
//! | [`TopicActor`] | Single-topic actor that owns state and runs an mpsc loop. |
//! | [`TopicRegistry`] | Lazily-populated map of topic name to actor, with reverse indices. |
//! | [`LocalActorRef<M>`] | Type-safe `mpsc::Sender` wrapper for in-process actors. |
//! | [`RemoteActorRef<M>`] | Stub for cross-process actors over TCP. |
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

/// Actor reference types -- type-safe handles for sending messages to
/// actors over local in-process channels or remote TCP connections.
///
/// See the [module-level documentation](self) for an overview of the
/// actor subsystem architecture.
pub mod actor_ref;

/// Message protocol between callers and topic actors.
///
/// Defines [`TopicMsg`](messages::TopicMsg) for in-process communication and
/// [`WireTopicMsg`](messages::WireTopicMsg) for serializable cross-process
/// transport.  Each variant carries a reply channel (oneshot or request ID)
/// so callers can await the actor's response.
pub mod messages;

/// Concurrent topic registry that lazily spawns one actor per topic name.
///
/// The [`TopicRegistry`](registry::TopicRegistry) uses a `DashMap` internally
/// and maintains reverse indices from subscription IDs and sink IDs back to
/// topic names, enabling O(1) lookups for `unsubscribe`, `drop_sink`, and
/// `subscriber_count` operations without broadcasting to every actor.
pub mod registry;

/// Single-topic actor implementation.
///
/// The [`TopicActor`](topic_actor::TopicActor) owns a topic's subscriber map,
/// log store, deduplication state, and offset allocator.  It runs a
/// single-threaded `mpsc` loop that processes messages sequentially,
/// eliminating the need for interior mutability or locks.
pub mod topic_actor;

/// Re-export of [`LocalActorRef`](actor_ref::LocalActorRef) and
/// [`RemoteActorRef`](actor_ref::RemoteActorRef) for convenient access.
pub use actor_ref::{LocalActorRef, RemoteActorRef};

/// Re-export of [`TopicMsg`](messages::TopicMsg) and
/// [`WireTopicMsg`](messages::WireTopicMsg) for convenient access.
pub use messages::{TopicMsg, WireTopicMsg};

/// Re-export of [`TopicRegistry`](registry::TopicRegistry) for convenient access.
pub use registry::TopicRegistry;

/// Re-export of [`TopicActor`](topic_actor::TopicActor) for convenient access.
pub use topic_actor::TopicActor;
