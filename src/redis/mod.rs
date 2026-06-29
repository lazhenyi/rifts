//! # Redis-Backed Multi-Instance Broker
//!
//! This module provides the Redis integration layer for the `rifts`
//! crate, enabling multiple server instances to share topic state
//! and route messages via Redis.
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────────────────┐     ┌──────────────────────┐
//! │     Instance A       │     │     Instance B       │
//! │  RedisActorBroker    │     │  RedisActorBroker    │
//! │  ┌────────────────┐  │     │  ┌────────────────┐  │
//! │  │ RedisStorage   │  │     │  │ RedisStorage   │  │
//! │  │ Offset/Log/    │  │     │  │ Offset/Log/    │  │
//! │  │ Dedupe/Snapshot│  │     │  │ Dedupe/Snapshot│  │
//! │  └───────┬────────┘  │     │  └───────┬────────┘  │
//! │  ┌───────▼────────┐  │     │  ┌───────▼────────┐  │
//! │  │ Redis Pub/Sub  │  │     │  │ Redis Pub/Sub  │  │
//! │  │ Fanout Bridge  │──┼─────┼──│ Fanout Bridge  │  │
//! │  └────────────────┘  │     │  └────────────────┘  │
//! └──────────┬───────────┘     └──────────┬───────────┘
//!            └───────────┬───────────────┘
//!                        │
//!                  ┌─────▼─────┐
//!                  │   Redis   │
//!                  │  Pub/Sub  │
//!                  │  Hashes   │
//!                  │  Sets     │
//!                  └───────────┘
//! ```
//!
//! ## Submodules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`connection`] | Redis connection pool and key helpers |
//! | [`storage`] | [`OffsetStore`], [`LogStore`], [`DedupeStore`], [`SnapshotStore`] Redis implementations |
//! | [`fanout`] | Redis Pub/Sub → local fanout bridge |
//! | [`registry`] | Redis-aware topic registry with Pub/Sub fanout |
//! | [`broker`] | [`RedisActorBroker`] implementing [`Broker`](crate::broker::Broker) |

pub mod broker;
pub mod connection;
pub mod fanout;
pub mod storage;

pub use broker::RedisActorBroker;
pub use connection::RedisPool;
pub use fanout::FanoutBridge;
pub use storage::dedupe::RedisDedupeStore;
pub use storage::log::RedisLogStore;
pub use storage::offset::RedisOffsetStore;
pub use storage::snapshot::RedisSnapshotStore;
