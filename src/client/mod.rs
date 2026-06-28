//! # rifts-client — Rift Realtime Protocol / 1.0 Async Rust Client SDK
//!
//! Connect to a [`rifts`](crate) server over WebSocket,
//! perform the Hello/Welcome/Ready handshake, and interact with topics
//! through a typed, broadcast-based event system.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use rifts::client::{RiftClient, RiftClientConfig, ClientEvent};
//! use rifts::message::SubscribeMode;
//!
//! # async fn run() -> rifts::client::Result<()> {
//! let client = RiftClient::new(
//!     "ws://localhost:9000",
//!     RiftClientConfig {
//!         client_id: "my-app".into(),
//!         token:    "my-jwt".into(),
//!         ..Default::default()
//!     },
//! );
//!
//! let mut events = client.subscribe_events();
//! client.connect().await?;
//!
//! client.subscribe("room/1", SubscribeMode::Live, None).await?;
//! client.publish(
//!     "room/1", "chat.message", "chat.message@1.0",
//!     serde_json::json!({"text": "hello"}),
//!     None,
//! ).await?;
//!
//! while let Ok(evt) = events.recv().await {
//!     match evt {
//!         ClientEvent::EventReceived { topic, event } => {
//!             println!("[{topic}] {}: {:?}", event.event, event.payload);
//!         }
//!         ClientEvent::Disconnected { .. } => break,
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```

mod config;
mod connection;
mod error;
mod events;
pub(crate) mod frame_builder;
pub(crate) mod heartbeat;
mod rift_client;
pub(crate) mod subscriber;

pub use config::RiftClientConfig;
pub use error::{ClientError, Result};
pub use events::ClientEvent;
pub use rift_client::{CommandOpts, PublishOpts, RiftClient, StateOpts};

// Re-export commonly used types for convenience.
pub use crate::frame::{Codec, Frame, FrameFlags, FrameType, Priority};
pub use crate::message::SubscribeMode;
pub use crate::message::command::Reply;
pub use crate::protocol::close::CloseCode;
