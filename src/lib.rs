//! Rift Realtime Protocol / 1.0 — server crate.
//!
//! Spec compliance: section 29 (minimum compliant implementation).
//! Sections 30 (module breakdown) drive the module structure of this
//! crate.
//!
//! # Quick start
//!
//! ```no_run
//! use rift::RiftServer;
//! use std::sync::Arc;
//! use tokio::sync::Notify;
//!
//! # async fn run() -> rift::Result<()> {
//! let shutdown = Arc::new(Notify::new());
//! let server = RiftServer::builder()
//!     .websocket_transport()
//!     .build()?;
//! server.run("127.0.0.1:9000".parse().unwrap(), shutdown).await?;
//! # Ok(()) }
//! ```

pub mod ack;
pub mod broker;
pub mod codec;
pub mod config;
pub mod connection;
pub mod error;
pub mod flow;
pub mod frame;
pub mod message;
pub mod metrics;
pub mod protocol;
pub mod server;
pub mod session;
pub mod topic;
pub mod trace;
pub mod transport;

// Re-exports for the public API.
pub use broker::{Broker, InMemoryBroker, PublishOutcome, SubscribeIntent};
pub use config::{CodecOffer, DefaultTopicProfile, ServerConfig};
pub use error::{BoxedStdError, ConfigError, Result, RiftError};
pub use frame::{Codec, Frame, FrameFlags, FrameType, Priority};
pub use message::{DeliveryMode, Message, MessageClass, SubscribeMode, SubscribeResult};
pub use metrics::Metrics;
pub use protocol::close::CloseCode;
pub use protocol::error_code::ErrorCode;
pub use protocol::hello::{AuthMode, Hello, Ready, ResumeResult, SdkInfo, Welcome};
pub use server::{RiftServer, RiftServerBuilder};
pub use session::{
    AllowAllAuth, AuthContext, AuthHints, AuthProvider, ClientId, OffsetTracker, Session,
    SessionId, SessionState, TokenAuth,
};
pub use topic::{OrderingPolicy, RetentionPolicy, TopicProfile, TopicStore};
pub use transport::websocket::WebSocketTransport;
pub use transport::{Transport, TransportConnection, TransportListener};
