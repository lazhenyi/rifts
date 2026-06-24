//! Transport abstraction.

pub mod frame_codec;

// Channel bridge for non-Send WS types (actix-web, ntex).
#[cfg(any(feature = "actix-web", feature = "ntex"))]
pub mod bridge;

#[cfg(feature = "actix-web")]
pub mod actix;
#[cfg(feature = "axum")]
pub mod axum;
#[cfg(feature = "ntex")]
pub mod ntex;
#[cfg(feature = "warp")]
pub mod warp;
#[cfg(feature = "websocket")]
pub mod websocket;

use async_trait::async_trait;
use std::net::SocketAddr;

use crate::error::Result;
use crate::frame::Frame;
use crate::protocol::close::CloseCode;

/// A transport binding. Implementations: WebSocket, WebTransport,
/// TCP, Unix socket.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Bind a listener on the given address.
    async fn bind(&self, addr: SocketAddr) -> Result<Box<dyn TransportListener>>;

    /// Name of the transport (e.g. `websocket`).
    fn name(&self) -> &'static str;
}

/// A bound transport listener.
#[async_trait]
pub trait TransportListener: Send {
    /// Accept the next incoming connection.
    async fn accept(&mut self) -> Result<Box<dyn TransportConnection>>;

    /// Local address the listener is bound to.
    fn local_addr(&self) -> Result<SocketAddr>;
}

/// A single bidirectional transport connection.
#[async_trait]
pub trait TransportConnection: Send {
    /// Read the next frame.
    async fn read_frame(&mut self) -> Result<Frame>;

    /// Write a frame.
    async fn write_frame(&mut self, frame: &Frame) -> Result<()>;

    /// Close with a structured close code.
    async fn close(&mut self, code: CloseCode, reason: &str) -> Result<()>;

    /// Peer address, if known.
    fn peer_addr(&self) -> Option<SocketAddr>;
}
