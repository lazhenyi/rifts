//! Channel bridge for non-`Send` WebSocket types (actix-web, ntex).
//!
//! These frameworks use `Rc`-based internals, making their WS types
//! `!Send`. This module provides a `BridgeConnection` that uses tokio
//! mpsc channels to shuttle frame data between the framework's runtime
//! (actix-rt) and the tokio runtime where `Connection::run()` lives.

use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::protocol::close::CloseCode;
use crate::transport::TransportConnection;
use crate::transport::frame_codec::{decode_binary_frame, decode_text_frame, encode_frame};

/// A transport connection backed by tokio channels.
///
/// The other ends of the channels are driven by framework-specific
/// reader/writer tasks that stay on the framework's runtime.
pub struct BridgeConnection {
    /// Frames received from the framework WS (read path).
    inbox: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
    /// Frames to send to the framework WS (write path).
    outbox: mpsc::Sender<Vec<u8>>,
    peer: Option<SocketAddr>,
}

/// Spawn framework-side bridge tasks and return a
/// `BridgeConnection` for the tokio side (Send variant).
pub fn spawn_bridge(
    peer: Option<SocketAddr>,
    capacity: usize,
    spawn_reader: impl FnOnce(mpsc::Sender<Vec<u8>>) + Send + 'static,
    spawn_writer: impl FnOnce(mpsc::Receiver<Vec<u8>>) + Send + 'static,
) -> Box<dyn TransportConnection> {
    let (inbox_tx, inbox_rx) = mpsc::channel::<Vec<u8>>(capacity);
    let (outbox_tx, outbox_rx) = mpsc::channel::<Vec<u8>>(capacity);

    spawn_reader(inbox_tx);
    spawn_writer(outbox_rx);

    Box::new(BridgeConnection {
        inbox: tokio::sync::Mutex::new(inbox_rx),
        outbox: outbox_tx,
        peer,
    })
}

/// Like `spawn_bridge`, but does not require the reader/writer
/// closures to be `Send`. Use this for actix-web and ntex where the
/// framework WS types contain `Rc` and are `!Send`.
pub fn spawn_bridge_local(
    peer: Option<SocketAddr>,
    capacity: usize,
    spawn_reader: impl FnOnce(mpsc::Sender<Vec<u8>>) + 'static,
    spawn_writer: impl FnOnce(mpsc::Receiver<Vec<u8>>) + 'static,
) -> Box<dyn TransportConnection> {
    let (inbox_tx, inbox_rx) = mpsc::channel::<Vec<u8>>(capacity);
    let (outbox_tx, outbox_rx) = mpsc::channel::<Vec<u8>>(capacity);

    spawn_reader(inbox_tx);
    spawn_writer(outbox_rx);

    Box::new(BridgeConnection {
        inbox: tokio::sync::Mutex::new(inbox_rx),
        outbox: outbox_tx,
        peer,
    })
}

#[async_trait]
impl TransportConnection for BridgeConnection {
    async fn read_frame(&mut self) -> Result<Frame> {
        let raw = self.inbox.lock().await.recv().await.ok_or_else(|| {
            RiftError::other(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "bridge channel closed",
            ))
        })?;
        // The raw bytes were stored with a 1-byte tag prefix:
        // b'B' → binary frame (decode_binary_frame)
        // b'T' → text frame   (decode_text_frame)
        // b'C' → close
        match raw.first() {
            Some(b'B') => decode_binary_frame(&raw[1..]),
            Some(b'T') => decode_text_frame(&raw[1..]),
            Some(b'C') => Err(RiftError::Session(crate::error::SessionReject::Expired)),
            _ => Err(RiftError::other(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid bridge frame tag",
            ))),
        }
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        // Prefix with 'B' for binary.
        let mut buf = Vec::with_capacity(1 + payload.len());
        buf.push(b'B');
        buf.extend_from_slice(&payload);
        self.outbox.send(buf).await.map_err(|_| {
            RiftError::other(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "bridge write channel closed",
            ))
        })
    }

    async fn close(&mut self, _code: CloseCode, _reason: &str) -> Result<()> {
        let _ = self.outbox.send(vec![b'C']).await;
        Ok(())
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
