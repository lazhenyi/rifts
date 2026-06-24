//! Warp WebSocket adapter.
//!
//! Wraps `warp::ws::WebSocket` as a Rift `TransportConnection`.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};

use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::protocol::close::CloseCode;
use crate::transport::TransportConnection;
use crate::transport::frame_codec::{decode_binary_frame, decode_text_frame, encode_frame};

/// A warp WebSocket connection adapted for the Rift protocol.
pub struct WarpWsConnection {
    rx: Arc<tokio::sync::Mutex<futures_util::stream::SplitStream<warp::ws::WebSocket>>>,
    tx: Arc<
        tokio::sync::Mutex<futures_util::stream::SplitSink<warp::ws::WebSocket, warp::ws::Message>>,
    >,
    peer: Option<SocketAddr>,
}

/// Wrap a warp `WebSocket` into a `TransportConnection`.
///
/// Call this from within `ws.on_upgrade()`:
///
/// ```ignore
/// warp::ws::ws()
///     .map(|ws: warp::ws::Ws| {
///         ws.on_upgrade(|socket| async move {
///             let conn = rift::transport::warp::into_connection(socket, None);
///             rift_server.accept_and_spawn(conn);
///         })
///     });
/// ```
pub fn into_connection(
    ws: warp::ws::WebSocket,
    peer: Option<SocketAddr>,
) -> Box<dyn TransportConnection> {
    let (tx, rx) = ws.split();
    Box::new(WarpWsConnection {
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
        tx: Arc::new(tokio::sync::Mutex::new(tx)),
        peer,
    })
}

#[async_trait]
impl TransportConnection for WarpWsConnection {
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let msg = self
                .rx
                .lock()
                .await
                .next()
                .await
                .ok_or_else(|| {
                    RiftError::other(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "warp websocket closed",
                    ))
                })?
                .map_err(|e| RiftError::other(std::io::Error::other(format!("{e:?}"))))?;
            if msg.is_text() {
                let text = msg
                    .to_str()
                    .map_err(|e| RiftError::other(std::io::Error::other(format!("{e:?}"))))?;
                return decode_text_frame(text.as_bytes());
            }
            if msg.is_binary() {
                return decode_binary_frame(&msg.into_bytes());
            }
            if msg.is_ping() || msg.is_pong() {
                continue;
            }
            if msg.is_close() {
                return Err(RiftError::Session(crate::error::SessionReject::Expired));
            }
        }
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        self.tx
            .lock()
            .await
            .send(warp::ws::Message::binary(payload.to_vec()))
            .await
            .map_err(RiftError::other)?;
        Ok(())
    }

    async fn close(&mut self, _code: CloseCode, _reason: &str) -> Result<()> {
        let msg = warp::ws::Message::close();
        let _ = self.tx.lock().await.send(msg).await;
        Ok(())
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
