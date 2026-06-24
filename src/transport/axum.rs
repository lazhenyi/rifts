//! Axum WebSocket adapter.
//!
//! Wraps `axum::extract::ws::WebSocket` as a Rift `TransportConnection`.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket};

use crate::error::{Result, RiftError};
use crate::frame::Frame;
use crate::protocol::close::CloseCode;
use crate::transport::TransportConnection;
use crate::transport::frame_codec::{decode_binary_frame, decode_text_frame, encode_frame};

/// An axum WebSocket connection adapted for the Rift protocol.
pub struct AxumWsConnection {
    ws: Arc<tokio::sync::Mutex<WebSocket>>,
    peer: Option<SocketAddr>,
}

/// Wrap an axum `WebSocket` into a `TransportConnection`.
///
/// Call this from within `ws.on_upgrade()`:
///
/// ```ignore
/// use axum::extract::ws::{WebSocket, WebSocketUpgrade};
///
/// async fn handler(ws: WebSocketUpgrade) -> impl axum::response::IntoResponse {
///     ws.on_upgrade(|socket| async move {
///         let conn = rift::transport::axum::into_connection(socket, None);
///         rift_server.accept_and_spawn(conn);
///     })
/// }
/// ```
pub fn into_connection(ws: WebSocket, peer: Option<SocketAddr>) -> Box<dyn TransportConnection> {
    Box::new(AxumWsConnection {
        ws: Arc::new(tokio::sync::Mutex::new(ws)),
        peer,
    })
}

#[async_trait]
impl TransportConnection for AxumWsConnection {
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let msg = self
                .ws
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| {
                    RiftError::other(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "axum websocket closed",
                    ))
                })?
                .map_err(RiftError::other)?;
            match msg {
                Message::Text(text) => return decode_text_frame(text.as_bytes()),
                Message::Binary(bin) => return decode_binary_frame(&bin),
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Close(_) => {
                    return Err(RiftError::Session(crate::error::SessionReject::Expired));
                }
            }
        }
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        self.ws
            .lock()
            .await
            .send(Message::Binary(payload))
            .await
            .map_err(RiftError::other)?;
        Ok(())
    }

    async fn close(&mut self, code: CloseCode, reason: &str) -> Result<()> {
        let frame = axum::extract::ws::CloseFrame {
            code: code.as_u16(),
            reason: axum::extract::ws::Utf8Bytes::from(reason),
        };
        let _ = self.ws.lock().await.send(Message::Close(Some(frame))).await;
        Ok(())
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}
