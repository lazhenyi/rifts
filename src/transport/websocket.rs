//! WebSocket transport — spec §3.1 (default transport).
//!
//! The `WebSocketConnection` is split into independent read/write
//! halves so that the server can read and write concurrently without
//! contending on a single mutex.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::{CloseFrame as WsCloseFrame, Message as WsMessage};
use tokio_tungstenite::{WebSocketStream, accept_async};

use crate::error::{BoxedStdError, FrameReject, Result, RiftError};
use crate::frame::{Codec as FrameCodec, Frame, FrameFlags, FrameType};
use crate::protocol::close::CloseCode;
use crate::transport::{Transport, TransportConnection, TransportListener};

/// WebSocket transport — uses `tokio-tungstenite` under the hood.
#[derive(Debug, Default, Clone, Copy)]
pub struct WebSocketTransport;

impl WebSocketTransport {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
    async fn bind(&self, addr: SocketAddr) -> Result<Box<dyn TransportListener>> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Box::new(WebSocketListener {
            inner: Arc::new(listener),
        }))
    }

    fn name(&self) -> &'static str {
        "websocket"
    }
}

struct WebSocketListener {
    inner: Arc<TcpListener>,
}

#[async_trait]
impl TransportListener for WebSocketListener {
    async fn accept(&mut self) -> Result<Box<dyn TransportConnection>> {
        let (stream, _addr) = self.inner.accept().await?;
        let ws = accept_async(stream).await?;
        Ok(Box::new(WebSocketConnection::new(ws)))
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.local_addr()?)
    }
}

/// A WebSocket-backed transport. Internally split into read/write
/// halves.
pub struct WebSocketConnection {
    reader: futures_util::stream::SplitStream<WebSocketStream<TcpStream>>,
    writer: Arc<
        tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocketStream<TcpStream>, WsMessage>>,
    >,
    peer: Option<SocketAddr>,
}

impl WebSocketConnection {
    fn new(ws: WebSocketStream<TcpStream>) -> Self {
        let peer = ws.get_ref().peer_addr().ok();
        let (writer, reader) = ws.split();
        Self {
            reader,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            peer,
        }
    }
}

#[async_trait]
impl TransportConnection for WebSocketConnection {
    async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let msg = self
                .reader
                .next()
                .await
                .ok_or_else(|| {
                    RiftError::other(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "websocket closed",
                    ))
                })?
                .map_err(|e| RiftError::WebSocket(BoxedStdError(Box::new(e))))?;
            match msg {
                WsMessage::Text(text) => {
                    return decode_text_frame(text.as_bytes());
                }
                WsMessage::Binary(bin) => {
                    return decode_binary_frame(&bin);
                }
                WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
                WsMessage::Close(close) => {
                    let _code = close.as_ref().map(|c| c.code.into()).unwrap_or(1000);
                    let _reason = close
                        .as_ref()
                        .map(|c| c.reason.as_ref())
                        .unwrap_or("")
                        .to_string();
                    return Err(RiftError::Session(crate::error::SessionReject::Expired));
                }
                _ => continue,
            }
        }
    }

    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let payload = encode_frame(frame)?;
        let mut w = self.writer.lock().await;
        w.send(WsMessage::Binary(payload.to_vec()))
            .await
            .map_err(|e| RiftError::WebSocket(BoxedStdError(Box::new(e))))?;
        w.flush()
            .await
            .map_err(|e| RiftError::WebSocket(BoxedStdError(Box::new(e))))?;
        Ok(())
    }

    async fn close(&mut self, code: CloseCode, reason: &str) -> Result<()> {
        let frame = WsCloseFrame {
            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                code.as_u16(),
            ),
            reason: reason.to_string().into(),
        };
        let mut w = self.writer.lock().await;
        let _ = w.send(WsMessage::Close(Some(frame))).await;
        let _ = w.close().await;
        Ok(())
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }
}

/// Wire format used by this server:
///
/// ```text
/// 1 byte   frame_type tag  (C/D/A/F/E)
/// 1 byte   codec tag       (J/B)
/// 2 bytes  flags (big endian)
/// 8 bytes  frame_id
/// 8 bytes  timestamp
/// 4 bytes  payload length
/// N bytes  payload
/// ```
pub fn encode_frame(frame: &Frame) -> Result<Bytes> {
    let payload = frame.payload.as_ref().cloned().unwrap_or_default();
    let mut buf = BytesMut::with_capacity(24 + payload.len());
    buf.extend_from_slice(&[frame.frame_type.tag(), frame.codec.tag()]);
    buf.extend_from_slice(&frame.flags.bits().to_be_bytes());
    buf.extend_from_slice(&frame.frame_id.to_be_bytes());
    buf.extend_from_slice(&frame.timestamp.to_be_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf.freeze())
}

pub fn decode_binary_frame(buf: &[u8]) -> Result<Frame> {
    if buf.len() < 24 {
        return Err(RiftError::Frame(FrameReject::FrameInvalid(format!(
            "binary frame too short: {}",
            buf.len()
        ))));
    }
    let frame_type = FrameType::from_tag(buf[0]).ok_or_else(|| {
        RiftError::Frame(FrameReject::FrameInvalid("unknown frame type tag".into()))
    })?;
    let codec = FrameCodec::from_tag(buf[1])
        .ok_or_else(|| RiftError::Frame(FrameReject::FrameInvalid("unknown codec tag".into())))?;
    let flags = u16::from_be_bytes([buf[2], buf[3]]);
    // Safety: buf length is at least 24 (checked above), so these slices are valid.
    let frame_id = u64::from_be_bytes([
        buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11],
    ]);
    let timestamp = i64::from_be_bytes([
        buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
    ]);
    let payload_len = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]) as usize;
    if buf.len() < 24 + payload_len {
        return Err(RiftError::Frame(FrameReject::FrameInvalid(format!(
            "payload truncated: want {}, have {}",
            payload_len,
            buf.len() - 24
        ))));
    }
    let payload = Bytes::copy_from_slice(&buf[24..24 + payload_len]);
    Ok(Frame {
        version: 0x0100,
        frame_id,
        frame_type,
        flags: FrameFlags::from_bits(flags),
        codec,
        session_id: None,
        stream_id: None,
        topic: None,
        event: None,
        message_id: None,
        correlation_id: None,
        trace_id: None,
        timestamp,
        ttl_ms: None,
        priority: None,
        payload: Some(payload),
    })
}

fn decode_text_frame(buf: &[u8]) -> Result<Frame> {
    let value: serde_json::Value = serde_json::from_slice(buf)
        .map_err(|e| RiftError::Frame(FrameReject::FrameInvalid(format!("json envelope: {e}"))))?;
    let obj = value
        .as_object()
        .ok_or_else(|| RiftError::Frame(FrameReject::FrameInvalid("expected object".into())))?;
    let frame_type = match obj.get("type").and_then(|v| v.as_str()) {
        Some("control") => FrameType::Control,
        Some("data") => FrameType::Data,
        Some("ack") => FrameType::Ack,
        Some("flow") => FrameType::Flow,
        Some("error") => FrameType::Error,
        Some(other) => {
            return Err(RiftError::Frame(FrameReject::FrameInvalid(format!(
                "unknown type: {other}"
            ))));
        }
        None => FrameType::Data,
    };
    let codec = match obj.get("codec").and_then(|v| v.as_str()) {
        Some("json") => FrameCodec::Json,
        Some("cbor") => FrameCodec::Cbor,
        _ => FrameCodec::Json,
    };
    let frame_id = obj.get("frame_id").and_then(|v| v.as_u64()).unwrap_or(0);
    let timestamp = obj.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
    let flags = obj.get("flags").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
    let payload = obj
        .get("payload")
        .map(|v| Bytes::from(serde_json::to_vec(v).unwrap_or_default()));
    Ok(Frame {
        version: 0x0100,
        frame_id,
        frame_type,
        flags: FrameFlags::from_bits(flags),
        codec,
        session_id: obj
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        stream_id: obj
            .get("stream_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        topic: obj.get("topic").and_then(|v| v.as_str()).map(String::from),
        event: obj.get("event").and_then(|v| v.as_str()).map(String::from),
        message_id: obj
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        correlation_id: obj
            .get("correlation_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        trace_id: obj
            .get("trace_id")
            .and_then(|v| v.as_str())
            .map(String::from),
        timestamp,
        ttl_ms: obj.get("ttl_ms").and_then(|v| v.as_u64()).map(|v| v as u32),
        priority: None,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_round_trip() {
        let f = Frame {
            version: 0x0100,
            frame_id: 42,
            frame_type: FrameType::Data,
            flags: FrameFlags::empty().with(FrameFlags::COMPRESSED),
            codec: FrameCodec::Cbor,
            session_id: Some("s".into()),
            stream_id: None,
            topic: Some("t".into()),
            event: Some("e".into()),
            message_id: Some("m".into()),
            correlation_id: None,
            trace_id: None,
            timestamp: 1000,
            ttl_ms: None,
            priority: None,
            payload: Some(Bytes::from_static(b"hi")),
        };
        let bytes = encode_frame(&f).unwrap();
        let back = decode_binary_frame(&bytes).unwrap();
        assert_eq!(back.frame_id, 42);
        assert_eq!(back.frame_type, FrameType::Data);
        assert_eq!(back.codec, FrameCodec::Cbor);
        assert!(back.flags.contains(FrameFlags::COMPRESSED));
        assert_eq!(back.payload.as_deref(), Some(&b"hi"[..]));
    }

    #[test]
    fn binary_too_short() {
        let r = decode_binary_frame(&[0u8; 5]);
        assert!(r.is_err());
    }

    #[test]
    fn text_envelope() {
        let json = serde_json::json!({
            "type": "data",
            "codec": "json",
            "frame_id": 1,
            "timestamp": 0,
            "flags": 0,
            "payload": {"x": 1},
        });
        let bytes = serde_json::to_vec(&json).unwrap();
        let f = decode_text_frame(&bytes).unwrap();
        assert_eq!(f.frame_type, FrameType::Data);
        assert_eq!(f.codec, FrameCodec::Json);
    }
}
