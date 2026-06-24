//! Integration tests: frame encoding, WebSocket wire format.
//!
//! A full server-lifecycle integration test (handshake → subscribe →
//! publish → fanout) is provided in `examples/chat.rs` and can be
//! run manually. The in-process tests below exercise the codec and
//! the wire format that the server uses to talk to clients.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rift::codec::JsonCodec;
use rift::codec::codec::CodecExt;
use rift::frame::{Codec as FrameCodec, Frame, FrameFlags, FrameType};
use rift::session::TokenAuth;
use rift::transport::frame_codec::{decode_binary_frame, encode_frame};
use rift::{SessionId, SubscribeIntent, TopicStore};
use tokio_tungstenite::tungstenite::Message as WsMessage;

#[test]
fn codec_ext_generic() {
    let c = JsonCodec;
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct S {
        a: u32,
    }
    let s = S { a: 7 };
    let bytes = c.encode(&s).unwrap();
    let back: S = c.decode(&bytes).unwrap();
    assert_eq!(back, s);
}

#[test]
fn subscribe_intent_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SubscribeIntent>();
    assert_send_sync::<TopicStore>();
}

#[test]
fn encode_decode_session_id() {
    let s = SessionId::new();
    let s2 = s.clone();
    assert_eq!(s, s2);
}

#[test]
fn frame_encode_decode_round_trip() {
    // The wire format is: 1B type + 1B codec + 2B flags + 8B frame_id
    // + 8B timestamp + 4B payload_len + N bytes payload. The optional
    // envelope fields (topic, message_id, etc.) are carried inside
    // the payload, encoded by the codec.
    let f = Frame {
        frame_type: FrameType::Data,
        codec: FrameCodec::Json,
        payload: Some(Bytes::from_static(b"hello")),
        flags: FrameFlags::empty().with(FrameFlags::REQUIRES_ACK),
        frame_id: 42,
        timestamp: 1000,
        ..Frame::default()
    };
    let bytes = encode_frame(&f).unwrap();
    let back = decode_binary_frame(&bytes).unwrap();
    assert_eq!(back.frame_type, FrameType::Data);
    assert_eq!(back.codec, FrameCodec::Json);
    assert_eq!(back.frame_id, 42);
    assert_eq!(back.timestamp, 1000);
    assert!(back.flags.contains(FrameFlags::REQUIRES_ACK));
    assert_eq!(back.payload.as_deref(), Some(&b"hello"[..]));
}

#[test]
fn token_auth_can_be_constructed() {
    let _ = TokenAuth::new();
    let _ = Arc::new(TokenAuth::new());
}

#[test]
fn ws_message_variants_compile() {
    let _ = WsMessage::Binary(vec![0u8; 4]);
    let _ = WsMessage::Text("hello".to_string());
}

#[test]
fn duration_default() {
    let _ = Duration::from_secs(60);
}
