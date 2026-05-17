//! Hello / Welcome / Ready handshake — spec §5.2 – §5.5.

use std::collections::BTreeMap;

use crate::frame::Codec;

/// Client hello (spec §5.2).
#[derive(Debug, Clone, Default)]
pub struct Hello {
    pub protocol: String, // "rift"
    pub version: u16,     // major << 8 | minor
    pub client_id: Option<String>,
    pub session_id: Option<String>,
    pub epoch: Option<u32>,
    pub codecs: Vec<Codec>,
    pub compression: Vec<String>,
    pub auth_modes: Vec<AuthMode>,
    pub last_offsets: BTreeMap<String, i64>,
    pub client_clock: Option<i64>,
    pub sdk: Option<SdkInfo>,
    pub features: Vec<String>,
}

impl Hello {
    pub fn new(codecs: Vec<Codec>) -> Self {
        Self {
            protocol: crate::protocol::version::PROTOCOL_NAME.to_string(),
            version: crate::protocol::version::encoded_version(),
            codecs,
            ..Default::default()
        }
    }
}

/// Authentication mode offered by the client or accepted by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMode {
    Bearer,
    Cookie,
    Mtls,
    SignedChallenge,
    Anonymous,
}

impl AuthMode {
    pub fn name(self) -> &'static str {
        match self {
            AuthMode::Bearer => "bearer",
            AuthMode::Cookie => "cookie",
            AuthMode::Mtls => "mtls",
            AuthMode::SignedChallenge => "signed_challenge",
            AuthMode::Anonymous => "anonymous",
        }
    }
}

/// SDK identification (spec §5.2 — `sdk`).
#[derive(Debug, Clone, Default)]
pub struct SdkInfo {
    pub name: String,
    pub version: String,
}

/// Server welcome (spec §5.3) — emitted after auth.
#[derive(Debug, Clone)]
pub struct Welcome {
    pub session_id: String,
    pub epoch: u32,
    pub negotiated_codec: Codec,
    pub negotiated_compression: Option<String>,
    pub server_time: i64,
    pub resume_window_ms: u32,
    pub features: Vec<String>,
}

/// Result of an attempted resume (spec §5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeResult {
    Resumed,
    Partial,
    Rejected,
    Expired,
    Conflict,
}

/// Server `ready` (spec §5.5).
#[derive(Debug, Clone)]
pub struct Ready {
    pub session_id: String,
    pub epoch: u32,
    pub ping_interval_ms: u32,
    pub pong_timeout_ms: u32,
    pub max_missed_pongs: u32,
    pub idle_timeout_ms: u32,
    pub jitter_ms: u32,
    pub max_payload_bytes: u32,
    pub max_topics_per_connection: u32,
    pub max_send_queue_bytes: u32,
    pub server_time: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_minimal() {
        let h = Hello::new(vec![Codec::Json, Codec::Cbor]);
        assert_eq!(h.protocol, "rift");
        assert!(!h.codecs.is_empty());
    }
}
