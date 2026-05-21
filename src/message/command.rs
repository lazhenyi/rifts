//! Command / Reply — spec §15.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{MessageReject, Result, RiftError};

/// Request-style command sent to the server (or a peer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    /// Command name.
    pub command: String,
    /// Correlation id linking this command to its reply.
    pub correlation_id: String,
    /// Request timeout in milliseconds.
    pub timeout_ms: u32,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
    /// Request payload.
    pub payload: serde_json::Value,
    /// Schema id.
    pub schema: String,
}

impl Command {
    pub fn new(
        command: impl Into<String>,
        correlation_id: impl Into<String>,
        timeout_ms: u32,
        schema: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            command: command.into(),
            correlation_id: correlation_id.into(),
            timeout_ms,
            idempotency_key: None,
            payload,
            schema: schema.into(),
        }
    }
}

/// Reply to a previously-issued command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    /// Correlation id of the originating command.
    pub correlation_id: String,
    /// Status.
    pub status: ReplyStatus,
    /// Optional response payload.
    pub payload: Option<serde_json::Value>,
    /// Optional structured error.
    pub error: Option<ReplyError>,
    /// Server time (ms since epoch).
    pub server_time: i64,
}

impl Reply {
    pub fn ok(correlation_id: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            status: ReplyStatus::Ok,
            payload: Some(payload),
            error: None,
            server_time: 0,
        }
    }

    pub fn error(
        correlation_id: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            status: ReplyStatus::Error,
            payload: None,
            error: Some(ReplyError {
                code: code.into(),
                message: message.into(),
            }),
            server_time: 0,
        }
    }
}

/// Reply status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyStatus {
    Ok,
    Error,
    Timeout,
    Rejected,
}

/// Structured error inside a reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyError {
    pub code: String,
    pub message: String,
}

pub fn encode_command(c: &Command) -> Result<Bytes> {
    if c.correlation_id.is_empty() {
        return Err(RiftError::Message(MessageReject::Rejected(
            "command requires correlation_id".into(),
        )));
    }
    Ok(Bytes::from(serde_json::to_vec(c)?))
}

pub fn decode_command(bytes: &[u8]) -> Result<Command> {
    let c: Command = serde_json::from_slice(bytes)?;
    if c.correlation_id.is_empty() {
        return Err(RiftError::Message(MessageReject::Rejected(
            "command requires correlation_id".into(),
        )));
    }
    Ok(c)
}

pub fn encode_reply(r: &Reply) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(r)?))
}

pub fn decode_reply(bytes: &[u8]) -> Result<Reply> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trip() {
        let c = Command::new(
            "room.create",
            "corr-1",
            5000,
            "room.create@1.0",
            serde_json::json!({"name": "general"}),
        );
        let bytes = encode_command(&c).unwrap();
        let back = decode_command(&bytes).unwrap();
        assert_eq!(back.correlation_id, "corr-1");
    }

    #[test]
    fn command_requires_correlation_id() {
        let c = Command {
            command: "x".into(),
            correlation_id: "".into(),
            timeout_ms: 100,
            idempotency_key: None,
            payload: serde_json::Value::Null,
            schema: "x@1.0".into(),
        };
        assert!(encode_command(&c).is_err());
    }

    #[test]
    fn reply_ok_and_error() {
        let ok = Reply::ok("c1", serde_json::json!({"id": 7}));
        assert_eq!(ok.status, ReplyStatus::Ok);
        let err = Reply::error("c1", "RIFT_AUTH_INVALID", "bad token");
        assert_eq!(err.status, ReplyStatus::Error);
    }
}
