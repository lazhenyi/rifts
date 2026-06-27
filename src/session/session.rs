//! Session — a logical connection that can outlive a single
//! transport connection (spec §5.4, §13).

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use crate::now_ms;

use ulid::Ulid;

/// Unique session id (ULID on the wire).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionId(pub String);

impl SessionId {
    pub fn new() -> Self {
        Self(Ulid::new().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Client long-lived identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

impl ClientId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Per-session lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Open,
    Hello,
    Authenticated,
    Resuming,
    Ready,
    Active,
    Draining,
    Closed,
}

/// A logical session — one per authenticated client. Survives
/// transport reconnects via `resume`.
pub struct Session {
    pub id: SessionId,
    pub client_id: ClientId,
    pub epoch: AtomicU32,
    pub state: parking_lot::Mutex<SessionState>,
    pub created_at: i64,
    pub last_active: AtomicU64,
}

impl Session {
    pub fn new(id: SessionId, client_id: ClientId) -> Self {
        let now = now_ms();
        Self {
            id,
            client_id,
            epoch: AtomicU32::new(1),
            state: parking_lot::Mutex::new(SessionState::Open),
            created_at: now,
            last_active: AtomicU64::new(now as u64),
        }
    }

    pub fn state(&self) -> SessionState {
        *self.state.lock()
    }

    pub fn set_state(&self, s: SessionState) {
        *self.state.lock() = s;
        self.touch();
    }

    pub fn touch(&self) {
        self.last_active.store(now_ms() as u64, Ordering::SeqCst);
    }

    pub fn current_epoch(&self) -> u32 {
        self.epoch.load(Ordering::SeqCst)
    }

    pub fn bump_epoch(&self) -> u32 {
        self.epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn is_alive(&self) -> bool {
        !matches!(self.state(), SessionState::Closed)
    }

    /// Returns the duration since the session was last touched.
    pub fn idle(&self) -> Duration {
        let last = self.last_active.load(Ordering::SeqCst) as i64;
        Duration::from_millis((now_ms() - last).max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_lifecycle() {
        let s = Session::new(SessionId::new(), ClientId::new("c1"));
        assert_eq!(s.state(), SessionState::Open);
        s.set_state(SessionState::Ready);
        assert_eq!(s.state(), SessionState::Ready);
        assert_eq!(s.current_epoch(), 1);
        assert_eq!(s.bump_epoch(), 2);
        s.set_state(SessionState::Closed);
        assert!(!s.is_alive());
    }

    #[test]
    fn session_id_is_unique() {
        let a = SessionId::new();
        let b = SessionId::new();
        assert_ne!(a, b);
    }
}
