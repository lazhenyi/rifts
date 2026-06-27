//! Resume orchestration — spec §5.4, §13.

use std::collections::HashMap;

use crate::error::{Result, RiftError, SessionReject};
use crate::session::offset_tracker::{OffsetTracker, ResumeDecision, decide};
use crate::session::session::Session;
use crate::topic::TopicStore;

/// What to do for a given resume attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// Session fully resumed; client may proceed.
    Resumed,
    /// Some topics replayed; client should process replayed frames.
    Partial,
    /// Server is still replaying; client should wait.
    Replaying,
    /// Client must pull a fresh snapshot for at least one topic.
    SnapshotRequired,
    /// Cold start: re-subscribe from scratch.
    ColdStart,
    /// Resume rejected — new session required.
    Rejected,
}

impl From<ResumeDecision> for ResumeOutcome {
    fn from(d: ResumeDecision) -> Self {
        match d {
            ResumeDecision::FullResume => ResumeOutcome::Resumed,
            ResumeDecision::PartialResume => ResumeOutcome::Partial,
            ResumeDecision::Replaying => ResumeOutcome::Replaying,
            ResumeDecision::SnapshotRequired => ResumeOutcome::SnapshotRequired,
            ResumeDecision::ColdStart => ResumeOutcome::ColdStart,
            ResumeDecision::Rejected => ResumeOutcome::Rejected,
        }
    }
}

/// Resume manager — checks epoch, decides outcome, exposes current
/// head offsets to the client.
pub struct ResumeManager {
    pub tracker: OffsetTracker,
}

impl Default for ResumeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ResumeManager {
    pub fn new() -> Self {
        Self {
            tracker: OffsetTracker::new(),
        }
    }

    /// Decide outcome for a resume attempt.
    ///
    /// - `session` — the existing server-side session being resumed.
    /// - `incoming_epoch` — the epoch the client claims.
    /// - `last_offsets` — last offsets the client processed.
    /// - `topic_offsets` — current head offset per topic on the server.
    pub fn evaluate(
        &self,
        session: &Session,
        incoming_epoch: u32,
        last_offsets: &HashMap<String, i64>,
        topic_offsets: &HashMap<String, i64>,
    ) -> Result<ResumeOutcome> {
        if !session.is_alive() {
            return Err(RiftError::Session(SessionReject::Expired));
        }
        if incoming_epoch != session.current_epoch() {
            return Err(RiftError::Session(SessionReject::Conflict {
                incoming: incoming_epoch,
                current: session.current_epoch(),
            }));
        }
        Ok(decide(last_offsets, topic_offsets).into())
    }

    /// Compute the head offset per topic currently in the store.
    pub fn topic_offsets(&self, store: &TopicStore, topics: &[String]) -> HashMap<String, i64> {
        let mut out = HashMap::new();
        for t in topics {
            if let Some(entry) = store.get(t) {
                out.insert(t.clone(), entry.head_offset());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::session::ClientId;
    use crate::topic::profile::TopicProfile;

    #[test]
    fn epoch_mismatch_rejected() {
        let m = ResumeManager::new();
        let s = Session::new(
            crate::session::session::SessionId::new(),
            ClientId::new("c"),
        );
        s.bump_epoch(); // server at epoch 2
        let mut last = HashMap::new();
        last.insert("t".into(), 1);
        let mut head = HashMap::new();
        head.insert("t".into(), 5);
        let r = m.evaluate(&s, 1, &last, &head);
        assert!(matches!(
            r,
            Err(RiftError::Session(SessionReject::Conflict { .. }))
        ));
    }

    #[test]
    fn happy_resume() {
        let m = ResumeManager::new();
        let s = Session::new(
            crate::session::session::SessionId::new(),
            ClientId::new("c"),
        );
        let mut last = HashMap::new();
        last.insert("t".into(), 4);
        let mut head = HashMap::new();
        head.insert("t".into(), 5);
        let r = m.evaluate(&s, s.current_epoch(), &last, &head).unwrap();
        assert_eq!(r, ResumeOutcome::Replaying);
    }

    #[test]
    fn topic_offsets_from_store() {
        use crate::broker::offset_store::OffsetStore;
        let m = ResumeManager::new();
        let store = TopicStore::new();
        let offsets = OffsetStore::new();
        let entry = store.get_or_create("t", TopicProfile::default()).unwrap();
        // Use OffsetStore for authoritative offsets.
        let o1 = offsets.alloc("t");
        let o2 = offsets.alloc("t");
        entry.append(crate::topic::store::LogEntry {
            offset: o1,
            publisher_session: None,
            message_id: "m1".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: bytes::Bytes::from_static(b"x"),
            timestamp: 0,
        });
        entry.append(crate::topic::store::LogEntry {
            offset: o2,
            publisher_session: None,
            message_id: "m2".into(),
            class: "event".into(),
            event: Some("e".into()),
            payload: bytes::Bytes::from_static(b"x"),
            timestamp: 0,
        });
        let heads = m.topic_offsets(&store, &["t".to_string()]);
        assert_eq!(heads.get("t").copied(), Some(2));
    }
}
