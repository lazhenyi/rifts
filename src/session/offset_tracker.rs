//! Offset tracking — per-topic last processed offset for a session.
//!
//! Used to drive resume (spec §13.2): the client submits its
//! `last_offsets`; the server replays anything the client missed.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::session::session::SessionId;

/// Per-session offset tracker.
#[derive(Default)]
pub struct OffsetTracker {
    inner: Mutex<HashMap<SessionId, HashMap<String, i64>>>,
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the latest offset the session has processed for `topic`.
    pub fn record(&self, session: &SessionId, topic: &str, offset: i64) {
        let mut g = self.inner.lock();
        g.entry(session.clone())
            .or_default()
            .insert(topic.to_string(), offset);
    }

    /// Read the last recorded offset for `(session, topic)`.
    pub fn get(&self, session: &SessionId, topic: &str) -> Option<i64> {
        self.inner
            .lock()
            .get(session)
            .and_then(|m| m.get(topic).copied())
    }

    /// Bulk read of all topics for a session.
    pub fn snapshot(&self, session: &SessionId) -> HashMap<String, i64> {
        self.inner.lock().get(session).cloned().unwrap_or_default()
    }

    /// Drop a session.
    pub fn forget(&self, session: &SessionId) {
        self.inner.lock().remove(session);
    }
}

/// Resume decision result (spec §13.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeDecision {
    FullResume,
    PartialResume,
    Replaying,
    SnapshotRequired,
    ColdStart,
    Rejected,
}

/// Decide what to do with a resume attempt given the client's offsets
/// and the topic's current state.
pub fn decide(
    last_offsets: &HashMap<String, i64>,
    topic_offsets: &HashMap<String, i64>,
) -> ResumeDecision {
    if last_offsets.is_empty() {
        return ResumeDecision::ColdStart;
    }
    let mut all_within = true;
    let mut any_behind = false;
    for (topic, last) in last_offsets {
        match topic_offsets.get(topic) {
            None => {
                // Topic not present — treat as snapshot.
                return ResumeDecision::SnapshotRequired;
            }
            Some(head) => {
                if *last > *head {
                    // Client is ahead of server — reject.
                    return ResumeDecision::Rejected;
                }
                if *last < *head {
                    any_behind = true;
                }
                if *last < *head - 1 {
                    all_within = false;
                }
            }
        }
    }
    if any_behind && all_within {
        ResumeDecision::Replaying
    } else if any_behind {
        ResumeDecision::PartialResume
    } else {
        ResumeDecision::FullResume
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_read() {
        let t = OffsetTracker::new();
        let s = SessionId::new();
        t.record(&s, "room/1", 10);
        t.record(&s, "room/2", 5);
        assert_eq!(t.get(&s, "room/1"), Some(10));
        let snap = t.snapshot(&s);
        assert_eq!(snap.get("room/1"), Some(&10));
        assert_eq!(snap.get("room/2"), Some(&5));
        t.forget(&s);
        assert!(t.snapshot(&s).is_empty());
    }

    #[test]
    fn decide_cold_start_when_empty() {
        let d = decide(&HashMap::new(), &HashMap::new());
        assert_eq!(d, ResumeDecision::ColdStart);
    }

    #[test]
    fn decide_rejected_when_client_ahead() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 100);
        let mut head = HashMap::new();
        head.insert("t".to_string(), 50);
        assert_eq!(decide(&last, &head), ResumeDecision::Rejected);
    }

    #[test]
    fn decide_replaying_when_slightly_behind() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 9);
        let mut head = HashMap::new();
        head.insert("t".to_string(), 10);
        assert_eq!(decide(&last, &head), ResumeDecision::Replaying);
    }

    #[test]
    fn decide_partial_when_far_behind() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 1);
        let mut head = HashMap::new();
        head.insert("t".to_string(), 100);
        assert_eq!(decide(&last, &head), ResumeDecision::PartialResume);
    }

    #[test]
    fn decide_snapshot_when_topic_missing() {
        let mut last = HashMap::new();
        last.insert("t".to_string(), 1);
        let head = HashMap::new();
        assert_eq!(decide(&last, &head), ResumeDecision::SnapshotRequired);
    }
}
