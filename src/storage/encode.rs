//! Key encoding helpers for storage backends.
//!
//! Each store uses a two-level namespace: `<topic_name>\x00<sub_key>`.
//! The `\x00` byte serves as a separator that ensures topic boundary
//! isolation — scanning `room/5\x00` never matches `room/50`.

/// Separator between topic name and sub-key.
pub const SEP: u8 = 0x00;

// ── Offset keys ──────────────────────────────────────────────

/// Key for a topic's current head offset.
/// Format: `<topic>\x00head`
pub fn offset_key(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 6);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(b"head");
    k
}

/// Prefix for scanning all entries for a topic.
pub fn offset_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

// ── Log keys ─────────────────────────────────────────────────

/// Key for a single log entry, sorted by offset.
/// Format: `<topic>\x00<offset:020>`
pub fn log_key(topic: &str, offset: i64) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 22);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(format!("{offset:020}").as_bytes());
    k
}

/// Prefix for scanning all log entries for a topic.
pub fn log_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

/// Inclusive start key for a replay range.
pub fn log_range_start(topic: &str, from: i64) -> Vec<u8> {
    log_key(topic, from)
}

/// Exclusive end key for a replay range (one past the end).
pub fn log_range_end(topic: &str, to: i64) -> Vec<u8> {
    // Append a byte past the 20-digit zero-padded offset so that
    // scanning up to this key includes offset `to` but not `to+1`.
    let mut k = log_key(topic, to);
    k.push(0xFF);
    k
}

// ── Dedupe keys ──────────────────────────────────────────────

/// Key for a single dedupe entry.
/// Format: `<topic>\x00<message_id>\x00`
pub fn dedupe_key(topic: &str, message_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1 + message_id.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(message_id.as_bytes());
    k.push(SEP);
    k
}

/// Prefix for scanning all dedupe entries for a topic.
pub fn dedupe_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

// ── Snapshot keys ────────────────────────────────────────────

/// Key for a single snapshot entry.
/// Format: `<topic>\x00<snapshot_id>`
pub fn snapshot_key(topic: &str, snapshot_id: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1 + snapshot_id.len());
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k.extend_from_slice(snapshot_id.as_bytes());
    k
}

/// Prefix for scanning all snapshots for a topic.
pub fn snapshot_prefix(topic: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1);
    k.extend_from_slice(topic.as_bytes());
    k.push(SEP);
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_key_has_separator() {
        let k = offset_key("room/5");
        assert!(k.windows(2).any(|w| w == [b'/', b'5']));
        assert!(k.contains(&SEP));
    }

    #[test]
    fn log_key_sort_order() {
        let k1 = log_key("t", 9);
        let k2 = log_key("t", 10);
        let k3 = log_key("t", 100);
        assert!(k1 < k2);
        assert!(k2 < k3);
    }

    #[test]
    fn log_key_topic_isolation() {
        let k_a = log_prefix("room/5");
        let k_b = log_prefix("room/50");
        // room/5\x00 should NOT match room/50\x00
        let room5_key = log_key("room/5", 1);
        assert!(room5_key.starts_with(&k_a));
        assert!(!room5_key.starts_with(&k_b));
    }

    #[test]
    fn dedupe_prefix_isolation() {
        let p = dedupe_prefix("t");
        let k = dedupe_key("t", "msg-1");
        assert!(k.starts_with(&p));
    }

    #[test]
    fn snapshot_key_round_trip() {
        let k = snapshot_key("room/1", "snap-abc");
        let p = snapshot_prefix("room/1");
        assert!(k.starts_with(&p));
    }
}
