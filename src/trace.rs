//! Lightweight trace context (spec §23.1).
//!
//! The full protocol carries `trace_id`, `span_id`, `parent_span_id`,
//! `sampled`, and `baggage`. We model the minimum needed by the
//! server: a `TraceContext` that travels with frames and an entry
//! point that creates new spans for in-flight work.

use ulid::Ulid;

/// A trace context attached to a frame.
#[derive(Debug, Clone, Default)]
pub struct TraceContext {
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub parent_span_id: Option<String>,
    pub sampled: bool,
}

impl TraceContext {
    pub fn new_root() -> Self {
        Self {
            trace_id: Some(Ulid::new().to_string()),
            span_id: Some(Ulid::new().to_string()),
            parent_span_id: None,
            sampled: true,
        }
    }

    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: Some(Ulid::new().to_string()),
            parent_span_id: self.span_id.clone(),
            sampled: self.sampled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_then_child() {
        let r = TraceContext::new_root();
        assert!(r.trace_id.is_some());
        assert!(r.span_id.is_some());
        let c = r.child();
        assert_eq!(c.trace_id, r.trace_id);
        assert_eq!(c.parent_span_id, r.span_id);
        assert_ne!(c.span_id, r.span_id);
    }
}
