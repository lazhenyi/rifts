//! # Lightweight Distributed Tracing Context (TraceContext)
//!
//! This module implements the tracing context model defined in spec section 23.1.
//!
//! The full protocol supports five fields: `trace_id`, `span_id`, `parent_span_id`,
//! `sampled`, and `baggage`. This implementation covers the minimum set required by
//! the server side (excluding `baggage`).
//!
//! ## Tracing Model
//!
//! ```text
//! RootSpan (new_root)
//!   ├── trace_id = ULID-1
//!   ├── span_id  = ULID-2
//!   ├── parent   = None
//!   └── sampled  = true
//!         │
//!         └── ChildSpan (child)
//!               ├── trace_id = ULID-1 (inherited)
//!               ├── span_id  = ULID-3
//!               ├── parent   = ULID-2
//!               └── sampled  = true (inherited)
//! ```
//!
//! Uses ULID as the ID generator, ensuring time-ordered and globally unique identifiers.
//!
//! ## Relationship with OpenTelemetry
//!
//! `TraceContext` is a protocol-layer concept, propagated only within frames.
//! In production it should be mapped to an OTLP `SpanContext` to integrate with
//! backends such as Jaeger or Zipkin.

use ulid::Ulid;

/// Tracing context attached to protocol frames.
///
/// Each [`Frame`](crate::frame::Frame) may carry an optional `TraceContext`,
/// used to propagate tracing information between client and server for
/// end-to-end distributed tracing.
///
/// # Field Descriptions
///
/// - `trace_id`: A globally unique trace chain identifier, shared across the
///   upstream and downstream of a single business operation.
/// - `span_id`: A unique identifier for the current operation.
/// - `parent_span_id`: The span ID of the parent operation, used to build the call tree.
/// - `sampled`: Whether this trace is sampled (`true` means the tracing backend
///   should record this trace chain).
#[derive(Debug, Clone, Default)]
pub struct TraceContext {
    /// Globally unique trace chain identifier (ULID string).
    ///
    /// `None` indicates that no tracing information is present.
    pub trace_id: Option<String>,

    /// Identifier for the current operation/span (ULID string).
    pub span_id: Option<String>,

    /// Span identifier of the parent operation, used to build the call tree.
    ///
    /// For root spans, this value is `None`.
    pub parent_span_id: Option<String>,

    /// Whether this trace is sampled.
    ///
    /// `true` means the tracing backend should record this trace chain;
    /// `false` means propagate only, do not record.
    /// Defaults to `true` (set in [`new_root`](TraceContext::new_root)).
    pub sampled: bool,
}

impl TraceContext {
    /// Creates a brand-new root tracing context.
    ///
    /// Generates independent `trace_id` and `span_id` with no parent span.
    /// This is the entry point for creating a tracing context when a new
    /// connection is established on the server side.
    pub fn new_root() -> Self {
        Self {
            trace_id: Some(Ulid::new().to_string()),
            span_id: Some(Ulid::new().to_string()),
            parent_span_id: None,
            sampled: true,
        }
    }

    /// Creates a child span based on the current context.
    ///
    /// The child span inherits the parent's `trace_id` and `sampled` flag.
    /// `parent_span_id` points to the current span, and a new `span_id` is generated.
    ///
    /// Used to record sub-operations during server-side processing (e.g., creating
    /// a child span when publishing a message).
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
