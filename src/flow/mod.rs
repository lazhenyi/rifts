//! Flow control module — backpressure management and rate limiting (Rift spec section 18).
//!
//! This module provides the mechanisms that prevent fast producers from overwhelming
//! slow consumers and that enforce per-connection or per-topic throughput limits.
//!
//! # Backpressure
//!
//! The [`BackpressureController`] monitors per-connection outbound queue depth and
//! selects an appropriate mitigation action when the high-water mark is reached.
//! Supported strategies include pausing the producer, dropping low-priority messages,
//! coalescing duplicate state snapshots, downgrading delivery frequency, disconnecting
//! the slow consumer, or switching to snapshot-on-demand polling.
//!
//! # Rate Limiting
//!
//! The [`RateLimiter`] implements a token-bucket algorithm that can enforce per-second
//! throughput caps with configurable burst tolerance. The [`RateLimitTable`] maps
//! arbitrary string keys (e.g. connection + topic combinations) to independent
//! rate limiters, creating them lazily on first access.

pub mod backpressure;
pub mod rate_limit;

pub use backpressure::{
    BackpressureAction, BackpressureController, BackpressureStrategy, is_volatile,
};
pub use rate_limit::{RateLimitTable, RateLimiter, SharedRateLimiter};
