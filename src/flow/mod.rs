//! Flow control — backpressure + rate limiting (spec §18).

pub mod backpressure;
pub mod rate_limit;

pub use backpressure::{
    BackpressureAction, BackpressureController, BackpressureStrategy, is_volatile,
};
pub use rate_limit::{RateLimitTable, RateLimiter, SharedRateLimiter};
