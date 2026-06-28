//! Token-bucket rate limiter (Rift spec section 18.2).
//!
//! This module provides a thread-safe, token-bucket rate limiter that can
//! enforce per-second throughput caps with configurable burst tolerance.
//!
//! # Algorithm
//!
//! The [`RateLimiter`] holds a bucket with a maximum capacity of `burst`
//! tokens. Tokens are replenished at a constant rate of `rps` tokens per
//! second, up to the burst cap. Consumers call [`RateLimiter::try_take`] or
//! [`RateLimiter::try_take_n`] to atomically check-and-decrement the token
//! count; the methods return `false` when insufficient tokens are available.
//!
//! # Per-connection / per-topic limiting
//!
//! The [`RateLimitTable`] maps arbitrary string keys to independent
//! [`RateLimiter`] instances. Each limiter is lazily created on first access,
//! allowing callers to enforce distinct limits per connection, per topic, or
//! per connection-topic pair without pre-allocating limiters for every
//! combination.

use std::time::Instant;

use parking_lot::Mutex as PlMutex;

/// A token-bucket rate limiter.
///
/// Each limiter allows up to `burst` tokens to be consumed in a single
/// burst, then refills at a constant rate of `rps` tokens per second.
/// Internally the limiter uses a [`parking_lot::Mutex`] to protect the
/// bucket state, keeping the critical section (a floating-point
/// arithmetic and a comparison) extremely short.
pub struct RateLimiter {
    /// Tokens replenished per second.
    rps: f64,
    /// Maximum number of tokens the bucket can hold (burst capacity).
    burst: f64,
    /// Mutable bucket state, protected by a fast mutex.
    tokens: PlMutex<Bucket>,
}

/// Internal mutable state of the token bucket.
///
/// Holds the current token count and the [`Instant`] at which the tokens
/// were last replenished.
struct Bucket {
    /// Current number of tokens in the bucket.
    tokens: f64,
    /// Timestamp of the last replenishment.
    last: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter with `rps` tokens per second and a burst
    /// capacity of `burst` tokens.
    ///
    /// The bucket starts fully replenished (i.e. `burst` tokens are
    /// immediately available).
    pub fn new(rps: u32, burst: u32) -> Self {
        Self {
            rps: rps as f64,
            burst: burst as f64,
            tokens: PlMutex::new(Bucket {
                tokens: burst as f64,
                last: Instant::now(),
            }),
        }
    }

    /// Try to consume a single token.
    ///
    /// Returns `true` if the token was available and consumed; `false`
    /// if the bucket is empty. This is a convenience wrapper around
    /// [`try_take_n`](Self::try_take_n) with `n = 1`.
    pub fn try_take(&self) -> bool {
        self.try_take_n(1)
    }

    /// Try to consume `n` tokens from the bucket.
    ///
    /// Before checking the balance the bucket is replenished based on
    /// the elapsed time since the last replenishment, capped at the
    /// burst capacity. Returns `true` if the bucket had at least `n`
    /// tokens; `false` otherwise (no tokens are consumed on failure).
    pub fn try_take_n(&self, n: u32) -> bool {
        let n = n as f64;
        let mut g = self.tokens.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(g.last).as_secs_f64();
        g.tokens = (g.tokens + elapsed * self.rps).min(self.burst);
        g.last = now;
        if g.tokens >= n {
            g.tokens -= n;
            true
        } else {
            false
        }
    }
}

/// Type alias for a shared, reference-counted [`RateLimiter`].
///
/// Used when multiple tasks need to share a single limiter, for example
/// all connections belonging to the same publisher sharing a per-topic
/// rate limit.
pub type SharedRateLimiter = std::sync::Arc<RateLimiter>;

/// Per-connection and per-topic rate limit table.
///
/// Maps a string key (typically `"conn:<id>:topic:<name>"`) to its own
/// [`RateLimiter`], lazily creating limiters on first access.
///
/// This allows operators to enforce distinct rate limits for different
/// connections or topics without pre-allocating a limiter for every
/// possible combination. The inner map is protected by a
/// [`parking_lot::Mutex`] and limiters are wrapped in [`std::sync::Arc`]
/// so they can be shared across tasks.
pub struct RateLimitTable {
    inner: PlMutex<std::collections::HashMap<String, std::sync::Arc<RateLimiter>>>,
}

impl Default for RateLimitTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimitTable {
    /// Create an empty rate limit table with no pre-allocated limiters.
    pub fn new() -> Self {
        Self {
            inner: PlMutex::new(std::collections::HashMap::new()),
        }
    }

    /// Get or create a [`RateLimiter`] for the given `key`.
    ///
    /// If a limiter for `key` already exists it is returned (the `rps`
    /// and `burst` parameters are ignored). Otherwise a new limiter is
    /// created with the supplied parameters, inserted into the table,
    /// and returned.
    pub fn get(&self, key: &str, rps: u32, burst: u32) -> std::sync::Arc<RateLimiter> {
        let mut g = self.inner.lock();
        g.entry(key.to_string())
            .or_insert_with(|| std::sync::Arc::new(RateLimiter::new(rps, burst)))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn burst_then_refill() {
        let r = RateLimiter::new(10, 3);
        assert!(r.try_take());
        assert!(r.try_take());
        assert!(r.try_take());
        assert!(!r.try_take());
        sleep(Duration::from_millis(150));
        assert!(r.try_take());
    }

    #[test]
    fn table_distinct_keys() {
        let t = RateLimitTable::new();
        let a = t.get("a", 1, 1);
        let b = t.get("b", 1, 1);
        assert!(!std::sync::Arc::ptr_eq(&a, &b));
        let a2 = t.get("a", 1, 1);
        assert!(std::sync::Arc::ptr_eq(&a, &a2));
    }
}
