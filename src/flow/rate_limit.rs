//! Token-bucket rate limiter.

use std::time::Instant;

use parking_lot::Mutex as PlMutex;

/// A token-bucket rate limiter — `rps` tokens per second, `burst` cap.
pub struct RateLimiter {
    rps: f64,
    burst: f64,
    tokens: PlMutex<Bucket>,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter with `rps` tokens per second and a
    /// burst capacity of `burst`.
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

    /// Try to take one token. Returns `true` on success.
    pub fn try_take(&self) -> bool {
        self.try_take_n(1)
    }

    /// Try to take `n` tokens. Returns `true` if the bucket had enough.
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

/// Shared rate limiter — wrap a `RateLimiter` in an `Arc`.
pub type SharedRateLimiter = std::sync::Arc<RateLimiter>;

/// Per-connection + per-topic rate limit table.
///
/// Maps a string key to its own `RateLimiter`, lazily creating
/// limiters on first access.
pub struct RateLimitTable {
    inner: PlMutex<std::collections::HashMap<String, std::sync::Arc<RateLimiter>>>,
}

impl Default for RateLimitTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimitTable {
    /// Create an empty rate limit table.
    pub fn new() -> Self {
        Self {
            inner: PlMutex::new(std::collections::HashMap::new()),
        }
    }

    /// Get or create a limiter for `key`.
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
