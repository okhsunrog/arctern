//! Shared token bucket for `bandwidth_limit`.

use std::sync::Mutex;
use std::time::Duration as StdDuration;

/// Shared token bucket for one job's outgoing bandwidth. Debt-based:
/// each stream may overshoot by one chunk and then sleeps off the
/// debt, so N parallel sends still sum to `rate`. Burst credit is
/// capped at half a second of rate — absorbs scheduler jitter without
/// letting an idle gap turn into an unthrottled surge.
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    inner: Mutex<(tokio::time::Instant, f64)>,
}

impl RateLimiter {
    pub fn new(rate: u64) -> Self {
        let rate = rate as f64;
        let burst = (rate * 0.5).max(256.0 * 1024.0);
        Self {
            rate,
            burst,
            inner: Mutex::new((tokio::time::Instant::now(), burst)),
        }
    }

    /// Account `n` sent bytes; sleeps whatever is needed to keep the
    /// aggregate under the configured rate.
    pub async fn throttle(&self, n: u64) {
        let wait = {
            let mut g = self.inner.lock().unwrap();
            let now = tokio::time::Instant::now();
            let dt = now.duration_since(g.0).as_secs_f64();
            g.0 = now;
            g.1 = (g.1 + dt * self.rate).min(self.burst);
            g.1 -= n as f64;
            if g.1 < 0.0 {
                StdDuration::from_secs_f64(-g.1 / self.rate)
            } else {
                StdDuration::ZERO
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}
