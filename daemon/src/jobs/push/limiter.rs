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

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    /// Elapsed virtual time across `f`, on a paused clock: tokio
    /// auto-advances to each sleep's deadline, so this measures what the
    /// limiter asked for rather than how fast the machine is.
    async fn virtual_elapsed<F, Fut>(f: F) -> StdDuration
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let start = tokio::time::Instant::now();
        f().await;
        tokio::time::Instant::now().duration_since(start)
    }

    #[tokio::test(start_paused = true)]
    async fn burst_credit_is_spent_before_anything_sleeps() {
        let l = RateLimiter::new(10 * MIB);
        // Burst is half a second of rate, so this fits inside it.
        let waited = virtual_elapsed(|| async { l.throttle(4 * MIB).await }).await;
        assert_eq!(waited, StdDuration::ZERO, "spending burst must not sleep");
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_transfer_converges_on_the_configured_rate() {
        let rate = 10 * MIB;
        let l = RateLimiter::new(rate);
        let chunk = 256 * 1024;
        let total = 40 * MIB;

        let waited = virtual_elapsed(|| async {
            let mut sent = 0;
            while sent < total {
                l.throttle(chunk).await;
                sent += chunk;
            }
        })
        .await;

        // 40 MiB at 10 MiB/s is 4s, less the half-second of burst credit
        // the bucket started with.
        let ideal = 3.5;
        let actual = waited.as_secs_f64();
        assert!(
            (actual - ideal).abs() < 0.2,
            "40 MiB at 10 MiB/s took {actual:.2}s of virtual time, expected about {ideal}s"
        );
    }

    // The bucket is shared so N concurrent senders sum to the rate
    // rather than getting it each.
    #[tokio::test(start_paused = true)]
    async fn parallel_senders_share_one_budget() {
        let rate = 10 * MIB;
        let l = std::sync::Arc::new(RateLimiter::new(rate));
        let chunk = 256 * 1024;
        let per_sender = 20 * MIB;

        let waited = virtual_elapsed(|| async {
            let mut handles = Vec::new();
            for _ in 0..2 {
                let l = l.clone();
                handles.push(tokio::spawn(async move {
                    let mut sent = 0;
                    while sent < per_sender {
                        l.throttle(chunk).await;
                        sent += chunk;
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        })
        .await;

        // Two senders of 20 MiB each is the same 40 MiB of work, so the
        // same ~3.5s — not half of it.
        let actual = waited.as_secs_f64();
        assert!(
            (actual - 3.5).abs() < 0.3,
            "two senders took {actual:.2}s; a shared budget should still take about 3.5s"
        );
    }

    // An idle gap must not bank unlimited credit, or a job that paused
    // overnight would resume by dumping at line rate. The cap is half a
    // second of rate, so an hour of idling buys 5 MiB and no more.
    #[tokio::test(start_paused = true)]
    async fn an_idle_gap_banks_at_most_the_burst() {
        let rate = 10 * MIB;
        let l = RateLimiter::new(rate);
        l.throttle(5 * MIB).await; // spend the credit it starts with
        tokio::time::sleep(StdDuration::from_secs(3600)).await;

        // 10 MiB after the gap: 5 MiB covered by the banked burst, the
        // rest paid for at the rate. Uncapped banking would make it free.
        let waited = virtual_elapsed(|| async { l.throttle(10 * MIB).await }).await;
        let actual = waited.as_secs_f64();
        assert!(
            (actual - 0.5).abs() < 0.05,
            "an hour idle then 10 MiB took {actual:.3}s, expected about 0.5s"
        );
    }
}
