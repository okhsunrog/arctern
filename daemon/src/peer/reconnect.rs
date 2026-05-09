//! Reconnect backoff for `PeerLink`. Exponential 1s, 2s, 4s, ... capped
//! at 60s per ARCHITECTURE.md "UI federation". Pure helper; the
//! background reconnect loop that drives it lives alongside the link
//! itself in step 9 (where it gains the openssh::Session lifecycle).

#![allow(dead_code)]

use std::time::Duration;

/// Stateless next-delay calculator. `attempt = 0` is the first retry
/// after a fresh disconnect. Caps at `MAX_BACKOFF`.
pub fn next_delay(attempt: u32) -> Duration {
    const MAX: Duration = Duration::from_secs(60);
    let shift = attempt.min(6);
    let secs: u64 = 1u64 << shift;
    Duration::from_secs(secs).min(MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_attempt_is_1s() {
        assert_eq!(next_delay(0), Duration::from_secs(1));
    }

    #[test]
    fn doubles_until_cap() {
        assert_eq!(next_delay(1), Duration::from_secs(2));
        assert_eq!(next_delay(2), Duration::from_secs(4));
        assert_eq!(next_delay(3), Duration::from_secs(8));
        assert_eq!(next_delay(4), Duration::from_secs(16));
        assert_eq!(next_delay(5), Duration::from_secs(32));
    }

    #[test]
    fn caps_at_60s() {
        assert_eq!(next_delay(6), Duration::from_secs(60));
        assert_eq!(next_delay(20), Duration::from_secs(60));
        assert_eq!(next_delay(u32::MAX), Duration::from_secs(60));
    }
}
