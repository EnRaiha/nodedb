// SPDX-License-Identifier: BUSL-1.1

//! Wall-clock access, one target split.
//!
//! `wasm32-unknown-unknown` has no std clock: `SystemTime::now()` panics with
//! "time not implemented on this platform". Builds for that target read the
//! host clock through `js_sys` instead; every other target, `wasm32-wasip1`
//! included, uses std.
//!
//! Callers keep their own conversion, saturation, and pre-epoch fallback —
//! this module only answers "how long since the Unix epoch".

use std::time::Duration;

/// A reading of `Date.now()`, in milliseconds, as an elapsed `Duration`.
///
/// `None` when the reading is negative, i.e. the clock stands before the Unix
/// epoch. Both arms agree on that contract, which is the whole point: callers
/// map the absence to an error, so a target that answered `Some(0)` instead
/// would store a plausible zero timestamp and lose the failure.
///
/// `Date.now()` returns an `f64`, and `as u64` **saturates** a negative value to
/// `0` rather than wrapping, so the guard has to come before the conversion —
/// converting first is exactly the bug this exists to prevent. Declared for
/// every target so the guard itself is testable on the host, where the
/// `js_sys` arm never compiles and could otherwise only be reviewed by eye.
#[cfg_attr(not(test), allow(dead_code))]
fn duration_from_epoch_millis(millis: f64) -> Option<Duration> {
    if millis < 0.0 {
        return None;
    }
    Some(Duration::from_millis(millis as u64))
}

/// Time elapsed since the Unix epoch.
///
/// `None` when the system clock reads earlier than the epoch. On
/// `wasm32-unknown-unknown` the value comes from `Date.now()` and therefore
/// has millisecond resolution.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn since_epoch() -> Option<Duration> {
    duration_from_epoch_millis(js_sys::Date::now())
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn since_epoch() -> Option<Duration> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
}

#[cfg(test)]
mod tests {
    use super::{duration_from_epoch_millis, since_epoch};
    use std::time::Duration;

    /// The helper exists so callers never reach `SystemTime::now()` on a
    /// target that has no clock. It must answer on every target it builds for.
    #[test]
    fn since_epoch_is_available() {
        let elapsed = since_epoch().expect("clock reads after the Unix epoch");
        assert!(
            elapsed.as_secs() > 1_600_000_000,
            "clock reads a date after 2020: {elapsed:?}"
        );
    }

    /// Callers assume the clock does not go backwards between two reads
    /// (HLC monotonicity, retry stamps, auth expiry).
    #[test]
    fn since_epoch_does_not_go_backwards() {
        let first = since_epoch().expect("clock");
        let second = since_epoch().expect("clock");
        assert!(second >= first, "{second:?} is before {first:?}");
    }

    /// A clock standing before the Unix epoch must read as absent, on every
    /// target.
    ///
    /// This is the arm `wasm32-unknown-unknown` reaches, and it cannot be
    /// exercised by running that target here — hence the guard living in a
    /// function the host can call. Without it `f64 as u64` saturates a negative
    /// reading to `0`, so the browser arm would answer `Some(0)` where the std
    /// arm answers `None`, and a caller mapping absence to an error would store
    /// a zero timestamp instead of reporting the fault.
    #[test]
    fn a_pre_epoch_reading_is_absent_not_zero() {
        assert_eq!(duration_from_epoch_millis(-1.0), None);
        assert_eq!(duration_from_epoch_millis(-1_700_000_000_000.0), None);
        assert_eq!(
            duration_from_epoch_millis(-0.5),
            None,
            "a sub-millisecond pre-epoch reading is still before the epoch"
        );
    }

    /// The epoch itself and anything after it are elapsed time, unchanged.
    #[test]
    fn a_post_epoch_reading_is_the_elapsed_time() {
        assert_eq!(duration_from_epoch_millis(0.0), Some(Duration::ZERO));
        assert_eq!(
            duration_from_epoch_millis(1_700_000_000_000.0),
            Some(Duration::from_millis(1_700_000_000_000))
        );
    }
}
