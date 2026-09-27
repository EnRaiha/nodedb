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

/// Time elapsed since the Unix epoch.
///
/// `None` when the system clock reads earlier than the epoch. On
/// `wasm32-unknown-unknown` the value comes from `Date.now()` and therefore
/// has millisecond resolution.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn since_epoch() -> Option<Duration> {
    let millis = js_sys::Date::now();
    // `Date.now()` returns a negative `f64` for a pre-epoch clock, and `as u64`
    // saturates that to `0` rather than wrapping. Converting first would report
    // `Some(0)` — a plausible-looking zero timestamp — where the std arm below
    // reports `None`, so the two targets would disagree on a case callers map to
    // an error. `Duration::from_millis` takes a `u64`, so the guard has to come
    // before the conversion, not after.
    if millis < 0.0 {
        return None;
    }
    Some(Duration::from_millis(millis as u64))
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn since_epoch() -> Option<Duration> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
}

#[cfg(test)]
mod tests {
    use super::since_epoch;

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
}
