// SPDX-License-Identifier: Apache-2.0

//! Bitemporal-aware validation helpers.
//!
//! CRDT constraint validation for bitemporal collections must scope to the
//! "current" version of each row — rows whose `_ts_valid_until` is open
//! (MAX or unset). A UNIQUE collision between an old (superseded) version
//! and a new live row is not a violation; two live rows with the same
//! value IS a violation.

/// Reserved column name for the valid-time upper bound.
pub const VALID_UNTIL: &str = "_ts_valid_until";

/// Reserved column name for the valid-time lower bound.
pub const VALID_FROM: &str = "_ts_valid_from";

/// Reserved column name for the system-time stamp.
pub const SYSTEM_TS: &str = "_ts_system";

/// Sentinel value representing "no upper bound" on valid-time.
pub const VALID_UNTIL_OPEN: i64 = i64::MAX;

/// Is a row "currently live" at the given system time?
///
/// A row is live when `valid_until` is open (MAX) or strictly greater
/// than the query time. Rows with `valid_until == now` are considered
/// just-superseded and NOT live.
pub fn is_live(valid_until: i64, now_ms: i64) -> bool {
    valid_until == VALID_UNTIL_OPEN || valid_until > now_ms
}

/// Do two valid-time windows overlap?
///
/// Windows are half-open `[from, until)`. Two windows overlap iff each
/// starts strictly before the other ends.
pub fn windows_overlap(a_from: i64, a_until: i64, b_from: i64, b_until: i64) -> bool {
    a_from < b_until && b_from < a_until
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_with_open_until() {
        assert!(is_live(VALID_UNTIL_OPEN, 1_000));
    }

    #[test]
    fn superseded_row_not_live() {
        assert!(!is_live(500, 1_000));
        assert!(!is_live(1_000, 1_000));
    }

    #[test]
    fn future_valid_until_is_live() {
        assert!(is_live(2_000, 1_000));
    }

    #[test]
    fn overlap_disjoint() {
        assert!(!windows_overlap(0, 100, 100, 200));
        assert!(!windows_overlap(200, 300, 0, 100));
    }

    #[test]
    fn overlap_shared_interval() {
        assert!(windows_overlap(0, 150, 100, 200));
        assert!(windows_overlap(100, 200, 0, 150));
    }

    #[test]
    fn overlap_contained() {
        assert!(windows_overlap(0, 1000, 100, 200));
        assert!(windows_overlap(100, 200, 0, 1000));
    }
}
