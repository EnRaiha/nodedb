// SPDX-License-Identifier: BUSL-1.1

//! Memory-budget bounding for unbounded (no-LIMIT) document scans.
//!
//! A `SELECT * FROM <coll>` without a LIMIT must NOT be silently truncated.
//! Instead the Data Plane fetches every row that fits a per-core, per-query
//! byte budget (`QueryTuning::max_scan_result_bytes`) and surfaces a
//! deterministic `ResourcesExhausted` error if the result would exceed it —
//! never returning fewer rows without saying so.
//!
//! The storage scan itself accepts a row-count `fetch_limit`, so we first
//! derive a conservative row ceiling from the byte budget (assuming a small
//! minimum row size) and fetch one extra row past it. After materialization we
//! sum the actual value+key bytes and abort if they exceed the budget. This
//! bounds the data-plane `Vec` allocation to roughly the budget rather than
//! the whole collection.
//!
//! True streaming/incremental bounding (so the storage scan stops the instant
//! the byte budget is hit, mid-fetch) is tracked as a follow-on unit (U4/U5);
//! the fetch-limit + post-materialization check here is the correct bound for
//! a scan that materializes into a `Vec`.

/// Conservative lower bound on the encoded size of one scanned row
/// (value bytes + key). Used only to translate a byte budget into a
/// row-count `fetch_limit` ceiling; the authoritative bound is the
/// post-materialization byte check in [`scan_bytes_exceeded`].
const MIN_ROW_BYTES: usize = 16;

/// Translate the byte budget into a storage `fetch_limit` row ceiling for an
/// otherwise-unbounded scan.
///
/// * `limit` is the plan's row limit (`usize::MAX` for no-LIMIT scans).
/// * `offset` is the plan's row offset.
/// * `budget_bytes` is `QueryTuning::max_scan_result_bytes`.
///
/// For an explicit `LIMIT n` (`limit != usize::MAX`) the original heuristic is
/// preserved: `(limit + offset) * 2`, floored at 1000. For an unbounded scan
/// the ceiling is `budget_bytes / MIN_ROW_BYTES + 1` — the `+ 1` lets the
/// handler detect that more rows exist than fit the budget and surface the
/// error rather than silently dropping them.
pub(super) fn fetch_limit_for(limit: usize, offset: usize, budget_bytes: usize) -> usize {
    if limit == usize::MAX {
        (budget_bytes / MIN_ROW_BYTES.max(1))
            .saturating_add(1)
            .max(1000)
    } else {
        (limit.saturating_add(offset)).saturating_mul(2).max(1000)
    }
}

/// Return `true` if the materialized rows exceed the byte budget.
///
/// Sums each row's value bytes plus its id length and compares against
/// `budget_bytes`. A `budget_bytes` of 0 disables the check (treated as
/// unlimited) so the bound can be turned off via configuration.
pub(super) fn scan_bytes_exceeded(rows: &[(String, Vec<u8>)], budget_bytes: usize) -> bool {
    if budget_bytes == 0 {
        return false;
    }
    let mut total: usize = 0;
    for (id, value) in rows {
        total = total.saturating_add(value.len()).saturating_add(id.len());
        if total > budget_bytes {
            return true;
        }
    }
    false
}

/// `scan_bytes_exceeded` for the audit-log / all-versions row shape
/// `(id, system_time_ms, body)`. The system-time `i64` is fixed-width and
/// negligible; only `id` + `body` bytes are summed.
pub(super) fn version_bytes_exceeded(rows: &[(String, i64, Vec<u8>)], budget_bytes: usize) -> bool {
    if budget_bytes == 0 {
        return false;
    }
    let mut total: usize = 0;
    for (id, _sys_ms, value) in rows {
        total = total.saturating_add(value.len()).saturating_add(id.len());
        if total > budget_bytes {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_limit_uses_heuristic() {
        // (10 + 5) * 2 = 30, floored at 1000 → 1000.
        assert_eq!(fetch_limit_for(10, 5, 1024), 1000);
        // (2000 + 0) * 2 = 4000.
        assert_eq!(fetch_limit_for(2000, 0, 1024), 4000);
    }

    #[test]
    fn unbounded_limit_derives_from_budget() {
        // 1600 / 16 = 100, + 1 = 101, but floored at 1000.
        assert_eq!(fetch_limit_for(usize::MAX, 0, 1600), 1000);
        // 64 KiB / 16 = 4096, + 1 = 4097.
        assert_eq!(fetch_limit_for(usize::MAX, 0, 64 * 1024), 4097);
    }

    #[test]
    fn unbounded_limit_does_not_overflow() {
        // Must not panic / wrap on a huge budget.
        let f = fetch_limit_for(usize::MAX, usize::MAX, usize::MAX);
        assert!(f >= 1000);
    }

    #[test]
    fn bytes_within_budget_not_exceeded() {
        let rows = vec![
            ("a".to_string(), vec![0u8; 100]),
            ("b".to_string(), vec![0u8; 100]),
        ];
        // 100 + 1 + 100 + 1 = 202 <= 1024.
        assert!(!scan_bytes_exceeded(&rows, 1024));
    }

    #[test]
    fn bytes_over_budget_exceeded() {
        let rows = vec![
            ("a".to_string(), vec![0u8; 600]),
            ("b".to_string(), vec![0u8; 600]),
        ];
        // 601 + 601 = 1202 > 1024.
        assert!(scan_bytes_exceeded(&rows, 1024));
    }

    #[test]
    fn zero_budget_disables_check() {
        let rows = vec![("a".to_string(), vec![0u8; 1_000_000])];
        assert!(!scan_bytes_exceeded(&rows, 0));
    }
}
