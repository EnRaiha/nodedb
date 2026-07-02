// SPDX-License-Identifier: BUSL-1.1

//! Auto-ANALYZE trigger: tracks DML counts per collection and triggers
//! automatic ANALYZE when a threshold is exceeded.
//!
//! Wired into SharedState and called from the write dispatch path after
//! each successful DML operation. When the threshold is exceeded,
//! `handle_analyze` is invoked to refresh column statistics.
//!
//! Threshold: 10% of the last ANALYZE row_count (minimum 1000 rows).
//! The counter is in-memory only — reset on restart (conservative).

use std::collections::HashMap;
use std::sync::Mutex;

/// Per-collection DML counter for auto-ANALYZE triggering.
///
/// Stored on `SharedState`. Called after successful writes to track
/// mutation volume per collection. Checked periodically or on-demand
/// to decide whether to re-ANALYZE.
pub struct DmlCounter {
    /// `(tenant_id, collection)` → mutation count since last ANALYZE.
    /// Uses Mutex + HashMap (not RwLock) because `record_dml` always
    /// needs write access to insert new entries via the entry API.
    counts: Mutex<HashMap<(u64, String), u64>>,
}

impl DmlCounter {
    pub fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
        }
    }

    /// Increment the DML count for a collection.
    ///
    /// Called after each successful INSERT/UPDATE/DELETE dispatch.
    /// Uses the entry API to atomically insert-or-increment (no TOCTOU).
    pub fn record_dml(&self, tenant_id: u64, collection: &str) {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        *map.entry((tenant_id, collection.to_string())).or_insert(0) += 1;
    }

    /// Check if a collection has exceeded the auto-ANALYZE threshold.
    ///
    /// Returns `true` if the DML count since last ANALYZE exceeds
    /// `max(last_row_count * 0.10, 1000)`.
    pub fn should_analyze(&self, tenant_id: u64, collection: &str, last_row_count: u64) -> bool {
        let threshold = (last_row_count / 10).max(1000);
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&(tenant_id, collection.to_string()))
            .copied()
            .unwrap_or(0)
            >= threshold
    }

    /// Reset the DML count for a collection (called after ANALYZE completes).
    pub fn reset(&self, tenant_id: u64, collection: &str) {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&(tenant_id, collection.to_string()));
    }
}

impl Default for DmlCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_counting() {
        let counter = DmlCounter::new();
        counter.record_dml(1, "users");
        counter.record_dml(1, "users");
        counter.record_dml(1, "users");
        assert!(!counter.should_analyze(1, "users", 0));
    }

    #[test]
    fn threshold_exceeded() {
        let counter = DmlCounter::new();
        for _ in 0..1001 {
            counter.record_dml(1, "users");
        }
        assert!(counter.should_analyze(1, "users", 0));
    }

    #[test]
    fn percentage_threshold() {
        let counter = DmlCounter::new();
        for _ in 0..10_001 {
            counter.record_dml(1, "big_table");
        }
        assert!(counter.should_analyze(1, "big_table", 100_000));
    }

    #[test]
    fn reset_clears() {
        let counter = DmlCounter::new();
        for _ in 0..2000 {
            counter.record_dml(1, "users");
        }
        assert!(counter.should_analyze(1, "users", 0));
        counter.reset(1, "users");
        assert!(!counter.should_analyze(1, "users", 0));
    }
}
