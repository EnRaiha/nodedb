// SPDX-License-Identifier: BUSL-1.1

//! Query-side partition selection: overlap pruning, merge candidate selection,
//! retention expiry scan.

use nodedb_types::timeseries::{PartitionState, TimeRange};

use super::entry::PartitionEntry;
use super::registry::PartitionRegistry;

impl PartitionRegistry {
    /// Find partitions that overlap a time range (for queries).
    pub fn query_partitions(&self, range: &TimeRange) -> Vec<&PartitionEntry> {
        self.partitions
            .values()
            .filter(|e| e.meta.is_queryable() && e.meta.overlaps(range))
            .collect()
    }

    /// Find partitions eligible for merging.
    ///
    /// Returns groups of `merge_count` consecutive sealed partitions
    /// that are all older than `merge_after` relative to `now_ms`.
    pub fn find_mergeable(&self, now_ms: i64) -> Vec<Vec<i64>> {
        let merge_after = self.config.merge_after_ms as i64;
        let merge_count = self.config.merge_count as usize;

        let sealed: Vec<i64> = self
            .partitions
            .iter()
            .filter(|(_, e)| {
                e.meta.state == PartitionState::Sealed && (now_ms - e.meta.max_ts) > merge_after
            })
            .map(|(&start, _)| start)
            .collect();

        let mut groups = Vec::new();
        let mut i = 0;
        while i + merge_count <= sealed.len() {
            groups.push(sealed[i..i + merge_count].to_vec());
            i += merge_count;
        }
        groups
    }

    /// Find partitions eligible for retention drop.
    ///
    /// When `bitemporal` is true, staleness is evaluated against each
    /// partition's `max_system_ts` (falling back to `max_ts` when 0 —
    /// partitions written before bitemporal tracking existed). This
    /// lets a late-arriving backfill with old event-time but current
    /// system-time survive the retention window.
    pub fn find_expired(&self, now_ms: i64, bitemporal: bool) -> Vec<i64> {
        if self.config.retention_period_ms == 0 {
            return Vec::new();
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        self.partitions
            .iter()
            .filter(|(_, e)| {
                let axis_ts = if bitemporal && e.meta.max_system_ts > 0 {
                    e.meta.max_system_ts
                } else {
                    e.meta.max_ts
                };
                axis_ts < cutoff && e.meta.state != PartitionState::Deleted
            })
            .map(|(&start, _)| start)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{PartitionInterval, TieredPartitionConfig};

    use super::*;

    fn test_config() -> TieredPartitionConfig {
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000); // 1d
        cfg.merge_after_ms = 7 * 86_400_000;
        cfg.merge_count = 3;
        cfg.retention_period_ms = 30 * 86_400_000;
        cfg
    }

    #[test]
    fn query_partitions_pruning() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;
        for d in 1..=10 {
            let (_, _) = reg.get_or_create_partition(d * day_ms);
        }

        // Query days 3-5.
        let range = TimeRange::new(3 * day_ms, 5 * day_ms + day_ms - 1);
        let results = reg.query_partitions(&range);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn find_mergeable() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        // Create and seal 6 partitions.
        for d in 1..=6 {
            reg.get_or_create_partition(d * day_ms);
            reg.seal_partition(d * day_ms);
        }

        // None mergeable yet (merge_after = 7d, data is "today").
        let now = 7 * day_ms;
        assert!(reg.find_mergeable(now).is_empty());

        // 15 days later, all are old enough. merge_count=3 → 2 groups.
        let now = 22 * day_ms;
        let groups = reg.find_mergeable(now);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 3);
    }

    #[test]
    fn find_expired() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        for d in 1..=5 {
            let start = d * day_ms;
            reg.get_or_create_partition(start);
            // Manually set max_ts so retention check works.
            if let Some(entry) = reg.partitions.get_mut(&start) {
                entry.meta.max_ts = start + day_ms - 1;
            }
        }

        // 40 days later, retention=30d → days 1-9 expired (but only 1-5 exist).
        let now = 40 * day_ms;
        let expired = reg.find_expired(now, false);
        assert_eq!(expired.len(), 5);
    }
}
