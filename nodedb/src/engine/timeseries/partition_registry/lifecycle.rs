// SPDX-License-Identifier: BUSL-1.1

//! Partition state transitions: delete, merge begin/commit/rollback, purge.

use nodedb_types::timeseries::{PartitionMeta, PartitionState};

use super::entry::PartitionEntry;
use super::registry::PartitionRegistry;

impl PartitionRegistry {
    /// Mark a partition as deleted.
    pub fn mark_deleted(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts) {
            entry.meta.state = PartitionState::Deleted;
            true
        } else {
            false
        }
    }

    /// Mark a partition as merging.
    pub fn mark_merging(&mut self, start_ts: i64) -> bool {
        if let Some(entry) = self.partitions.get_mut(&start_ts)
            && entry.meta.state == PartitionState::Sealed
        {
            entry.meta.state = PartitionState::Merging;
            return true;
        }
        false
    }

    /// Insert a merged partition and mark sources as deleted.
    pub fn commit_merge(
        &mut self,
        merged_meta: PartitionMeta,
        merged_dir: String,
        source_starts: &[i64],
    ) {
        // Mark sources as deleted first (before inserting merged, in case
        // the merged partition's start_ts overlaps a source key).
        for &src in source_starts {
            self.mark_deleted(src);
        }
        // Insert (or overwrite) the merged partition.
        let start_ts = merged_meta.min_ts;
        self.partitions.insert(
            start_ts,
            PartitionEntry {
                meta: merged_meta,
                dir_name: merged_dir,
            },
        );
    }

    /// Remove deleted partitions from the registry (after physical cleanup).
    pub fn purge_deleted(&mut self) -> Vec<String> {
        let deleted: Vec<(i64, String)> = self
            .partitions
            .iter()
            .filter(|(_, e)| e.meta.state == PartitionState::Deleted)
            .map(|(&start, e)| (start, e.dir_name.clone()))
            .collect();

        let mut dirs = Vec::new();
        for (start, dir) in deleted {
            self.partitions.remove(&start);
            dirs.push(dir);
        }
        dirs
    }

    /// Roll back a partition from Merging to Sealed (merge failure recovery).
    pub fn rollback_merging(&mut self, start_ts: i64) {
        if let Some(entry) = self.partitions.get_mut(&start_ts)
            && entry.meta.state == PartitionState::Merging
        {
            entry.meta.state = PartitionState::Sealed;
        }
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
    fn commit_merge_and_purge() {
        let mut reg = PartitionRegistry::new(test_config());
        let day_ms = 86_400_000i64;

        let starts: Vec<i64> = (1..=3).map(|d| d * day_ms).collect();
        for &s in &starts {
            reg.get_or_create_partition(s);
            reg.seal_partition(s);
        }

        // Merge.
        for &s in &starts {
            reg.mark_merging(s);
        }

        let merged_meta = PartitionMeta {
            min_ts: starts[0],
            max_ts: starts[2] + day_ms - 1,
            row_count: 3000,
            size_bytes: 1024,
            schema_version: 1,
            state: PartitionState::Merged,
            interval_ms: 3 * day_ms as u64,
            last_flushed_wal_lsn: 100,
            column_stats: std::collections::HashMap::new(),
            max_system_ts: 0,
        };
        reg.commit_merge(merged_meta, "ts-merged".into(), &starts);

        // Sources are deleted, merged exists. The merged partition's min_ts
        // equals starts[0], so it overwrites one deleted entry → 3 total
        // (1 merged at starts[0], 2 deleted at starts[1] and starts[2]).
        assert_eq!(reg.partition_count(), 3);
        let dirs = reg.purge_deleted();
        assert_eq!(dirs.len(), 2); // starts[1] and starts[2]
        assert_eq!(reg.partition_count(), 1); // only the merged partition
    }
}
