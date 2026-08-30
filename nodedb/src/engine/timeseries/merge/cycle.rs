// SPDX-License-Identifier: BUSL-1.1

//! Background merge cycle: pick mergeable groups, merge, atomically update registry.

use std::path::{Path, PathBuf};

use crate::engine::timeseries::columnar_segment::SegmentError;
use crate::engine::timeseries::partition_registry::{PartitionRegistry, format_partition_dir};

use super::partitions::merge_partitions;

/// Execute a merge cycle with crash-safe persistence.
///
/// Three-step atomic protocol per merge:
/// 1. Write merged partition to new directory (crash → orphan, no manifest entry)
/// 2. Commit to registry + persist manifest atomically (crash → write-rename atomic)
/// 3. Background cleanup of source directories (crash → cleanup on next startup)
pub fn run_merge_cycle(
    registry: &mut PartitionRegistry,
    base_dir: &Path,
    now_ms: i64,
) -> Result<usize, SegmentError> {
    let groups = registry.find_mergeable(now_ms);
    let mut merge_count = 0;

    for group_starts in &groups {
        // Collect source directories.
        let source_dirs: Vec<PathBuf> = group_starts
            .iter()
            .filter_map(|&start| registry.get(start).map(|e| base_dir.join(&e.dir_name)))
            .collect();

        if source_dirs.len() != group_starts.len() {
            continue; // Some partitions already gone.
        }

        // Mark sources as merging.
        for &start in group_starts {
            registry.mark_merging(start);
        }

        // Determine output name.
        let (Some(&first_start), Some(&last_start)) = (group_starts.first(), group_starts.last())
        else {
            continue;
        };
        let last_entry = registry.get(last_start);
        let last_end = last_entry.map(|e| e.meta.max_ts).unwrap_or(first_start);
        let output_name = format_partition_dir(first_start, last_end);

        // Execute merge.
        match merge_partitions(base_dir, &source_dirs, &output_name) {
            Ok(result) => {
                // Step 2: Atomic manifest update.
                registry.commit_merge(result.meta, result.dir_name, group_starts);

                // Persist manifest (atomic write-rename).
                let manifest_path = base_dir.join("partition_manifest.json");
                if let Err(e) = registry.persist(&manifest_path) {
                    tracing::warn!(error = %e, "failed to persist partition manifest after merge");
                }

                merge_count += 1;
            }
            Err(_e) => {
                // Merge failed — roll back to Sealed state.
                for &start in group_starts {
                    registry.rollback_merging(start);
                }
            }
        }
    }

    Ok(merge_count)
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{MetricSample, PartitionInterval, TieredPartitionConfig};
    use tempfile::TempDir;

    use crate::engine::timeseries::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
    use crate::engine::timeseries::columnar_segment::ColumnarSegmentWriter;
    use crate::engine::timeseries::partition_registry::PartitionRegistry;

    use super::*;

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn write_test_partition(base_dir: &Path, name: &str, start_ts: i64, count: usize) -> PathBuf {
        let writer = ColumnarSegmentWriter::new(base_dir);
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..count {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: start_ts + i as i64,
                    value: (start_ts + i as i64) as f64,
                },
            );
        }
        let drain = mt.drain();
        writer
            .write_partition(name, &drain.view(), 86_400_000, 0, None)
            .unwrap();
        base_dir.join(name)
    }

    #[test]
    fn run_merge_cycle_test() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = TieredPartitionConfig::origin_defaults();
        cfg.partition_by = PartitionInterval::Duration(86_400_000);
        cfg.merge_after_ms = 1000; // 1 second for testing
        cfg.merge_count = 3;
        let mut registry = PartitionRegistry::new(cfg);

        let day_ms = 86_400_000i64;

        // Create and seal 3 partitions with data.
        for d in 1..=3 {
            let start = d * day_ms;
            let (entry, _) = registry.get_or_create_partition(start);
            let dir_name = entry.dir_name.clone();

            write_test_partition(tmp.path(), &dir_name, start, 10);

            // Update meta.
            if let Some(e) = registry.get_mut(start) {
                e.meta.row_count = 10;
                e.meta.max_ts = start + 9;
            }
            registry.seal_partition(start);
        }

        assert_eq!(registry.sealed_count(), 3);

        // Run merge — should merge all 3.
        let now = 10 * day_ms; // far enough in the future
        let merged = run_merge_cycle(&mut registry, tmp.path(), now).unwrap();
        assert_eq!(merged, 1);

        // Purge deleted.
        let deleted_dirs = registry.purge_deleted();
        assert_eq!(deleted_dirs.len(), 2); // 2 deleted (1 overwritten by merged)
    }
}
