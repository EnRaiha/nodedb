// SPDX-License-Identifier: BUSL-1.1

//! Timeseries engine reclaim — remove per-collection partition
//! directories.
//!
//! Partition layout (see `dispatch/meta_retention/handlers.rs` and
//! `engine/timeseries/partition_registry.rs`):
//! `{data_dir}/ts/{database_id}/{tenant_id}/{collection}/<partition-N>/...`.
//! The directory is scoped by database + tenant, so reclaim removes only
//! the requested collection's partitions for the owning (db, tenant).

use std::path::Path;

use tracing::{debug, warn};

use super::ReclaimStats;
use crate::data::executor::handlers::timeseries::paths::ts_collection_dir;

/// Recursively remove the partition directory for `collection`.
/// Returns the total bytes freed across every regular file below it.
/// Idempotent: a missing directory counts as zero.
pub fn reclaim_timeseries_partitions(
    data_dir: &Path,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> ReclaimStats {
    let partition_dir = ts_collection_dir(data_dir, database_id, tenant_id, collection);
    if !partition_dir.exists() {
        return ReclaimStats::default();
    }

    let mut stats = ReclaimStats::default();
    tally_tree(&partition_dir, &mut stats);

    if let Err(e) = std::fs::remove_dir_all(&partition_dir) {
        warn!(
            dir = %partition_dir.display(),
            error = %e,
            "timeseries reclaim: remove_dir_all failed; partial state left for next attempt"
        );
        // Return whatever we tallied; the next idempotent purge will
        // pick up the rest.
        return stats;
    }
    debug!(
        dir = %partition_dir.display(),
        files = stats.files_unlinked,
        bytes = stats.bytes_freed,
        "timeseries reclaim: partition directory removed"
    );
    stats
}

/// Walk the tree rooted at `root`, accumulating file count and byte
/// size into `stats`. Non-fatal on errors (partial tallies are OK).
fn tally_tree(root: &Path, stats: &mut ReclaimStats) {
    let entries = match std::fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let ft = match entry.file_type() {
            Ok(f) => f,
            Err(_) => continue,
        };
        if ft.is_dir() {
            tally_tree(&path, stats);
        } else if ft.is_file() {
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            stats.files_unlinked = stats.files_unlinked.saturating_add(1);
            stats.bytes_freed = stats.bytes_freed.saturating_add(size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn removes_partition_dir_and_tallies_bytes() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();
        let coll_dir = ts_collection_dir(base, 0, 1, "metrics");
        std::fs::create_dir_all(coll_dir.join("p-001")).unwrap();
        std::fs::write(coll_dir.join("p-001").join("data.bin"), b"abcd").unwrap();
        std::fs::write(coll_dir.join("p-001").join("meta.bin"), b"ef").unwrap();
        std::fs::create_dir_all(coll_dir.join("p-002")).unwrap();
        std::fs::write(coll_dir.join("p-002").join("data.bin"), b"g").unwrap();

        // Different collection — must not be touched.
        let other = ts_collection_dir(base, 0, 1, "events");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("x.bin"), b"keep").unwrap();

        let stats = reclaim_timeseries_partitions(base, 0, 1, "metrics");
        assert_eq!(stats.files_unlinked, 3);
        assert_eq!(stats.bytes_freed, 4 + 2 + 1);
        assert!(!ts_collection_dir(base, 0, 1, "metrics").exists());
        assert!(other.join("x.bin").exists());
    }

    #[test]
    fn missing_dir_is_noop() {
        let tmp = TempDir::new().unwrap();
        let s = reclaim_timeseries_partitions(tmp.path(), 0, 1, "nope");
        assert_eq!(s.files_unlinked, 0);
    }
}
