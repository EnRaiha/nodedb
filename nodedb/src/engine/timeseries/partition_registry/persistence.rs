// SPDX-License-Identifier: BUSL-1.1

//! Manifest persistence, recovery, and orphan cleanup.

use nodedb_types::timeseries::{PartitionState, TieredPartitionConfig};

use super::entry::PartitionEntry;
use super::registry::PartitionRegistry;

impl PartitionRegistry {
    /// Export registry state for persistence.
    pub fn export(&self) -> Vec<(i64, PartitionEntry)> {
        self.partitions
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect()
    }

    /// Import persisted registry state.
    ///
    /// Callers that already hold a key issued by this registry (re-import of an
    /// export) use this. Anything registering a NEWLY written partition must go
    /// through [`PartitionRegistry::insert_partition`], which cannot displace a
    /// partition that happens to share a start timestamp.
    pub fn import(&mut self, entries: Vec<(i64, PartitionEntry)>) {
        for (start, entry) in entries {
            self.partitions.insert(start, entry);
        }
    }

    /// Register a newly written partition, returning the key it was filed under.
    ///
    /// The map key is a handle ordered by start timestamp; nothing resolves a
    /// partition by computing a boundary from it — every consumer either
    /// iterates the map or reuses a key the map handed back. Two partitions can
    /// legitimately share a `min_ts` (late or duplicate-timestamp ingest), so
    /// filing strictly under `min_ts` would silently drop one of them while its
    /// rows sit on disk and a checkpoint has already reported them durable. A
    /// colliding partition takes the next free slot instead.
    ///
    /// Re-registering the SAME directory is idempotent — that is a restore or a
    /// boot rescan seeing a partition it already knows, not a second partition.
    pub fn insert_partition(&mut self, entry: PartitionEntry) -> i64 {
        let mut key = entry.meta.min_ts;
        while let Some(existing) = self.partitions.get(&key) {
            if existing.dir_name == entry.dir_name || key == i64::MAX {
                break;
            }
            key += 1;
        }
        self.partitions.insert(key, entry);
        key
    }

    /// Persist the registry to a JSON file (atomic via write + rename).
    ///
    /// The write-then-rename pattern ensures crash safety:
    /// - Write to `{path}.tmp`
    /// - Rename `{path}.tmp` → `{path}` (atomic on most filesystems)
    ///   If crash during write: `.tmp` file is orphaned, original intact.
    ///   If crash during rename: atomic — either old or new version visible.
    pub fn persist(&self, path: &std::path::Path) -> crate::Result<()> {
        let entries = self.export();
        let json = sonic_rs::to_vec_pretty(&entries).map_err(|e| crate::Error::Serialization {
            format: "json".to_string(),
            detail: format!("serialize partition registry: {e}"),
        })?;

        let tmp_path = path.with_extension("tmp");
        nodedb_wal::segment::atomic_write_fsync(&tmp_path, path, &json).map_err(|e| {
            crate::Error::Storage {
                engine: "timeseries".to_string(),
                detail: format!("atomic write {}: {e}", path.display()),
            }
        })?;
        Ok(())
    }

    /// Recover registry from a persisted JSON file.
    ///
    /// Loads partition entries, filters out stale states:
    /// - `Merging` → rolled back to `Sealed` (incomplete merge on crash)
    /// - `Deleted` → removed (cleanup on recovery)
    pub fn recover(path: &std::path::Path, config: TieredPartitionConfig) -> crate::Result<Self> {
        let data = std::fs::read(path).map_err(|e| crate::Error::Storage {
            engine: "timeseries".to_string(),
            detail: format!("read {}: {e}", path.display()),
        })?;
        let entries: Vec<(i64, PartitionEntry)> =
            sonic_rs::from_slice(&data).map_err(|e| crate::Error::Serialization {
                format: "json".to_string(),
                detail: format!("parse {}: {e}", path.display()),
            })?;

        let mut registry = Self::new(config);

        for (start, mut entry) in entries {
            match entry.meta.state {
                PartitionState::Merging => {
                    // Incomplete merge — roll back to Sealed.
                    entry.meta.state = PartitionState::Sealed;
                }
                PartitionState::Deleted => {
                    // Skip deleted partitions (cleanup).
                    continue;
                }
                _ => {}
            }
            registry.partitions.insert(start, entry);
        }

        Ok(registry)
    }

    /// Clean up orphaned partition directories that have no manifest entry.
    ///
    /// Called on startup after `recover()`. Scans the timeseries data directory
    /// and removes directories that aren't in the registry (partial merge output).
    pub fn cleanup_orphans(&self, base_dir: &std::path::Path) -> Vec<String> {
        let mut removed = Vec::new();
        let known_dirs: std::collections::HashSet<&str> = self
            .partitions
            .values()
            .map(|e| e.dir_name.as_str())
            .collect();

        if let Ok(entries) = std::fs::read_dir(base_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && name.starts_with("ts-")
                    && !known_dirs.contains(name)
                {
                    if let Err(e) = std::fs::remove_dir_all(entry.path()) {
                        tracing::warn!(dir = name, error = %e, "failed to cleanup orphan partition");
                    } else {
                        removed.push(name.to_string());
                    }
                }
            }
        }
        removed
    }
}
