// SPDX-License-Identifier: BUSL-1.1

//! High-water-mark dedup for cross-shard event delivery.
//!
//! Tracks the highest contiguous processed LSN per source vShard.
//! Any event with `source_lsn <= hwm[source_vshard]` is a known duplicate
//! and is dropped. This is a single `u64` per sender — NOT per-event keys.
//! Survives restarts via redb persistence.

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::warn;

/// redb table: source_vshard_id (u16 as u64 key) → highest processed LSN.
const HWM_TABLE: TableDefinition<u64, u64> = TableDefinition::new("cross_shard_hwm");

/// Per-source-vShard high-water-mark store.
///
/// Thread-safe: updated by the cross-shard receiver (one per node),
/// read by dedup checks on incoming events.
pub struct HwmStore {
    /// In-memory cache: source_vshard → highest processed LSN.
    marks: RwLock<HashMap<u32, u64>>,
    /// Durable storage for crash recovery.
    db: Database,
}

impl HwmStore {
    /// Open or create the HWM store.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("cross_shard_hwm.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open hwm db {}: {e}", path.display()),
        })?;

        // Ensure table exists.
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            txn.open_table(HWM_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        // Load existing marks.
        let marks = Self::load_marks(&db)?;

        Ok(Self {
            marks: RwLock::new(marks),
            db,
        })
    }

    /// Check if an event is a duplicate (source_lsn <= hwm).
    /// Returns `true` if the event should be dropped.
    pub fn is_duplicate(&self, source_vshard: u32, source_lsn: u64) -> bool {
        let marks = self.marks.read().unwrap_or_else(|p| p.into_inner());
        marks
            .get(&source_vshard)
            .is_some_and(|&hwm| source_lsn <= hwm)
    }

    /// Advance the HWM for a source vShard after successful processing.
    /// Only advances forward (monotonic).
    pub fn advance(&self, source_vshard: u32, source_lsn: u64) {
        let mut marks = self.marks.write().unwrap_or_else(|p| p.into_inner());
        let current = marks.entry(source_vshard).or_insert(0);
        if source_lsn > *current {
            *current = source_lsn;
            // Persist to redb (best-effort — in-memory is authoritative until crash).
            if let Err(e) = self.persist(source_vshard, source_lsn) {
                warn!(
                    source_vshard,
                    source_lsn,
                    error = %e,
                    "failed to persist HWM"
                );
            }
        }
    }

    /// Get the current HWM for a source vShard.
    pub fn get(&self, source_vshard: u32) -> u64 {
        let marks = self.marks.read().unwrap_or_else(|p| p.into_inner());
        marks.get(&source_vshard).copied().unwrap_or(0)
    }

    /// Persist a single HWM to redb.
    fn persist(&self, source_vshard: u32, lsn: u64) -> crate::Result<()> {
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(HWM_TABLE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            table
                .insert(source_vshard as u64, lsn)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    /// Load all HWMs from redb on startup.
    fn load_marks(db: &Database) -> crate::Result<HashMap<u32, u64>> {
        let txn = db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_read: {e}"),
        })?;
        let table = txn
            .open_table(HWM_TABLE)
            .map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;

        let mut marks = HashMap::new();
        let iter = table.iter().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("iter: {e}"),
        })?;
        for entry in iter {
            let guard = entry.map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("entry: {e}"),
            })?;
            let vshard = guard.0.value() as u32;
            let lsn = guard.1.value();
            marks.insert(vshard, lsn);
        }
        Ok(marks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = HwmStore::open(dir.path()).unwrap();

        assert!(!store.is_duplicate(3, 100));
        store.advance(3, 100);
        assert!(store.is_duplicate(3, 100));
        assert!(store.is_duplicate(3, 50));
        assert!(!store.is_duplicate(3, 101));
    }

    #[test]
    fn monotonic_advance() {
        let dir = tempfile::tempdir().unwrap();
        let store = HwmStore::open(dir.path()).unwrap();

        store.advance(1, 200);
        store.advance(1, 150); // Should not regress.
        assert_eq!(store.get(1), 200);
        assert!(store.is_duplicate(1, 150));
    }

    #[test]
    fn per_vshard_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let store = HwmStore::open(dir.path()).unwrap();

        store.advance(1, 100);
        store.advance(2, 200);

        assert!(store.is_duplicate(1, 100));
        assert!(!store.is_duplicate(1, 200));
        assert!(store.is_duplicate(2, 200));
        assert!(!store.is_duplicate(2, 300));
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = HwmStore::open(dir.path()).unwrap();
            store.advance(5, 500);
            store.advance(10, 1000);
        }
        // Reopen.
        let store = HwmStore::open(dir.path()).unwrap();
        assert_eq!(store.get(5), 500);
        assert_eq!(store.get(10), 1000);
        assert!(store.is_duplicate(5, 500));
    }

    #[test]
    fn unknown_vshard_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let store = HwmStore::open(dir.path()).unwrap();
        assert_eq!(store.get(99), 0);
        assert!(!store.is_duplicate(99, 1));
    }
}
