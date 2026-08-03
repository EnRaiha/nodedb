// SPDX-License-Identifier: BUSL-1.1

//! Streaming MV state persistence: periodic flush to redb + restore on startup.
//!
//! MvState is primarily in-memory for O(1) access. This module persists
//! snapshots to redb periodically (every 30s) so state survives restarts.
//! On startup, the registry restores state from the latest snapshot.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use redb::{Database, ReadableDatabase, TableDefinition};
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use super::registry::MvRegistry;
use super::state::GroupState;
use crate::types::DatabaseId;

/// redb table: "v2:{database_id}:{tenant_id}:{mv_name}" → MessagePack-serialized MvSnapshot.
const MV_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("mv_state");

/// Serialized MV state: Vec of (group_key, per-aggregate GroupState list).
pub type MvSnapshot = Vec<(String, Vec<GroupState>)>;

/// How often to persist MV state to redb.
const PERSIST_INTERVAL: Duration = Duration::from_secs(30);

fn state_key(database_id: DatabaseId, tenant_id: u64, mv_name: &str) -> String {
    format!("v2:{}:{tenant_id}:{mv_name}", database_id.as_u64())
}

/// Manages persistence of streaming MV state.
pub struct MvPersistence {
    db: Database,
}

impl MvPersistence {
    /// Open or create the MV state store.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("mv_state.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open mv_state db {}: {e}", path.display()),
        })?;

        // Ensure table exists.
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            txn.open_table(MV_STATE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        Ok(Self { db })
    }

    /// Persist a single MV's state snapshot.
    pub fn save(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        mv_name: &str,
        snapshot: &[(String, Vec<GroupState>)],
    ) -> crate::Result<()> {
        let key = state_key(database_id, tenant_id, mv_name);
        let bytes = zerompk::to_msgpack_vec(&snapshot.to_vec()).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("mv_state: {e}"),
            }
        })?;

        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(MV_STATE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            table
                .insert(key.as_str(), bytes.as_slice())
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

    /// Load a persisted MV state snapshot.
    pub fn load(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        mv_name: &str,
    ) -> crate::Result<Option<MvSnapshot>> {
        let key = state_key(database_id, tenant_id, mv_name);
        let txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_read: {e}"),
        })?;
        let table = txn
            .open_table(MV_STATE)
            .map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;

        match table.get(key.as_str()) {
            Ok(Some(guard)) => {
                let bytes: &[u8] = guard.value();
                let snapshot: MvSnapshot =
                    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("mv_state restore: {e}"),
                    })?;
                Ok(Some(snapshot))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("get mv_state: {e}"),
            }),
        }
    }

    /// Delete persisted state for a dropped MV.
    pub fn delete(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        mv_name: &str,
    ) -> crate::Result<()> {
        let key = state_key(database_id, tenant_id, mv_name);
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(MV_STATE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            let _ = table.remove(key.as_str());
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    /// Flush all MV states from the registry to redb.
    pub fn flush_all(&self, registry: &MvRegistry) {
        for mv_def in registry.list_all() {
            if let Some(state) =
                registry.get_state(mv_def.database_id, mv_def.tenant_id, &mv_def.name)
            {
                let snapshot = state.snapshot();
                if !snapshot.is_empty()
                    && let Err(e) = self.save(
                        mv_def.database_id,
                        mv_def.tenant_id,
                        &mv_def.name,
                        &snapshot,
                    )
                {
                    warn!(
                        mv = %mv_def.name,
                        error = %e,
                        "failed to persist MV state"
                    );
                }
            }
        }
    }

    /// Restore all MV states from redb into the registry.
    pub fn restore_all(&self, registry: &MvRegistry) {
        let mut restored = 0u32;
        for mv_def in registry.list_all() {
            match self.load(mv_def.database_id, mv_def.tenant_id, &mv_def.name) {
                Ok(Some(snapshot)) if !snapshot.is_empty() => {
                    if let Some(state) =
                        registry.get_state(mv_def.database_id, mv_def.tenant_id, &mv_def.name)
                    {
                        state.restore(snapshot);
                        restored += 1;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(mv = %mv_def.name, error = %e, "failed to restore MV state");
                }
            }
        }
        if restored > 0 {
            info!(restored, "restored streaming MV states from redb");
        }
    }
}

/// Spawn the background persistence task.
pub fn spawn_persist_task(
    persistence: Arc<MvPersistence>,
    registry: Arc<MvRegistry>,
    watermark_tracker: Arc<crate::event::watermark_tracker::WatermarkTracker>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!("MV persistence task started");
        loop {
            tokio::select! {
                _ = tokio::time::sleep(PERSIST_INTERVAL) => {
                    // Finalize time buckets using the global watermark event_time.
                    // This is min(per_partition_event_times) — the wall-clock time
                    // below which ALL partitions have advanced. Groups with
                    // latest_event_time < this value will receive no more events.
                    let cutoff = watermark_tracker.global_watermark_event_time();

                    if cutoff > 0 {
                        let mut total_finalized = 0u32;
                        for mv_def in registry.list_all() {
                            if let Some(state) = registry.get_state(mv_def.database_id, mv_def.tenant_id, &mv_def.name) {
                                total_finalized += state.finalize_buckets(cutoff);
                            }
                        }
                        if total_finalized > 0 {
                            debug!(
                                finalized = total_finalized,
                                cutoff_ms = cutoff,
                                "MV time buckets finalized via global watermark event_time"
                            );
                        }
                    }

                    // Persist state to redb.
                    persistence.flush_all(&registry);
                    trace!("MV state flushed to redb");
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        persistence.flush_all(&registry);
                        debug!("MV persistence task: final flush on shutdown");
                        return;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let persist = MvPersistence::open(dir.path()).unwrap();

        let snapshot = vec![
            (
                "INSERT".to_string(),
                vec![GroupState {
                    count: 5,
                    sum: 100.0,
                    min: Some(10.0),
                    max: Some(50.0),
                    finalized: false,
                    latest_event_time: 0,
                }],
            ),
            (
                "UPDATE".to_string(),
                vec![GroupState {
                    count: 3,
                    sum: 30.0,
                    min: Some(5.0),
                    max: Some(15.0),
                    finalized: false,
                    latest_event_time: 0,
                }],
            ),
        ];

        persist
            .save(DatabaseId::new(1), 1, "order_stats", &snapshot)
            .unwrap();

        let loaded = persist
            .load(DatabaseId::new(1), 1, "order_stats")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].0, "INSERT");
        assert_eq!(loaded[0].1[0].count, 5);
    }

    #[test]
    fn load_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let persist = MvPersistence::open(dir.path()).unwrap();
        assert!(
            persist
                .load(DatabaseId::new(1), 1, "nonexistent")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn delete_removes_state() {
        let dir = tempfile::tempdir().unwrap();
        let persist = MvPersistence::open(dir.path()).unwrap();

        let snapshot = vec![("k".to_string(), vec![GroupState::default()])];
        persist
            .save(DatabaseId::new(1), 1, "mv1", &snapshot)
            .unwrap();
        persist.delete(DatabaseId::new(1), 1, "mv1").unwrap();
        assert!(
            persist
                .load(DatabaseId::new(1), 1, "mv1")
                .unwrap()
                .is_none()
        );
    }
}
