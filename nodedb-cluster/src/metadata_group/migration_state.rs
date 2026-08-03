// SPDX-License-Identifier: BUSL-1.1

//! Durable migration state: `MigrationStateTable`, `MigrationId`,
//! `MigrationCheckpointPayload`, and `MigrationPhaseTag`.
//!
//! `MigrationStateTable` is an `Arc<Mutex<_>>`-backed redb table
//! (keyed by `MigrationId` as a hyphenated UUID string) that stores
//! the latest `PersistedMigrationCheckpoint` for each in-flight
//! migration.  Rows are upserted on every committed `MigrationCheckpoint`
//! entry and deleted when a committed `MigrationAbort` has applied all
//! compensations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use redb::ReadableDatabase;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{ClusterError, MigrationCheckpointError, Result};
use nodedb_types::Hlc;

// ── Public type aliases ──────────────────────────────────────────────────────

/// Unique identifier for a single migration run.  A new UUID is
/// generated for every `MigrationExecutor::execute` call so that a
/// resumed migration can be distinguished from a completely new one
/// targeting the same vShard.
pub type MigrationId = Uuid;

// ── Phase tag ────────────────────────────────────────────────────────────────

/// Discriminant-only label for the current phase of a migration.
///
/// Stored inside every `MigrationCheckpointPayload` so the recovery
/// path can route without pattern-matching the full payload.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MigrationPhaseTag {
    AddLearner,
    CatchUp,
    PromoteLearner,
    LeadershipTransfer,
    Cutover,
    Complete,
}

// ── Per-phase checkpoint payloads ────────────────────────────────────────────

/// Payload written to the metadata Raft group at each phase boundary.
///
/// Each variant carries enough information to resume the migration from
/// scratch if the coordinator crashes between two phases.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MigrationCheckpointPayload {
    /// Written just before `propose_conf_change(AddLearner)`.
    AddLearner {
        vshard_id: u32,
        source_node: u64,
        target_node: u64,
        source_group: u64,
        write_pause_budget_us: u64,
        started_at_hlc: Hlc,
    },
    /// Written just after `AddLearner` conf-change commits.
    CatchUp {
        vshard_id: u32,
        /// Log index at which `AddLearner` committed.  The resuming
        /// coordinator uses this to skip re-proposing `AddLearner` if
        /// the learner is already present in the group.
        learner_log_index_at_add: u64,
    },
    /// Written just after `PromoteLearner` conf-change commits.
    PromoteLearner {
        vshard_id: u32,
        target_node: u64,
        source_group: u64,
    },
    /// Written just before `propose_and_wait(LeadershipTransfer)`.
    LeadershipTransfer {
        vshard_id: u32,
        /// Whether `PromoteLearner` has already committed.
        target_is_voter: bool,
        new_leader_node_id: u64,
        source_group: u64,
    },
    /// Written after `propose_and_wait(LeadershipTransfer)` returns
    /// success (cut-over committed).
    Cutover {
        vshard_id: u32,
        new_leader_node_id: u64,
        source_group: u64,
    },
    /// Written after the ghost stub is installed and `save_ghosts` succeeds.
    Complete {
        vshard_id: u32,
        actual_pause_us: u64,
        ghost_stub_installed: bool,
    },
}

impl MigrationCheckpointPayload {
    /// Returns the phase tag matching this payload variant.
    pub fn phase_tag(&self) -> MigrationPhaseTag {
        match self {
            Self::AddLearner { .. } => MigrationPhaseTag::AddLearner,
            Self::CatchUp { .. } => MigrationPhaseTag::CatchUp,
            Self::PromoteLearner { .. } => MigrationPhaseTag::PromoteLearner,
            Self::LeadershipTransfer { .. } => MigrationPhaseTag::LeadershipTransfer,
            Self::Cutover { .. } => MigrationPhaseTag::Cutover,
            Self::Complete { .. } => MigrationPhaseTag::Complete,
        }
    }

    /// Encode as zerompk bytes for CRC32C computation.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| {
            ClusterError::MigrationCheckpoint(MigrationCheckpointError::Codec {
                detail: format!("payload encode: {e}"),
            })
        })
    }

    /// Compute the CRC32C of the encoded payload bytes.
    pub fn crc32c(&self) -> Result<u32> {
        let bytes = self.to_bytes()?;
        Ok(crc32c::crc32c(&bytes))
    }
}

// ── Persisted checkpoint row ─────────────────────────────────────────────────

/// The value stored in the `MIGRATION_STATE_TABLE` redb table.
///
/// One row per `MigrationId`; upserted on every committed
/// `MigrationCheckpoint` and deleted when `MigrationAbort` applies.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PersistedMigrationCheckpoint {
    pub migration_id: String, // UUID as hyphenated string
    pub attempt: u32,
    pub payload: MigrationCheckpointPayload,
    pub crc32c: u32,
    /// Wall-clock milliseconds at proposal time (non-authoritative —
    /// used only for timeout-based abort decisions during recovery).
    pub ts_ms: u64,
}

impl PersistedMigrationCheckpoint {
    pub fn migration_uuid(&self) -> Option<MigrationId> {
        self.migration_id.parse().ok()
    }
}

// ── MigrationStateTable ──────────────────────────────────────────────────────

/// In-memory mirror of `_cluster.migration_state`.
///
/// Mutations go through `MigrationStateTable` methods which update both
/// the in-memory `HashMap` and the backing redb table atomically.
/// The `Arc<Mutex<_>>` wrapper allows the table to be shared across the
/// `CacheApplier` and `MigrationExecutor` without a lifetime tie-in.
///
/// `ClusterCatalog` is held as `Arc` to survive actor hand-offs.
pub struct MigrationStateTable {
    /// In-memory view keyed by UUID string (matches redb key format).
    mem: HashMap<String, PersistedMigrationCheckpoint>,
    db: Arc<redb::Database>,
}

impl MigrationStateTable {
    pub const TABLE: redb::TableDefinition<'static, &'static str, &'static [u8]> =
        redb::TableDefinition::new("_cluster.migration_state");

    /// Create a new, empty in-memory table backed by `db`.
    pub fn new(db: Arc<redb::Database>) -> Self {
        Self {
            mem: HashMap::new(),
            db,
        }
    }

    /// Load all persisted checkpoints from redb into the in-memory map.
    ///
    /// Called once at startup before recovery runs.
    pub fn load_all(&mut self) -> Result<()> {
        let txn = self.db.begin_read().map_err(|e| ClusterError::Storage {
            detail: format!("migration_state begin_read: {e}"),
        })?;
        let table = txn
            .open_table(Self::TABLE)
            .map_err(|e| ClusterError::Storage {
                detail: format!("migration_state open_table: {e}"),
            })?;
        let range = table.range::<&str>(..).map_err(|e| ClusterError::Storage {
            detail: format!("migration_state range: {e}"),
        })?;
        for entry in range {
            let (key, value) = entry.map_err(|e| ClusterError::Storage {
                detail: format!("migration_state iter: {e}"),
            })?;
            let key_str = key.value().to_owned();
            match zerompk::from_msgpack::<PersistedMigrationCheckpoint>(value.value()) {
                Ok(row) => {
                    self.mem.insert(key_str, row);
                }
                Err(e) => {
                    tracing::warn!(key = %key_str, error = %e, "migration_state: corrupt row skipped");
                }
            }
        }
        Ok(())
    }

    /// Upsert a checkpoint row (in-memory + redb).
    ///
    /// Idempotent: if the exact `(migration_id, phase, attempt)` tuple
    /// is already stored, this is a no-op.
    pub fn upsert(&mut self, row: PersistedMigrationCheckpoint) -> Result<()> {
        let key = row.migration_id.clone();
        // Idempotency: skip if identical (migration_id, phase_tag, attempt) already stored.
        if let Some(existing) = self.mem.get(&key)
            && existing.payload.phase_tag() == row.payload.phase_tag()
            && existing.attempt == row.attempt
        {
            return Ok(());
        }
        // Encode and persist.
        let bytes = zerompk::to_msgpack_vec(&row).map_err(|e| ClusterError::Codec {
            detail: format!("migration_state encode: {e}"),
        })?;
        let txn = self.db.begin_write().map_err(|e| ClusterError::Storage {
            detail: format!("migration_state begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(Self::TABLE)
                .map_err(|e| ClusterError::Storage {
                    detail: format!("migration_state open_table: {e}"),
                })?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| ClusterError::Storage {
                    detail: format!("migration_state insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| ClusterError::Storage {
            detail: format!("migration_state commit: {e}"),
        })?;
        self.mem.insert(key, row);
        Ok(())
    }

    /// Remove a checkpoint row (in-memory + redb).  No-op if not present.
    pub fn remove(&mut self, migration_id: &MigrationId) -> Result<()> {
        let key = migration_id.hyphenated().to_string();
        self.mem.remove(&key);
        let txn = self.db.begin_write().map_err(|e| ClusterError::Storage {
            detail: format!("migration_state begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(Self::TABLE)
                .map_err(|e| ClusterError::Storage {
                    detail: format!("migration_state open_table: {e}"),
                })?;
            let _ = table
                .remove(key.as_str())
                .map_err(|e| ClusterError::Storage {
                    detail: format!("migration_state remove: {e}"),
                })?;
        }
        txn.commit().map_err(|e| ClusterError::Storage {
            detail: format!("migration_state commit: {e}"),
        })?;
        Ok(())
    }

    /// Get a snapshot of all in-flight checkpoints (in-memory view).
    pub fn all_checkpoints(&self) -> Vec<PersistedMigrationCheckpoint> {
        self.mem.values().cloned().collect()
    }

    /// Lookup the latest checkpoint for a migration ID.
    pub fn get(&self, migration_id: &MigrationId) -> Option<&PersistedMigrationCheckpoint> {
        self.mem.get(&migration_id.hyphenated().to_string())
    }
}

// ── Shared handle ────────────────────────────────────────────────────────────

/// `Arc<Mutex<MigrationStateTable>>` — the type threaded through
/// `MetadataCache`, `CacheApplier`, and `MigrationExecutor`.
pub type SharedMigrationStateTable = Arc<Mutex<MigrationStateTable>>;

/// Construct a shared migration state table backed by `db`.
pub fn new_shared(db: Arc<redb::Database>) -> SharedMigrationStateTable {
    Arc::new(Mutex::new(MigrationStateTable::new(db)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> Arc<redb::Database> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");
        let db = redb::Database::create(&path).unwrap();
        // Create the table.
        let txn = db.begin_write().unwrap();
        {
            let _ = txn.open_table(MigrationStateTable::TABLE).unwrap();
        }
        txn.commit().unwrap();
        // Box keeps the TempDir alive — we leak it intentionally for tests.
        std::mem::forget(dir);
        Arc::new(db)
    }

    fn make_row(
        id: MigrationId,
        phase: MigrationCheckpointPayload,
        attempt: u32,
    ) -> PersistedMigrationCheckpoint {
        let crc = phase.crc32c().unwrap();
        PersistedMigrationCheckpoint {
            migration_id: id.hyphenated().to_string(),
            attempt,
            payload: phase,
            crc32c: crc,
            ts_ms: 0,
        }
    }

    #[test]
    fn upsert_and_load_roundtrip() {
        let db = temp_db();
        let mut table = MigrationStateTable::new(Arc::clone(&db));

        let id = Uuid::new_v4();
        let payload = MigrationCheckpointPayload::AddLearner {
            vshard_id: 5,
            source_node: 1,
            target_node: 2,
            source_group: 0,
            write_pause_budget_us: 500_000,
            started_at_hlc: Hlc::default(),
        };
        let row = make_row(id, payload.clone(), 0);
        table.upsert(row).unwrap();

        // Reload from redb.
        let mut table2 = MigrationStateTable::new(Arc::clone(&db));
        table2.load_all().unwrap();
        let loaded = table2.get(&id).unwrap();
        assert_eq!(loaded.payload, payload);
        assert_eq!(loaded.attempt, 0);
    }

    #[test]
    fn idempotent_upsert_same_phase_attempt() {
        let db = temp_db();
        let mut table = MigrationStateTable::new(Arc::clone(&db));

        let id = Uuid::new_v4();
        let payload = MigrationCheckpointPayload::CatchUp {
            vshard_id: 3,
            learner_log_index_at_add: 10,
        };
        let row = make_row(id, payload, 0);
        table.upsert(row.clone()).unwrap();
        table.upsert(row).unwrap(); // second upsert — must be no-op
        assert_eq!(table.all_checkpoints().len(), 1);
    }

    #[test]
    fn remove_deletes_from_redb() {
        let db = temp_db();
        let mut table = MigrationStateTable::new(Arc::clone(&db));

        let id = Uuid::new_v4();
        let payload = MigrationCheckpointPayload::Complete {
            vshard_id: 7,
            actual_pause_us: 100,
            ghost_stub_installed: true,
        };
        table.upsert(make_row(id, payload, 1)).unwrap();
        table.remove(&id).unwrap();

        let mut table2 = MigrationStateTable::new(Arc::clone(&db));
        table2.load_all().unwrap();
        assert!(table2.get(&id).is_none());
    }

    #[test]
    fn payload_crc32c_detects_corruption() {
        let payload = MigrationCheckpointPayload::CatchUp {
            vshard_id: 9,
            learner_log_index_at_add: 42,
        };
        let mut bytes = payload.to_bytes().unwrap();
        // Flip a byte.
        bytes[0] ^= 0xFF;
        // The CRC of the original should differ from the CRC of the mutated bytes.
        let original_crc = payload.crc32c().unwrap();
        let corrupted_crc = crc32c::crc32c(&bytes);
        assert_ne!(original_crc, corrupted_crc);
    }

    #[test]
    fn zerompk_payload_roundtrip() {
        let payload = MigrationCheckpointPayload::LeadershipTransfer {
            vshard_id: 11,
            target_is_voter: true,
            new_leader_node_id: 7,
            source_group: 2,
        };
        let bytes = zerompk::to_msgpack_vec(&payload).unwrap();
        let decoded: MigrationCheckpointPayload = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(payload, decoded);
    }
}
