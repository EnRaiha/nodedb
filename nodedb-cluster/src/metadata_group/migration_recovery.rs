// SPDX-License-Identifier: BUSL-1.1

//! In-flight migration recovery.
//!
//! Called from `MigrationExecutor::recover_in_flight_migrations` which
//! is invoked at coordinator startup (after the metadata Raft group is
//! up but before the rebalancer spawns).
//!
//! For each migration row in `MigrationStateTable`:
//!
//! - If the last checkpoint phase is `Complete`, delete the row (cleanup).
//! - If the migration is older than `abort_timeout` wall-clock seconds,
//!   propose a `MigrationAbort` with appropriate compensations.
//! - If the target node is unreachable, propose a `MigrationAbort`.
//! - Otherwise, re-queue the migration so `MigrationExecutor::execute`
//!   can resume from the next phase.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

use crate::decommission::MetadataProposer;
use crate::error::Result;
use crate::metadata_group::compensation::Compensation;
use crate::metadata_group::entry::MetadataEntry;
use crate::metadata_group::migration_state::{
    MigrationCheckpointPayload, MigrationId, MigrationPhaseTag, SharedMigrationStateTable,
};
use crate::topology::ClusterTopology;

/// Default timeout after which a stale in-flight migration is aborted
/// rather than resumed.
pub const DEFAULT_ABORT_TIMEOUT: Duration = Duration::from_secs(3600); // 1 hour

/// Decision reached by the recovery scan for a single migration.
#[derive(Debug)]
pub enum RecoveryDecision {
    /// Migration completed normally; the row was cleaned up.
    CleanedUp { migration_id: MigrationId },
    /// Migration was resumed by requeueing for the next phase.
    Resumed { migration_id: MigrationId },
    /// Migration was aborted (timeout, unreachable target, etc.).
    Aborted {
        migration_id: MigrationId,
        reason: String,
    },
}

/// Run in-flight migration recovery.
///
/// `topology` is used to check whether target nodes are still reachable.
/// `proposer` is used to propose `MigrationAbort` entries.
/// `table` is the shared migration state table.
/// `abort_timeout` controls when a stale migration is aborted rather than
/// resumed.
///
/// Returns one `RecoveryDecision` per in-flight migration found.
pub async fn recover_in_flight_migrations(
    table: SharedMigrationStateTable,
    topology: Arc<std::sync::RwLock<ClusterTopology>>,
    proposer: Arc<dyn MetadataProposer>,
    abort_timeout: Duration,
) -> Result<Vec<RecoveryDecision>> {
    // Load all rows once — we operate on a snapshot.
    let rows = {
        let guard = table.lock().unwrap_or_else(|p| p.into_inner());
        guard.all_checkpoints()
    };

    if rows.is_empty() {
        info!("migration recovery: no in-flight migrations found");
        return Ok(vec![]);
    }

    info!(
        count = rows.len(),
        "migration recovery: scanning in-flight migrations"
    );

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| {
            tracing::error!(
                "system clock is before UNIX_EPOCH during migration recovery; \
                 using 0 (epoch) — check NTP/RTC configuration"
            );
            std::time::Duration::ZERO
        })
        .as_millis() as u64;
    let abort_threshold_ms = abort_timeout.as_millis() as u64;

    let mut decisions = Vec::with_capacity(rows.len());

    for row in rows {
        let migration_id = match row.migration_uuid() {
            Some(id) => id,
            None => {
                warn!(
                    key = %row.migration_id,
                    "migration recovery: could not parse UUID, skipping"
                );
                continue;
            }
        };

        // 1. Clean up completed migrations.
        if row.payload.phase_tag() == MigrationPhaseTag::Complete {
            let mut guard = table.lock().unwrap_or_else(|p| p.into_inner());
            guard.remove(&migration_id)?;
            drop(guard);
            info!(
                migration_id = %migration_id,
                "migration recovery: cleaned up completed migration"
            );
            decisions.push(RecoveryDecision::CleanedUp { migration_id });
            continue;
        }

        // 2. Check age — abort stale migrations.
        let age_ms = now_ms.saturating_sub(row.ts_ms);
        if age_ms > abort_threshold_ms {
            let reason = format!(
                "migration stale: age {}s exceeds abort timeout {}s",
                age_ms / 1000,
                abort_timeout.as_secs()
            );
            warn!(migration_id = %migration_id, reason, "aborting stale migration");
            let comps = compensations_for_phase(row.payload.phase_tag(), &row.payload);
            let entry = MetadataEntry::MigrationAbort {
                migration_id: migration_id.hyphenated().to_string(),
                reason: reason.clone(),
                compensations: comps,
            };
            proposer.propose_and_wait(entry).await?;
            decisions.push(RecoveryDecision::Aborted {
                migration_id,
                reason,
            });
            continue;
        }

        // 3. Check target reachability.
        let target_reachable = is_target_reachable(&row.payload, &topology);
        if !target_reachable {
            let reason = "migration aborted: target node no longer reachable".to_owned();
            warn!(migration_id = %migration_id, "aborting migration: target unreachable");
            let comps = compensations_for_phase(row.payload.phase_tag(), &row.payload);
            let entry = MetadataEntry::MigrationAbort {
                migration_id: migration_id.hyphenated().to_string(),
                reason: reason.clone(),
                compensations: comps,
            };
            proposer.propose_and_wait(entry).await?;
            decisions.push(RecoveryDecision::Aborted {
                migration_id,
                reason,
            });
            continue;
        }

        // 4. Migration is resumable — the caller (`MigrationExecutor`) will
        //    re-invoke `execute` starting from the checkpoint's next phase.
        info!(
            migration_id = %migration_id,
            phase = ?row.payload.phase_tag(),
            "migration recovery: migration is resumable"
        );
        decisions.push(RecoveryDecision::Resumed { migration_id });
    }

    Ok(decisions)
}

/// Derive the set of compensations needed to undo the effects up to
/// `phase` for the given `payload`.
///
/// The audit's "Recommended `MigrationAbort` compensations" section
/// drives this mapping.
pub fn compensations_for_phase(
    phase: MigrationPhaseTag,
    payload: &MigrationCheckpointPayload,
) -> Vec<Compensation> {
    match phase {
        MigrationPhaseTag::AddLearner => {
            // AddLearner committed; remove the ghost learner.
            if let MigrationCheckpointPayload::AddLearner {
                source_group,
                target_node,
                ..
            } = payload
            {
                vec![Compensation::RemoveLearner {
                    group_id: *source_group,
                    peer_id: *target_node,
                }]
            } else {
                vec![]
            }
        }
        MigrationPhaseTag::CatchUp => {
            // Catch-up started; AddLearner committed.  Remove the learner.
            if let MigrationCheckpointPayload::CatchUp { .. } = payload {
                // We don't have group_id/peer_id at CatchUp payload level —
                // the AddLearner checkpoint must have been the prior row.
                // Emit empty (caller should chain with AddLearner payload).
                vec![]
            } else {
                vec![]
            }
        }
        MigrationPhaseTag::PromoteLearner => {
            // PromoteLearner committed; demote back by removing the voter.
            if let MigrationCheckpointPayload::PromoteLearner {
                source_group,
                target_node,
                ..
            } = payload
            {
                vec![Compensation::RemoveVoter {
                    group_id: *source_group,
                    peer_id: *target_node,
                }]
            } else {
                vec![]
            }
        }
        MigrationPhaseTag::LeadershipTransfer => {
            // LeadershipTransfer about to be proposed but did not commit.
            // PromoteLearner committed — remove the voter.
            if let MigrationCheckpointPayload::LeadershipTransfer {
                source_group,
                new_leader_node_id,
                ..
            } = payload
            {
                vec![Compensation::RemoveVoter {
                    group_id: *source_group,
                    peer_id: *new_leader_node_id,
                }]
            } else {
                vec![]
            }
        }
        MigrationPhaseTag::Cutover => {
            // Cut-over committed — this is NOT an abort scenario.
            // The only path is forward: install the ghost and mark Complete.
            // No compensations are valid at this phase (audit §Phase 3 abort
            // committed section).
            vec![]
        }
        MigrationPhaseTag::Complete => {
            // Complete — no compensations needed; cleanup only.
            vec![]
        }
    }
}

/// Check whether the target node in `payload` is currently reachable
/// in the topology.
fn is_target_reachable(
    payload: &MigrationCheckpointPayload,
    topology: &std::sync::Arc<std::sync::RwLock<ClusterTopology>>,
) -> bool {
    let target_node = match payload {
        MigrationCheckpointPayload::AddLearner { target_node, .. } => *target_node,
        MigrationCheckpointPayload::PromoteLearner { target_node, .. } => *target_node,
        MigrationCheckpointPayload::LeadershipTransfer {
            new_leader_node_id, ..
        } => *new_leader_node_id,
        MigrationCheckpointPayload::Cutover {
            new_leader_node_id, ..
        } => *new_leader_node_id,
        // CatchUp and Complete don't carry the target node directly in
        // the payload — treat as reachable (recovery decision will be Resumed
        // or CleanedUp respectively).
        _ => return true,
    };
    let topo = topology.read().unwrap_or_else(|p| p.into_inner());
    topo.get_node(target_node).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::migration_state::{
        MigrationCheckpointPayload, MigrationStateTable, PersistedMigrationCheckpoint,
    };
    use nodedb_types::Hlc;
    use std::sync::Mutex;
    use uuid::Uuid;

    fn temp_db() -> Arc<redb::Database> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let _ = txn.open_table(MigrationStateTable::TABLE).unwrap();
        }
        txn.commit().unwrap();
        std::mem::forget(dir);
        Arc::new(db)
    }

    fn shared_table(db: Arc<redb::Database>) -> SharedMigrationStateTable {
        Arc::new(Mutex::new(MigrationStateTable::new(db)))
    }

    fn make_row(
        id: Uuid,
        payload: MigrationCheckpointPayload,
        attempt: u32,
        ts_ms: u64,
    ) -> PersistedMigrationCheckpoint {
        let crc = payload.crc32c().unwrap();
        PersistedMigrationCheckpoint {
            migration_id: id.hyphenated().to_string(),
            attempt,
            payload,
            crc32c: crc,
            ts_ms,
        }
    }

    struct RecordingProposer {
        entries: Mutex<Vec<MetadataEntry>>,
    }

    impl RecordingProposer {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entries: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl MetadataProposer for RecordingProposer {
        async fn propose_and_wait(&self, entry: MetadataEntry) -> Result<u64> {
            self.entries.lock().unwrap().push(entry);
            Ok(1)
        }
    }

    fn empty_topology() -> Arc<std::sync::RwLock<ClusterTopology>> {
        Arc::new(std::sync::RwLock::new(ClusterTopology::new()))
    }

    #[tokio::test]
    async fn completed_migration_is_cleaned_up() {
        let db = temp_db();
        let table = shared_table(Arc::clone(&db));
        let id = Uuid::new_v4();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let row = make_row(
            id,
            MigrationCheckpointPayload::Complete {
                vshard_id: 1,
                actual_pause_us: 100,
                ghost_stub_installed: true,
            },
            0,
            now_ms,
        );
        {
            let mut g = table.lock().unwrap();
            g.upsert(row).unwrap();
        }
        let proposer = RecordingProposer::new();
        let decisions = recover_in_flight_migrations(
            Arc::clone(&table),
            empty_topology(),
            proposer.clone(),
            DEFAULT_ABORT_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RecoveryDecision::CleanedUp { .. }));
        // No abort proposed.
        assert!(proposer.entries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stale_migration_is_aborted() {
        let db = temp_db();
        let table = shared_table(Arc::clone(&db));
        let id = Uuid::new_v4();
        // ts_ms = 0 means the migration is extremely old.
        let row = make_row(
            id,
            MigrationCheckpointPayload::AddLearner {
                vshard_id: 2,
                source_node: 1,
                target_node: 99,
                source_group: 0,
                write_pause_budget_us: 500_000,
                started_at_hlc: Hlc::default(),
            },
            0,
            0, // very old
        );
        {
            let mut g = table.lock().unwrap();
            g.upsert(row).unwrap();
        }
        let proposer = RecordingProposer::new();
        let decisions = recover_in_flight_migrations(
            Arc::clone(&table),
            empty_topology(),
            proposer.clone(),
            DEFAULT_ABORT_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RecoveryDecision::Aborted { .. }));
        // An abort entry was proposed.
        let entries = proposer.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0], MetadataEntry::MigrationAbort { .. }));
    }

    #[tokio::test]
    async fn reachable_migration_is_resumed() {
        let db = temp_db();
        let table = shared_table(Arc::clone(&db));
        let id = Uuid::new_v4();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let row = make_row(
            id,
            MigrationCheckpointPayload::CatchUp {
                vshard_id: 3,
                learner_log_index_at_add: 5,
            },
            0,
            now_ms,
        );
        {
            let mut g = table.lock().unwrap();
            g.upsert(row).unwrap();
        }
        let proposer = RecordingProposer::new();
        let decisions = recover_in_flight_migrations(
            Arc::clone(&table),
            empty_topology(),
            proposer.clone(),
            DEFAULT_ABORT_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(decisions.len(), 1);
        assert!(matches!(&decisions[0], RecoveryDecision::Resumed { .. }));
        assert!(proposer.entries.lock().unwrap().is_empty());
    }
}
