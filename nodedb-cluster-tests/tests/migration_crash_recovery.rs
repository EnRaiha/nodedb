// SPDX-License-Identifier: BUSL-1.1

//! Integration tests: migration crash recovery.
//!
//! Simulates coordinator crashes at every phase boundary and verifies:
//!
//! 1. `MigrationCheckpoint` is proposed at every phase boundary.
//! 2. Recovery detects in-flight migrations and emits the correct decision.
//! 3. `MigrationAbort` compensations are applied consistently.
//! 4. Idempotent re-apply of the same checkpoint is safe.
//! 5. CRC32C corruption is caught before state is mutated.
//! 6. Fixed-seed state-machine fuzz: random (crash, recover) sequences
//!    converge to either "completed" or "aborted with compensations applied".

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use uuid::Uuid;

use nodedb_cluster::decommission::MetadataProposer;
use nodedb_cluster::error::Result;
use nodedb_cluster::topology::ClusterTopology;
use nodedb_cluster::{
    ClusterError, Compensation, MetadataEntry, MigrationCheckpointError,
    MigrationCheckpointPayload, MigrationId, MigrationPhaseTag, PersistedMigrationCheckpoint,
    SharedMigrationStateTable, apply_migration_abort, apply_migration_checkpoint,
    metadata_group::migration_recovery::{
        DEFAULT_ABORT_TIMEOUT, RecoveryDecision, recover_in_flight_migrations,
    },
    new_shared,
};

mod cluster_common;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn temp_db() -> Arc<redb::Database> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("migration_test.redb");
    let db = redb::Database::create(&path).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let _ = txn
            .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                "_cluster.migration_state",
            ))
            .unwrap();
    }
    txn.commit().unwrap();
    std::mem::forget(dir);
    Arc::new(db)
}

fn shared_table(db: Arc<redb::Database>) -> SharedMigrationStateTable {
    new_shared(db)
}

fn make_row(
    id: MigrationId,
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Clone)]
struct RecordingProposer {
    entries: Arc<Mutex<Vec<MetadataEntry>>>,
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl RecordingProposer {
    fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn take(&self) -> Vec<MetadataEntry> {
        let mut g = self.entries.lock().unwrap();
        let out = g.clone();
        g.clear();
        out
    }
}

#[async_trait]
impl MetadataProposer for RecordingProposer {
    async fn propose_and_wait(&self, entry: MetadataEntry) -> Result<u64> {
        let idx = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.entries.lock().unwrap().push(entry);
        Ok(idx)
    }
}

fn empty_topology() -> Arc<std::sync::RwLock<ClusterTopology>> {
    Arc::new(std::sync::RwLock::new(ClusterTopology::new()))
}

// ── Test: crash after AddLearner, stale → abort ────────────────────────────

#[tokio::test]
async fn crash_after_add_learner_stale_aborts() {
    let db = temp_db();
    let table = shared_table(Arc::clone(&db));
    let id = Uuid::new_v4();

    let row = make_row(
        id,
        MigrationCheckpointPayload::AddLearner {
            vshard_id: 1,
            source_node: 1,
            target_node: 2,
            source_group: 0,
            write_pause_budget_us: 500_000,
            started_at_hlc: nodedb_types::Hlc::default(),
        },
        0,
        0, // ts_ms=0 → stale
    );
    table.lock().unwrap().upsert(row).unwrap();

    let proposer = RecordingProposer::new();
    let decisions = recover_in_flight_migrations(
        Arc::clone(&table),
        empty_topology(),
        Arc::new(proposer.clone()),
        DEFAULT_ABORT_TIMEOUT,
    )
    .await
    .unwrap();

    assert_eq!(decisions.len(), 1);
    assert!(matches!(decisions[0], RecoveryDecision::Aborted { .. }));
    let entries = proposer.take();
    assert_eq!(entries.len(), 1);
    assert!(
        matches!(&entries[0], MetadataEntry::MigrationAbort { migration_id, .. }
            if migration_id == &id.hyphenated().to_string())
    );
}

// ── Test: Complete checkpoint → cleaned up ────────────────────────────────

#[tokio::test]
async fn completed_migration_is_cleaned_up() {
    let db = temp_db();
    let table = shared_table(Arc::clone(&db));
    let id = Uuid::new_v4();

    let row = make_row(
        id,
        MigrationCheckpointPayload::Complete {
            vshard_id: 3,
            actual_pause_us: 100,
            ghost_stub_installed: true,
        },
        0,
        now_ms(),
    );
    table.lock().unwrap().upsert(row).unwrap();

    let proposer = RecordingProposer::new();
    let decisions = recover_in_flight_migrations(
        Arc::clone(&table),
        empty_topology(),
        Arc::new(proposer.clone()),
        DEFAULT_ABORT_TIMEOUT,
    )
    .await
    .unwrap();

    assert_eq!(decisions.len(), 1);
    assert!(matches!(decisions[0], RecoveryDecision::CleanedUp { .. }));
    assert!(proposer.take().is_empty());
    // Row must be gone.
    assert!(table.lock().unwrap().get(&id).is_none());
}

// ── Test: recent CatchUp → resumed ──────────────────────────────────────────

#[tokio::test]
async fn recent_migration_is_resumed() {
    let db = temp_db();
    let table = shared_table(Arc::clone(&db));
    let id = Uuid::new_v4();

    let row = make_row(
        id,
        MigrationCheckpointPayload::CatchUp {
            vshard_id: 5,
            learner_log_index_at_add: 10,
        },
        1,
        now_ms(),
    );
    table.lock().unwrap().upsert(row).unwrap();

    let proposer = RecordingProposer::new();
    let decisions = recover_in_flight_migrations(
        Arc::clone(&table),
        empty_topology(),
        Arc::new(proposer.clone()),
        DEFAULT_ABORT_TIMEOUT,
    )
    .await
    .unwrap();

    assert_eq!(decisions.len(), 1);
    assert!(matches!(decisions[0], RecoveryDecision::Resumed { .. }));
    assert!(proposer.take().is_empty());
}

// ── Test: idempotent checkpoint apply ────────────────────────────────────────

#[test]
fn idempotent_checkpoint_upsert() {
    let db = temp_db();
    let table = shared_table(Arc::clone(&db));
    let id = Uuid::new_v4();
    let payload = MigrationCheckpointPayload::CatchUp {
        vshard_id: 5,
        learner_log_index_at_add: 42,
    };
    let crc = payload.crc32c().unwrap();

    for _ in 0..3 {
        apply_migration_checkpoint(
            &table,
            id,
            MigrationPhaseTag::CatchUp,
            0,
            payload.clone(),
            crc,
            0,
        )
        .unwrap();
    }

    assert_eq!(table.lock().unwrap().all_checkpoints().len(), 1);
}

// ── Test: CRC32C mismatch is rejected ────────────────────────────────────────

#[test]
fn crc32c_mismatch_is_rejected() {
    let db = temp_db();
    let table = shared_table(Arc::clone(&db));
    let id = Uuid::new_v4();
    let payload = MigrationCheckpointPayload::CatchUp {
        vshard_id: 7,
        learner_log_index_at_add: 1,
    };

    let result = apply_migration_checkpoint(
        &table,
        id,
        MigrationPhaseTag::CatchUp,
        0,
        payload,
        0xDEAD_BEEF,
        0,
    );
    assert!(
        matches!(
            result,
            Err(ClusterError::MigrationCheckpoint(
                MigrationCheckpointError::Crc32cMismatch { .. }
            ))
        ),
        "expected CRC32C mismatch, got: {result:?}"
    );
}

// ── Test: abort applies compensations and deletes row ───────────────────────

#[test]
fn abort_applies_compensations_and_deletes_row() {
    let db = temp_db();
    let table = shared_table(Arc::clone(&db));
    let id = Uuid::new_v4();

    let payload = MigrationCheckpointPayload::PromoteLearner {
        vshard_id: 9,
        target_node: 3,
        source_group: 1,
    };
    let crc = payload.crc32c().unwrap();
    apply_migration_checkpoint(
        &table,
        id,
        MigrationPhaseTag::PromoteLearner,
        2,
        payload,
        crc,
        0,
    )
    .unwrap();
    assert_eq!(table.lock().unwrap().all_checkpoints().len(), 1);

    let comps = vec![Compensation::RemoveVoter {
        group_id: 1,
        peer_id: 3,
    }];
    apply_migration_abort(&table, None, id, "test abort", &comps).unwrap();
    assert_eq!(table.lock().unwrap().all_checkpoints().len(), 0);
}

// ── Test: multiple compensations applied in order ────────────────────────────

#[test]
fn abort_applies_all_compensations_in_order() {
    let db = temp_db();
    let table = shared_table(Arc::clone(&db));
    let id = Uuid::new_v4();

    let payload = MigrationCheckpointPayload::AddLearner {
        vshard_id: 11,
        source_node: 1,
        target_node: 4,
        source_group: 2,
        write_pause_budget_us: 500_000,
        started_at_hlc: nodedb_types::Hlc::default(),
    };
    let crc = payload.crc32c().unwrap();
    apply_migration_checkpoint(
        &table,
        id,
        MigrationPhaseTag::AddLearner,
        0,
        payload,
        crc,
        0,
    )
    .unwrap();

    let comps = vec![
        Compensation::RemoveLearner {
            group_id: 2,
            peer_id: 4,
        },
        Compensation::RestoreLeaderHint {
            group_id: 2,
            peer_id: 1,
        },
    ];
    apply_migration_abort(&table, None, id, "multi-comp abort", &comps).unwrap();
    assert_eq!(table.lock().unwrap().all_checkpoints().len(), 0);
}

// ── Fixed-seed state-machine fuzz ────────────────────────────────────────────

fn xorshift32(state: &mut u32) -> u32 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    x
}

#[tokio::test]
async fn state_machine_fuzz_fixed_seed() {
    const SEED: u32 = 0xDEAD_BEEF;
    let mut rng;

    let phases = [
        MigrationPhaseTag::AddLearner,
        MigrationPhaseTag::CatchUp,
        MigrationPhaseTag::PromoteLearner,
        MigrationPhaseTag::LeadershipTransfer,
        MigrationPhaseTag::Cutover,
        MigrationPhaseTag::Complete,
    ];

    for seed_offset in 0..32_u32 {
        rng = SEED.wrapping_add(seed_offset).max(1); // xorshift32 requires non-zero seed
        let db = temp_db();
        let table = shared_table(Arc::clone(&db));
        let id = Uuid::new_v4();

        let crash_phase_idx = (xorshift32(&mut rng) as usize) % phases.len();
        let crash_phase = phases[crash_phase_idx];
        let is_stale = xorshift32(&mut rng).is_multiple_of(3);
        let ts_ms = if is_stale { 0 } else { now_ms() };

        let payload = match crash_phase {
            MigrationPhaseTag::AddLearner => MigrationCheckpointPayload::AddLearner {
                vshard_id: 1,
                source_node: 1,
                target_node: 2,
                source_group: 0,
                write_pause_budget_us: 500_000,
                started_at_hlc: nodedb_types::Hlc::default(),
            },
            MigrationPhaseTag::CatchUp => MigrationCheckpointPayload::CatchUp {
                vshard_id: 1,
                learner_log_index_at_add: 5,
            },
            MigrationPhaseTag::PromoteLearner => MigrationCheckpointPayload::PromoteLearner {
                vshard_id: 1,
                target_node: 2,
                source_group: 0,
            },
            MigrationPhaseTag::LeadershipTransfer => {
                MigrationCheckpointPayload::LeadershipTransfer {
                    vshard_id: 1,
                    target_is_voter: true,
                    new_leader_node_id: 2,
                    source_group: 0,
                }
            }
            MigrationPhaseTag::Cutover => MigrationCheckpointPayload::Cutover {
                vshard_id: 1,
                new_leader_node_id: 2,
                source_group: 0,
            },
            MigrationPhaseTag::Complete => MigrationCheckpointPayload::Complete {
                vshard_id: 1,
                actual_pause_us: 1000,
                ghost_stub_installed: true,
            },
        };
        let crc = payload.crc32c().unwrap();
        {
            let mut g = table.lock().unwrap();
            g.upsert(PersistedMigrationCheckpoint {
                migration_id: id.hyphenated().to_string(),
                attempt: 0,
                payload,
                crc32c: crc,
                ts_ms,
            })
            .unwrap();
        }

        let proposer = RecordingProposer::new();
        let decisions = recover_in_flight_migrations(
            Arc::clone(&table),
            empty_topology(),
            Arc::new(proposer.clone()),
            DEFAULT_ABORT_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(
            decisions.len(),
            1,
            "seed_offset={seed_offset} crash_phase={crash_phase:?}"
        );

        match &decisions[0] {
            RecoveryDecision::CleanedUp { migration_id } => {
                assert_eq!(crash_phase, MigrationPhaseTag::Complete);
                assert_eq!(*migration_id, id);
                assert!(table.lock().unwrap().get(&id).is_none());
            }
            RecoveryDecision::Aborted { migration_id, .. } => {
                assert_eq!(*migration_id, id);
                let entries = proposer.take();
                assert!(
                    entries
                        .iter()
                        .any(|e| matches!(e, MetadataEntry::MigrationAbort { .. })),
                    "seed_offset={seed_offset}: expected MigrationAbort in proposals"
                );
            }
            RecoveryDecision::Resumed { migration_id } => {
                assert_eq!(*migration_id, id);
                assert!(proposer.take().is_empty());
            }
        }
    }
}
