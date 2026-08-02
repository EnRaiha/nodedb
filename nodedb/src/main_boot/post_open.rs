// SPDX-License-Identifier: BUSL-1.1

//! Post-`SharedState::open` catalog steps: fire the catalog-open gate,
//! replay the surrogate WAL, recover in-progress tenant moves, and
//! bootstrap the superuser credential.

use std::sync::Arc;

use nodedb::ServerConfig;
use nodedb::bootstrap;
use nodedb::control::startup::ReadyGate;
use nodedb::control::state::SharedState;

/// Pure relocation of what used to be inline in `main()` right after
/// `SharedState::open` + wiring returned.
pub(crate) async fn run(
    shared: &Arc<SharedState>,
    wal_records: &Arc<[nodedb_wal::WalRecord]>,
    replay_tombstones: &nodedb_wal::TombstoneSet,
    config: &ServerConfig,
    catalog_gate: &ReadyGate,
) -> anyhow::Result<()> {
    // System catalog (redb) is open — fire the ClusterCatalogOpen gate.
    catalog_gate.fire();

    // Replay surrogate WAL records into the in-memory registry.
    bootstrap::credentials::replay_surrogate_wal(shared, wal_records, replay_tombstones);

    // Recover any in-progress MOVE TENANT operations from the journal.
    // This runs synchronously before accepting connections so that
    // in-flight tenant moves are resolved before any client can issue
    // new ones against the same tenant.
    nodedb::control::server::shared::ddl::neutral::tenant::move_tenant::recovery::recover_all(
        shared,
    )
    .await;

    // Bootstrap superuser credential (or warn about trust mode).
    bootstrap::credentials::bootstrap_superuser(shared, config)?;

    Ok(())
}
