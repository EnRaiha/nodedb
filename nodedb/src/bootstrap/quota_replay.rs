// SPDX-License-Identifier: BUSL-1.1

//! Boot-time replay of persisted quota records into live enforcement.
//!
//! Quota rows are durable catalog state, but the components that enforce
//! them — the admission registry, the memory governor, the maintenance CPU
//! budget — are rebuilt empty on every start. Without this pass a restarted
//! node reports caps through `SHOW QUOTA` that nothing applies, until an
//! operator re-runs the DDL. Replay pushes every row back through the same
//! install path the DDL apply uses.

use std::sync::Arc;

use nodedb_types::QuotaRecord;
use tracing::{error, info, warn};

use crate::control::catalog_entry::post_apply::quota;
use crate::control::state::SharedState;
use crate::diag::{DATABASE_SCOPE, TENANT_SCOPE};

/// Install every persisted quota record into live enforcement.
///
/// Never fails the boot: a row that cannot be applied is logged and skipped.
pub fn replay_quotas(shared: &Arc<SharedState>) {
    let catalog = shared.credentials.catalog();

    let mut databases = 0usize;
    let mut skipped = 0usize;
    match catalog.list_database_quotas_lossy() {
        Ok((rows, bad_keys)) => {
            for key in bad_keys {
                warn!(database = key, "quota row skipped: value did not decode");
                crate::diag::quota_row_undecodable(DATABASE_SCOPE, key, None);
                skipped += 1;
            }
            for (db_id, record) in rows {
                if !usable(&record, DATABASE_SCOPE, db_id.as_u64(), None) {
                    skipped += 1;
                    continue;
                }
                quota::put_database(db_id, &record, shared);
                databases += 1;
            }
        }
        Err(e) => {
            error!(error = %e, "database quota replay skipped: catalog read failed");
            crate::diag::quota_scope_replay_aborted(&e, DATABASE_SCOPE);
        }
    }

    let mut tenants = 0usize;
    match catalog.list_all_tenant_quotas_lossy() {
        Ok((rows, bad_keys)) => {
            for (db, tenant) in bad_keys {
                warn!(
                    database = db,
                    tenant, "quota row skipped: value did not decode"
                );
                crate::diag::quota_row_undecodable(TENANT_SCOPE, db, Some(tenant));
                skipped += 1;
            }
            for (db_id, tenant_id, record) in rows {
                if !usable(
                    &record,
                    TENANT_SCOPE,
                    db_id.as_u64(),
                    Some(tenant_id.as_u64()),
                ) {
                    skipped += 1;
                    continue;
                }
                quota::put_tenant(db_id, tenant_id, &record, shared);
                tenants += 1;
            }
        }
        Err(e) => {
            error!(error = %e, "tenant quota replay skipped: catalog read failed");
            crate::diag::quota_scope_replay_aborted(&e, TENANT_SCOPE);
        }
    }

    info!(
        databases,
        tenants, skipped, "quota replay installed persisted caps"
    );
}

/// Report whether a record holds its invariants, recording the scope if not.
fn usable(
    record: &QuotaRecord,
    scope: &'static str,
    database_id: u64,
    tenant_id: Option<u64>,
) -> bool {
    match record.validate() {
        Ok(()) => true,
        Err(e) => {
            warn!(
                scope,
                database_id,
                tenant_id = ?tenant_id,
                error = %e,
                "quota row skipped: invalid record"
            );
            crate::diag::quota_row_invalid(&e, scope, database_id, tenant_id);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor};
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// One mebibyte, the memory ceiling these tests configure.
    const MIB: u64 = 1024 * 1024;

    /// Shared state with a live catalog, admission registry, and governor.
    fn make_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut shared = SharedState::new(dispatcher, wal).expect("shared state");
        let state = Arc::get_mut(&mut shared).expect("state is uniquely owned here");
        state.governor = Some(make_governor());
        (shared, dir)
    }

    /// Governor with a generous global ceiling so only scope caps can deny.
    fn make_governor() -> Arc<MemoryGovernor> {
        let engine_bytes = 64 * MIB as usize;
        let engine_limits = EngineLimits::uniform(engine_bytes);
        let gov = MemoryGovernor::new(GovernorConfig {
            global_ceiling: engine_bytes * EngineId::ALL.len(),
            engine_limits,
        })
        .expect("governor");
        Arc::new(gov)
    }

    fn record(max_connections: u32, max_memory_bytes: u64) -> QuotaRecord {
        QuotaRecord {
            max_connections,
            max_memory_bytes,
            ..QuotaRecord::DEFAULT
        }
    }

    /// Reserve `bytes` for the scope, returning whether the governor allows it.
    fn reserve_allowed(
        shared: &Arc<SharedState>,
        db: DatabaseId,
        tenant: TenantId,
        bytes: u64,
    ) -> bool {
        let gov = shared.governor.clone().expect("governor");
        gov.try_reserve(db, tenant, EngineId::DocumentSchemaless, bytes as usize)
            .is_ok()
    }

    #[test]
    fn database_quota_row_is_installed_into_registry_and_governor() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(11);
        let tenant = TenantId::new(0);
        shared
            .credentials
            .catalog()
            .write_database_quota(db, &record(2, MIB))
            .expect("persist database quota");

        replay_quotas(&shared);

        assert_eq!(
            shared.admission_registry.database_live_connections(db),
            Some(0),
            "the persisted connection cap must be live after replay"
        );
        assert!(
            !reserve_allowed(&shared, db, tenant, 2 * MIB),
            "2 MiB must exceed the replayed 1 MiB database budget"
        );
    }

    #[test]
    fn tenant_quota_row_is_installed_for_its_own_pair() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(12);
        let tenant = TenantId::new(5);
        let other = TenantId::new(6);
        shared
            .credentials
            .catalog()
            .write_tenant_quota(db, tenant, &record(2, MIB))
            .expect("persist tenant quota");

        replay_quotas(&shared);

        assert_eq!(
            shared
                .admission_registry
                .tenant_live_connections(db, tenant),
            Some(0),
            "the persisted tenant cap must be live after replay"
        );
        assert_eq!(
            shared.admission_registry.tenant_live_connections(db, other),
            None,
            "a tenant with no row must stay uncapped"
        );
        assert!(
            !reserve_allowed(&shared, db, tenant, 2 * MIB),
            "2 MiB must exceed the replayed 1 MiB tenant budget"
        );
    }

    #[test]
    fn replay_with_no_rows_installs_nothing() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(13);
        let tenant = TenantId::new(7);

        replay_quotas(&shared);

        assert_eq!(
            shared.admission_registry.database_live_connections(db),
            None
        );
        assert_eq!(
            shared
                .admission_registry
                .tenant_live_connections(db, tenant),
            None
        );
        assert!(reserve_allowed(&shared, db, tenant, 2 * MIB));
    }

    #[test]
    fn zero_memory_row_installs_the_connection_cap_only() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(14);
        let tenant = TenantId::new(0);
        shared
            .credentials
            .catalog()
            .write_database_quota(db, &record(3, 0))
            .expect("persist database quota");

        replay_quotas(&shared);

        assert_eq!(
            shared.admission_registry.database_live_connections(db),
            Some(0),
            "the connection cap applies even with no memory ceiling"
        );
        assert!(
            reserve_allowed(&shared, db, tenant, 8 * MIB),
            "max_memory_bytes = 0 means unlimited, so no budget is installed"
        );
    }

    /// Bytes redb accepts as a value but zerompk cannot decode.
    const CORRUPT: &[u8] = &[0xc1, 0xc1, 0xc1];

    #[test]
    fn corrupt_database_row_is_skipped_and_the_others_install() {
        let (shared, _dir) = make_state();
        let good = DatabaseId::new(21);
        let bad = DatabaseId::new(22);
        let catalog = shared.credentials.catalog();
        catalog
            .write_database_quota(good, &record(2, 0))
            .expect("persist good quota");
        catalog
            .write_raw_database_quota(bad, CORRUPT)
            .expect("persist corrupt row");

        replay_quotas(&shared);

        assert_eq!(
            shared.admission_registry.database_live_connections(good),
            Some(0),
            "one bad row must not disarm every other database"
        );
        assert_eq!(
            shared.admission_registry.database_live_connections(bad),
            None,
            "the undecodable row installs nothing"
        );
    }

    #[test]
    fn corrupt_tenant_row_is_skipped_and_the_others_install() {
        let (shared, _dir) = make_state();
        let db = DatabaseId::new(23);
        let good = TenantId::new(1);
        let bad = TenantId::new(2);
        let catalog = shared.credentials.catalog();
        catalog
            .write_tenant_quota(db, good, &record(2, 0))
            .expect("persist good quota");
        catalog
            .write_raw_tenant_quota(db, bad, CORRUPT)
            .expect("persist corrupt row");

        replay_quotas(&shared);

        assert_eq!(
            shared.admission_registry.tenant_live_connections(db, good),
            Some(0),
            "one bad row must not disarm every other tenant"
        );
        assert_eq!(
            shared.admission_registry.tenant_live_connections(db, bad),
            None,
            "the undecodable row installs nothing"
        );
    }

    #[test]
    fn each_database_keeps_its_own_connection_cap() {
        let (shared, _dir) = make_state();
        let tight = DatabaseId::new(15);
        let loose = DatabaseId::new(16);
        let catalog = shared.credentials.catalog();
        catalog
            .write_database_quota(tight, &record(1, 0))
            .expect("persist tight quota");
        catalog
            .write_database_quota(loose, &record(4, 0))
            .expect("persist loose quota");

        replay_quotas(&shared);

        let registry = &shared.admission_registry;
        let permit = registry
            .try_acquire_database(tight)
            .expect("first admission")
            .expect("a configured cap hands out a permit");
        registry
            .try_acquire_database(tight)
            .expect_err("the cap of 1 refuses the second connection");
        let loose_permit = registry
            .try_acquire_database(loose)
            .expect("the second database has its own cap of 4")
            .expect("a configured cap hands out a permit");
        drop(permit);
        drop(loose_permit);
    }
}
