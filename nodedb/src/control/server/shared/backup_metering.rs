// SPDX-License-Identifier: BUSL-1.1

//! Quota admission and metering for whole-tenant backup and restore.
//!
//! A backup or restore has no `PhysicalPlan`, so each describes itself under
//! the synthetic collection marker `tenant:<id>`. A scope grant caps it only
//! when written against that marker, or against `*`. Every door runs the same
//! pair: the admission before the first read or write, the charge on the
//! success path, so the charge never refuses anything.
//!
//! * The backup permission covers a backup and a restore: the tenant COPY path
//!   and `BACKUP` / `RESTORE DATABASE` alike.
//! * A `RESTORE DATABASE` also counts against the write quota of every
//!   collection it restores, under the collection's qualified name as DML and
//!   COPY charge it, so a write grant on one collection caps the restore. A
//!   write grant on `*` covers every collection and is charged once per row.
//!   A write grant on the tenant's `tenant:<id>` marker caps the tenant's
//!   whole restore and is charged the tenant's rows.

use nodedb_types::calvin::EngineTag;

use crate::control::backup::CollectionRows;
use crate::control::security::identity::Permission;
use crate::control::security::permission::parse_permission;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_types::QualifiedCollection;

use super::metering::{PlanMeteringInfo, meter_dispatch};
use super::quota_admission::admit_quota_for_dispatch;

fn tenant_marker(tenant_id: u64, permission: Permission) -> PlanMeteringInfo {
    PlanMeteringInfo::for_collection(
        format!("tenant:{tenant_id}"),
        EngineTag::Meta,
        "sql",
        permission,
    )
}

/// Refuse a backup or restore of `tenant_id` whose covering scope already
/// spent its hard cap.
pub(crate) fn admit_backup_restore_quota(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    tenant_id: u64,
) -> crate::Result<()> {
    if !state.metering_config.enabled {
        return Ok(());
    }
    admit_quota_for_dispatch(state, scope, &tenant_marker(tenant_id, Permission::Backup))
}

/// Meter one completed backup or restore of `tenant_id`. `rows: None` charges
/// one unit.
pub(crate) fn meter_backup_restore(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    tenant_id: u64,
    rows: Option<u64>,
) {
    if !state.metering_config.enabled {
        return;
    }
    meter_dispatch(
        state,
        scope,
        &tenant_marker(tenant_id, Permission::Backup),
        rows,
    );
}

/// The name a restored collection's write quota is admitted and charged
/// under. A collection of a database this cluster lacks has no qualified name
/// before the restore creates the database, so only a `*` grant covers it.
fn restored_collection(collection: &CollectionRows) -> String {
    match collection.database_id {
        Some(database_id) => {
            QualifiedCollection::new(DatabaseId::new(database_id), &collection.collection)
                .as_str()
                .to_string()
        }
        None => "*".to_string(),
    }
}

fn collection_write(collection: &CollectionRows) -> PlanMeteringInfo {
    PlanMeteringInfo::for_collection(
        restored_collection(collection),
        EngineTag::Meta,
        "sql",
        Permission::Write,
    )
}

/// Refuse a restore into `tenant_id` when a write scope covering its
/// `tenant:<id>` marker, or any of its `collections`, already spent its hard
/// cap.
pub(crate) fn admit_restore_write_quota(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    tenant_id: u64,
    collections: &[CollectionRows],
) -> crate::Result<()> {
    if !state.metering_config.enabled {
        return Ok(());
    }
    admit_quota_for_dispatch(state, scope, &tenant_marker(tenant_id, Permission::Write))?;
    for collection in collections {
        admit_quota_for_dispatch(state, scope, &collection_write(collection))?;
    }
    Ok(())
}

/// Charge the rows a restore verified in each of `collections` of
/// `tenant_id` to the write quota: every covering collection or `*` grant per
/// collection, then every grant on the tenant's marker once for the total.
pub(crate) fn meter_restore_writes(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    tenant_id: u64,
    collections: &[CollectionRows],
) {
    if !state.metering_config.enabled {
        return;
    }
    for collection in collections {
        meter_dispatch(
            state,
            scope,
            &collection_write(collection),
            Some(collection.rows),
        );
    }
    let rows = collections.iter().map(|collection| collection.rows).sum();
    charge_marker_grants(state, scope, &format!("tenant:{tenant_id}"), rows);
}

/// Charge `rows` to every held write scope that grants exactly `marker`. A
/// `*` grant also covers the marker, but the per-collection charge already
/// charged it for these rows.
fn charge_marker_grants(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    marker: &str,
    rows: u64,
) {
    if scope.identity().is_internal_service() {
        return;
    }
    let cost = state
        .metering_config
        .operation_costs
        .get("sql")
        .copied()
        .unwrap_or(1);
    let tokens = cost.saturating_mul(rows.max(1));
    let auth = scope.auth();
    let now_secs = crate::control::security::time::now_secs();
    for scope_name in state.scope_grants.effective_scopes(&auth.id, &auth.org_ids) {
        let grants_marker =
            state
                .scope_defs
                .resolve(&scope_name)
                .into_iter()
                .any(|(permission, collection)| {
                    parse_permission(&permission) == Some(Permission::Write) && collection == marker
                });
        if grants_marker {
            state
                .quota_manager
                .record_usage(&scope_name, &auth.id, tokens, now_secs);
        }
    }
}
