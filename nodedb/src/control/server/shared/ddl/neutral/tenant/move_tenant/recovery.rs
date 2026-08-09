// SPDX-License-Identifier: BUSL-1.1

//! Crash recovery and idempotency checks for `MOVE TENANT`.
//!
//! On startup, the maintenance loop scans the journal for in-progress entries
//! and calls [`recover_all`] to resume or compensate each one.
//!
//! At handler entry time, [`tenant_already_in_target`] provides the idempotent
//! short-circuit: if a previously completed move is re-issued, the response
//! is `MOVE_TENANT_ALREADY_AT_TARGET`.
//!
//! Ported verbatim from the pgwire `ddl::tenant::move_tenant::recovery`
//! handler; only the result construction changed from pgwire `Response` /
//! `PgWireError` to the protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::catalog::{MovePhase, MoveTenantJournalEntry, SystemCatalog};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::support::{ddl_err, status};
use super::entry::SNAPSHOT_TIMEOUT;
use super::journal;
use super::{cutover, drain, snapshot};
use nodedb_types::error::sqlstate;

/// Check whether the source database has already been emptied by a prior move.
///
/// Returns `true` if the source database has no active collections AND the
/// target database has at least one — indicating a previously completed move.
/// Both checks use all collections in the respective databases because `MOVE
/// TENANT` transfers the entire source database namespace atomically.
pub fn tenant_already_in_target(
    catalog: &SystemCatalog,
    _tenant_id: TenantId,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
) -> crate::Result<bool> {
    let source_colls = catalog.load_all_collections(source_db_id)?;
    let active_in_source = source_colls.iter().any(|c| c.is_active);
    if active_in_source {
        return Ok(false);
    }
    let target_colls = catalog.load_all_collections(target_db_id)?;
    Ok(target_colls.iter().any(|c| c.is_active))
}

/// Resume or compensate a single in-progress journal entry.
///
/// Called both from handler entry (when a journal entry is found for the
/// same tenant at the start of a new `MOVE TENANT` invocation) and from
/// startup recovery.
pub async fn resume_or_compensate(
    state: &SharedState,
    catalog: &SystemCatalog,
    entry: MoveTenantJournalEntry,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = TenantId::new(entry.tenant_id);
    let source_db_id = DatabaseId::new(entry.source_db_id);
    let target_db_id = DatabaseId::new(entry.target_db_id);

    match entry.phase {
        MovePhase::Preflight | MovePhase::Drain => {
            // Journal recorded but drain never completed.
            // Compensate: remove journal, return error asking operator to retry.
            journal::delete_journal_entry_logged(catalog, tenant_id);
            Err(ddl_err(
                sqlstate::MOVE_TENANT_DRAIN_TIMEOUT,
                format!(
                    "MOVE TENANT '{}' was interrupted during drain and has been rolled back; \
                     please retry the operation",
                    entry.tenant_name
                ),
            ))
        }
        MovePhase::Snapshot => {
            // Drain completed but snapshot was interrupted.
            // Compensate: release drain, remove journal.
            drain::release(state, tenant_id, source_db_id);
            journal::delete_journal_entry_logged(catalog, tenant_id);
            Err(ddl_err(
                sqlstate::MOVE_TENANT_SNAPSHOT_FAILED,
                format!(
                    "MOVE TENANT '{}' was interrupted during snapshot and has been rolled back; \
                     please retry the operation",
                    entry.tenant_name
                ),
            ))
        }
        MovePhase::Cutover => {
            // Snapshot succeeded but cutover was interrupted. Check if cutover
            // actually completed (idempotency: tenant may already be in target).
            let already_moved =
                tenant_already_in_target(catalog, tenant_id, source_db_id, target_db_id)
                    .map_err(|e| ddl_err("XX000", format!("idempotency check: {e}")))?;

            if already_moved {
                // Cutover succeeded but client crashed before reading the response.
                // Clean up the journal and return success.
                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
                state.audit_record(
                    crate::control::security::audit::AuditEvent::AdminAction,
                    Some(tenant_id),
                    &identity.username,
                    &format!(
                        "MOVE TENANT {} FROM {} TO {} recovered (cutover was already complete)",
                        entry.tenant_name, entry.source_db_name, entry.target_db_name
                    ),
                );
                return Ok(status("MOVE TENANT"));
            }

            // Cutover proposal did not apply. Re-run the snapshot and cutover.
            let snapshot_result =
                snapshot::run(state, tenant_id, source_db_id, SNAPSHOT_TIMEOUT).await;
            let snapshot_bytes = match snapshot_result {
                Ok(b) => b,
                Err(ref e) => {
                    drain::release(state, tenant_id, source_db_id);
                    journal::delete_journal_entry_logged(catalog, tenant_id);
                    return Err(ddl_err(sqlstate::MOVE_TENANT_SNAPSHOT_FAILED, e.message()));
                }
            };

            let cutover_result = cutover::run(
                state,
                catalog,
                tenant_id,
                source_db_id,
                target_db_id,
                &snapshot_bytes,
            )
            .await;

            if let Err(ref e) = cutover_result {
                drain::release(state, tenant_id, source_db_id);
                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
                return Err(ddl_err(sqlstate::MOVE_TENANT_CUTOVER_FAILED, e.message()));
            }

            if let Some(ref key) = entry.temp_snapshot_key {
                let _ = snapshot::delete_temp(state, key).await;
            }
            journal::delete_journal_entry_logged(catalog, tenant_id);
            state.audit_record(
                crate::control::security::audit::AuditEvent::AdminAction,
                Some(tenant_id),
                &identity.username,
                &format!(
                    "MOVE TENANT {} FROM {} TO {} recovered (cutover re-applied)",
                    entry.tenant_name, entry.source_db_name, entry.target_db_name
                ),
            );
            Ok(status("MOVE TENANT"))
        }
        MovePhase::Resumed => {
            // Move completed normally; journal entry should have been removed.
            // Clean it up now as a belt-and-suspenders measure.
            journal::delete_journal_entry_logged(catalog, tenant_id);
            Ok(status("MOVE TENANT"))
        }
    }
}

/// Scan the journal at startup and recover any in-progress entries.
///
/// Called once during server startup before accepting connections.
pub async fn recover_all(state: &SharedState) {
    let catalog = state.credentials.catalog();

    let entries = match journal::scan_all_journal_entries(catalog) {
        Ok(e) => e,
        Err(err) => {
            tracing::error!(
                error = %err,
                "move_tenant recovery: failed to scan journal; skipping"
            );
            return;
        }
    };

    for entry in entries {
        tracing::info!(
            tenant = entry.tenant_id,
            phase = ?entry.phase,
            "move_tenant recovery: found in-progress entry"
        );
        let tenant_id = TenantId::new(entry.tenant_id);
        let source_db_id = DatabaseId::new(entry.source_db_id);
        let target_db_id = DatabaseId::new(entry.target_db_id);

        match entry.phase {
            MovePhase::Preflight | MovePhase::Drain | MovePhase::Snapshot => {
                // Compensate: no data was moved; release drain, remove journal.
                drain::release(state, tenant_id, source_db_id);
                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
                tracing::info!(
                    tenant = entry.tenant_id,
                    "move_tenant recovery: compensated (no data moved)"
                );
            }
            MovePhase::Cutover => {
                // Check if cutover completed before crash. A catalog read
                // failure here is not silently swallowed — surface it in logs
                // and treat as "not moved" so we re-attempt cutover (which is
                // idempotent: the Raft proposal is rejected if already
                // applied).
                let already_moved = match catalog.load_all_collections(source_db_id) {
                    Ok(colls) => colls.iter().all(|col| !col.is_active),
                    Err(err) => {
                        tracing::warn!(
                            tenant = entry.tenant_id,
                            source_db = entry.source_db_id,
                            error = %err,
                            "move_tenant recovery: failed to read source collections; \
                             treating as not-moved and will retry cutover"
                        );
                        false
                    }
                };

                if already_moved {
                    tracing::info!(
                        tenant = entry.tenant_id,
                        "move_tenant recovery: cutover already complete; cleaning journal"
                    );
                } else {
                    // Re-attempt cutover.
                    if let Ok(snap_bytes) =
                        snapshot::run(state, tenant_id, source_db_id, SNAPSHOT_TIMEOUT).await
                    {
                        let _ = cutover::run(
                            state,
                            catalog,
                            tenant_id,
                            source_db_id,
                            target_db_id,
                            &snap_bytes,
                        )
                        .await;
                    }
                    drain::release(state, tenant_id, source_db_id);
                }

                if let Some(ref key) = entry.temp_snapshot_key {
                    let _ = snapshot::delete_temp(state, key).await;
                }
                journal::delete_journal_entry_logged(catalog, tenant_id);
            }
            MovePhase::Resumed => {
                // Belt-and-suspenders cleanup.
                journal::delete_journal_entry_logged(catalog, tenant_id);
            }
        }
    }
}
