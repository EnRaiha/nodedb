// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral `ALTER COLLECTION` handlers.
//!
//! Provides the [`DdlError`] constructor (preserving the exact SQLSTATE codes
//! and messages the pgwire handlers produced), the single-row `ALTER`-status
//! result builder, and the neutral `propose_and_apply` mirror of the pgwire
//! `ddl::catalog_propose::propose_and_apply` (same propose + local-apply
//! ordering, same `XX000` / `"metadata propose: {e}"` error).

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::catalog_entry::apply::local::apply_locally_if_needed;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::security::catalog::StoredCollection;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced.
pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Build the single `Status` result every `ALTER` sub-command returns. `command`
/// is the pgwire command tag, preserved verbatim (`ALTER TABLE` for ADD COLUMN,
/// `ALTER COLLECTION` for every other sub-command).
pub(super) fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Look up the collection `name` for `tenant_id` and reject it unless it is
/// active. `catalog.get_collection` does not filter on `is_active`, so a
/// bare `.ok_or_else(...)` on `None` still returns a soft-deleted (dropped)
/// row; a dropped collection must be indistinguishable from a missing one to
/// the caller, so both cases share the same SQLSTATE `42P01` "does not
/// exist" error. Shared by every `ALTER COLLECTION` sub-command, including
/// `strict_schema::load_strict_collection`, which layers its own strict-type
/// and schema-decode checks on top.
pub(super) fn load_active_collection(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> Result<StoredCollection, DdlError> {
    state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id, name)
        .map_err(|e| err("XX000", e.to_string()))?
        .filter(|c| c.is_active)
        .ok_or_else(|| err("42P01", format!("collection '{name}' does not exist")))
}

/// Neutral mirror of the pgwire `ddl::catalog_propose::propose_and_apply`.
///
/// Propose `entry` through the metadata raft group and, when the proposer
/// reports `LocalOnly`, apply the entry locally so
/// the primary row and the companion `StoredOwner` row both land in redb.
pub(super) fn propose_and_apply(
    state: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, DdlError> {
    let outcome = propose_catalog_entry(state, entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    apply_locally_if_needed(state, entry, outcome);
    Ok(outcome)
}

/// Async variant of [`propose_and_apply`] for online DDL that runs
/// concurrently with ingest.
///
/// The local catalog apply (`LocalOnly` path) performs a redb write
/// transaction whose `commit()` issues an `fsync`; that fsync can take tens
/// of milliseconds. Running it inline on the Tokio worker would monopolise
/// the worker for the duration of the flush, stalling every concurrent
/// `INSERT` task scheduled on it — an online `ALTER` must never block the
/// write path. Moving the blocking commit onto a `spawn_blocking` thread
/// keeps the worker free to service writes while the catalog is made durable.
///
/// Proposal ordering is unchanged: the durable apply still completes before
/// this call returns, so the cross-core schema-register barrier that follows
/// observes the applied schema.
pub(super) async fn propose_and_apply_async(
    state: &SharedState,
    entry: CatalogEntry,
) -> Result<ProposeOutcome, DdlError> {
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        // Clone only the cheap `Arc<Database>` handle (not `SharedState`) so
        // the blocking closure owns exactly what the apply needs.
        let catalog = state.credentials.catalog().clone();
        tokio::task::spawn_blocking(move || {
            crate::control::catalog_entry::apply::apply_to(&entry, &catalog)
        })
        .await
        .map_err(|e| err("XX000", format!("catalog apply join: {e}")))?
        .map_err(|e| err("XX000", format!("catalog apply: {e}")))?;
    }
    Ok(outcome)
}
