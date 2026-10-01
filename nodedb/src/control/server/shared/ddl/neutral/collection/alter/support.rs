// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral `ALTER COLLECTION` handlers.
//!
//! Provides the [`DdlError`] constructor, the single-row `ALTER`-status
//! result builder, and the neutral `propose_and_apply`. A propose error keeps its own
//! SQLSTATE under a `"metadata propose"` prefix.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::security::catalog::StoredCollection;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

/// Construct a [`DdlError`] from a SQLSTATE code and a message.
pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Build the single `Status` result every `ALTER` sub-command returns. `command`
/// is the pgwire command tag (`ALTER TABLE` for ADD COLUMN,
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
        .map_err(|e| DdlError::from_error(&e))?
        .filter(|c| c.is_active)
        .ok_or_else(|| err("42P01", format!("collection '{name}' does not exist")))
}

/// Propose `entry` from an async handler, including online DDL that runs
/// concurrently with ingest. On return it applied on this node, primary row
/// and companion `StoredOwner` row both, or it is held for COMMIT. It awaits
/// the entry's post-apply Data Plane work on any runtime flavor.
///
/// The apply's redb commit issues an `fsync`. The proposer runs that wait off
/// the Tokio worker's task queue on a multi-thread runtime, so an online
/// `ALTER` never stalls the `INSERT` tasks scheduled on it. The durable apply
/// completes before this call returns, so the cross-core schema-register
/// barrier that follows observes the applied schema.
pub(super) async fn propose_and_apply_async(
    state: &SharedState,
    entry: CatalogEntry,
) -> Result<ProposeOutcome, DdlError> {
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))
}
