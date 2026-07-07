// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral `ALTER COLLECTION` handlers.
//!
//! Provides the [`DdlError`] constructor (preserving the exact SQLSTATE codes
//! and messages the pgwire handlers produced), the single-row `ALTER`-status
//! result builder, and the neutral `propose_and_apply` mirror of the pgwire
//! `ddl::catalog_propose::propose_and_apply` (same propose + local-apply
//! ordering, same `XX000` / `"metadata propose: {e}"` error).

use crate::control::catalog_entry::CatalogEntry;
use crate::control::catalog_entry::apply::local::apply_locally_if_needed;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

/// Construct a [`DdlError`], preserving the exact SQLSTATE codes and messages
/// the pgwire handlers produced.
pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
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

/// Neutral mirror of the pgwire `ddl::catalog_propose::propose_and_apply`.
///
/// Propose `entry` through the metadata raft group and, when the proposer
/// reports `Ok(0)` (single-node / no-applier path), apply the entry locally so
/// the primary row and the companion `StoredOwner` row both land in redb.
/// Returns the committed `log_index`.
pub(super) fn propose_and_apply(
    state: &SharedState,
    entry: &CatalogEntry,
) -> Result<u64, DdlError> {
    let log_index = propose_catalog_entry(state, entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    apply_locally_if_needed(state, entry, log_index);
    Ok(log_index)
}

/// Async variant of [`propose_and_apply`] for online DDL that runs
/// concurrently with ingest.
///
/// The local catalog apply (`log_index == 0` path) performs a redb write
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
) -> Result<u64, DdlError> {
    let log_index = propose_catalog_entry(state, &entry)
        .map_err(|e| err("XX000", format!("metadata propose: {e}")))?;
    if log_index == 0 {
        // Clone only the cheap `Arc<Database>` handle (not `SharedState`) so
        // the blocking closure owns exactly what the apply needs.
        let catalog = state.credentials.catalog().clone();
        tokio::task::spawn_blocking(move || {
            crate::control::catalog_entry::apply::apply_to(&entry, &catalog)
        })
        .await
        .map_err(|e| err("XX000", format!("catalog apply join: {e}")))?;
    }
    Ok(log_index)
}
