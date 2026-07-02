// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral propose-and-apply helper for parent-replicated DDL.
//!
//! Neutral twin of the pgwire `catalog_propose::propose_and_apply`: it runs the
//! same three-step ritual (build entry → propose through the metadata raft group
//! → local-apply fallback when the proposer reports `Ok(0)`), but yields a
//! protocol-neutral [`DdlError`] instead of a pgwire `PgWireError` so the
//! neutral family handlers carry no pgwire types.
//!
//! Every neutral `CREATE` / `ALTER` handler routes its catalog write through
//! this helper, which makes the step-3 local-apply omission unrepresentable.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::catalog_entry::apply::local::apply_locally_if_needed;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::state::SharedState;

use super::result::DdlError;

/// Propose `entry` through the metadata raft group and, when the proposer
/// reports `Ok(0)` (single-node / no-applier path), apply the entry locally so
/// the primary row and the companion `StoredOwner` row both land in redb.
///
/// Returns the committed `log_index`. Callers gate single-node-only side
/// effects (in-memory registry refresh) on `log_index == 0`; the remote-apply
/// path (`log_index > 0`) reaches the corresponding applier on the same node.
pub fn propose_and_apply(state: &SharedState, entry: &CatalogEntry) -> Result<u64, DdlError> {
    let log_index = propose_catalog_entry(state, entry).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("metadata propose: {e}"),
    })?;
    apply_locally_if_needed(state, entry, log_index);
    Ok(log_index)
}
