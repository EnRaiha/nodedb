// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral propose-and-apply helper for parent-replicated DDL.
//!
//! Neutral twin of the pgwire `catalog_propose::propose_and_apply`: it runs the
//! same three-step ritual (build entry → propose through the metadata raft group
//! → local apply when the proposer reports `LocalOnly`), but yields a
//! protocol-neutral [`DdlError`] instead of a pgwire `PgWireError` so the
//! neutral family handlers carry no pgwire types.
//!
//! Every neutral `CREATE` / `ALTER` handler routes its catalog write through
//! this helper, which makes the step-3 local-apply omission unrepresentable.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::catalog_entry::apply::local::apply_locally_if_needed;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;

use super::result::DdlError;

/// Propose `entry` through the metadata raft group and, when the proposer
/// reports [`ProposeOutcome::LocalOnly`], apply the entry locally so the
/// primary row and the companion `StoredOwner` row both land in redb.
///
/// Callers gate their own single-node-only side effects (in-memory registry
/// refresh) on `needs_local_apply`. A `Buffered` outcome belongs to an open
/// transaction: nothing durable is applied. The Data-Plane registration a
/// collection CREATE/ALTER dispatches next is deliberately NOT gated on the
/// outcome — the transaction encodes its own writes against the shape it sees,
/// and ROLLBACK puts the Data Plane back (`session::ddl_rollback`).
pub fn propose_and_apply(
    state: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, DdlError> {
    let outcome = propose_catalog_entry(state, entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    apply_locally_if_needed(state, entry, outcome);
    Ok(outcome)
}
