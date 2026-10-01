// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral propose-and-apply helper for parent-replicated DDL.
//!
//! Neutral twin of the pgwire `catalog_propose::propose_and_apply`: it
//! proposes through the metadata proposer, but yields a protocol-neutral
//! [`DdlError`] instead of a pgwire `PgWireError` so the neutral family
//! handlers carry no pgwire types.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;

use super::result::DdlError;

/// Propose `entry`, on any runtime flavor. On return it applied on this node,
/// with both post-apply lanes awaited, or it is held for COMMIT.
///
/// A `Buffered` outcome belongs to an open transaction: nothing durable is
/// applied. The Data-Plane registration a collection CREATE/ALTER dispatches
/// next is deliberately NOT gated on the outcome — the transaction encodes its
/// own writes against the shape it sees, and ROLLBACK puts the Data Plane back
/// (`session::ddl_rollback`).
pub async fn propose_and_apply_async(
    state: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, DdlError> {
    propose_catalog_entry_async(state, entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))
}
