// SPDX-License-Identifier: BUSL-1.1

//! Authorization for reads that name a sorted index instead of a collection.
//!
//! `RANK` / `TOPK` / `RANGE` / `SORTED_COUNT` take an index name and nothing
//! else, yet every one of them returns the stored keys of the collection the
//! index was built over. The collection is what the caller must hold `Read`
//! on, so it is resolved from the index registry — the record
//! `CREATE SORTED INDEX` files — and the read is gated on it.
//!
//! None of these plans carries a filter slot: the reply is a rank, a count, or
//! a ranked key list, never a row body. So the gate is the refusing one — a
//! read policy on the owning collection cannot be honored through this shape
//! and fails closed instead of returning the unfiltered ordering.

use crate::control::security::catalog::IndexKind;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::DdlError;
use super::super::refuse_gate::RefusingReadGate;
use super::parse::ddl_err;

/// The collection `index_name` was built over, or `None` when no such index
/// is registered.
///
/// A catalog failure is an error rather than `None`: a lookup that could not
/// be completed says nothing about whether the index exists, and treating it
/// as absent would drop the registry cleanup a drop depends on.
pub(super) fn resolve_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    index_name: &str,
) -> Result<Option<String>, DdlError> {
    state
        .credentials
        .catalog()
        .index_collection(
            database_id.as_u64(),
            identity.tenant_id.as_u64(),
            index_name,
            IndexKind::Sorted,
        )
        .map_err(|error| {
            ddl_err(
                "58000",
                format!("unable to resolve the collection of sorted index '{index_name}': {error}"),
            )
        })
}

/// The collection `index_name` was built over.
///
/// An index with no registry record resolves to nothing and the read is
/// refused: without a collection there is no grant to check, and running the
/// read anyway is the bypass this resolution exists to close.
pub(super) fn owning_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    index_name: &str,
) -> Result<String, DdlError> {
    resolve_collection(state, identity, database_id, index_name)?.ok_or_else(|| {
        ddl_err(
            "42704",
            format!("sorted index '{index_name}' does not exist"),
        )
    })
}

/// Fail closed unless the caller may read the index's owning collection, and
/// no read policy restricts it there.
///
/// `what` completes the sentence "RLS policies on '<collection>' are not
/// supported with {what}", so it names the function and says what its result
/// carries instead of rows.
///
/// Returns the owning collection the read was gated on. That collection is also
/// what routes the read to the one core holding the index's tree
/// (`super::dispatch::SortedIndexTarget`), so the gate and the dispatch resolve
/// it once, together, and cannot name different collections.
pub(super) fn gate_read(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    index_name: &str,
    what: &str,
) -> Result<String, DdlError> {
    let collection = owning_collection(state, identity, database_id, index_name)?;
    RefusingReadGate::open(state, identity, database_id, &collection, what)?;
    Ok(collection)
}
