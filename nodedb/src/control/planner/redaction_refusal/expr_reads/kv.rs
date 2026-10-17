// SPDX-License-Identifier: BUSL-1.1

//! Column reads of the key-value engine's operations.

use nodedb_physical::physical_plan::KvOp;

use super::reads::NodeReads;

pub(super) fn reads(op: &KvOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        KvOp::Scan {
            filters,
            computed_columns,
            sort_keys,
            ..
        } => {
            out.filter_bytes(filters)?;
            out.computed_bytes(computed_columns)?;
            out.sort_keys(sort_keys);
        }
        KvOp::InsertOnConflictUpdate { updates, .. } => out.updates(updates),
        // The assignments carry encoded literals, so only the predicate reads.
        KvOp::PredicateUpdate { filters, .. } | KvOp::PredicateDelete { filters, .. } => {
            out.filter_bytes(filters)?;
        }
        KvOp::ResolveWrite(inner) => reads(inner, out)?,
        // A read or delete by key reports whether the key exists.
        KvOp::Get { .. }
        | KvOp::Delete { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. } => out.key_lookup = true,
        // Writes of a caller-supplied entry, counters, sorted-index reads,
        // DDL, and maintenance read no value column in a predicate or an
        // expression.
        KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::BatchPut { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Truncate { .. }
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTxnRead { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::ResolvedWrite { .. } => {}
    }
    Ok(())
}
