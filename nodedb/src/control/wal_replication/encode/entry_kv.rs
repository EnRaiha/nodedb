// SPDX-License-Identifier: BUSL-1.1

//! Classify a `KvOp` into an optional `ReplicatedWrite`.
//!
//! Exhaustive over `KvOp` (not a catch-all): a new variant is a compile error
//! here, so no future KV write is silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::kv;
use nodedb_physical::physical_plan::KvOp;

/// Encode a `KvOp` write variant into its `ReplicatedWrite` wire shape, or
/// `None` when the op is not a single-shard replicated write.
pub(super) fn kv_write(op: &KvOp) -> Option<ReplicatedWrite> {
    Some(match op {
        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        } => kv::put(collection, key, value, *ttl_ms, surrogate.as_u32()),
        // The compiled RLS predicates and the RETURNING projection every write
        // below carries are properties of the requesting session, not of the
        // row: they are deliberately absent from the durable record, so a
        // replay re-applies the write that was already admitted rather than
        // re-deciding it — and answers no client, so it projects nothing.
        KvOp::Delete {
            collection, keys, ..
        } => kv::delete(collection, keys),
        KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        } => kv::insert(collection, key, value, *ttl_ms, surrogate.as_u32()),
        KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        } => kv::insert_if_absent(collection, key, value, *ttl_ms, surrogate.as_u32()),
        KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
            ..
        } => kv::insert_on_conflict_update(
            collection,
            key,
            value,
            *ttl_ms,
            updates,
            surrogate.as_u32(),
        ),
        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates,
            ..
        } => kv::batch_put(collection, entries, *ttl_ms, surrogates),
        KvOp::Expire {
            collection,
            key,
            ttl_ms,
            ..
        } => kv::expire(collection, key, *ttl_ms),
        KvOp::Persist {
            collection, key, ..
        } => kv::persist(collection, key),
        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
            ..
        } => kv::incr(collection, key, *delta, *ttl_ms, surrogate.as_u32()),
        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
            ..
        } => kv::incr_float(collection, key, *delta, surrogate.as_u32()),
        KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
            ..
        } => kv::cas(collection, key, expected, new_value, surrogate.as_u32()),
        KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate,
            ..
        } => kv::get_set(collection, key, new_value, surrogate.as_u32()),
        KvOp::RegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => kv::register_sorted_index(kv::RegisterSortedIndexFields {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        }),
        KvOp::DropSortedIndex { index_name } => kv::drop_sorted_index(index_name),
        KvOp::RegisterIndex {
            collection,
            field,
            field_position,
            backfill,
        } => kv::register_index(collection, field, *field_position, *backfill),
        KvOp::DropIndex { collection, field } => kv::drop_index(collection, field),
        KvOp::FieldSet {
            collection,
            key,
            updates,
            surrogate,
            ..
        } => kv::field_set(collection, key, updates, surrogate.as_u32()),
        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            ..
        } => kv::transfer(
            collection,
            source_key,
            dest_key,
            field,
            *amount,
            debit_surrogate.as_u32(),
            credit_surrogate.as_u32(),
        ),
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            ..
        } => kv::transfer_item(
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate.as_u32(),
        ),

        KvOp::Truncate { collection } => kv::truncate(collection),

        // Not a write — reads / scans / sorted-index queries.
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => return None,
    })
}
