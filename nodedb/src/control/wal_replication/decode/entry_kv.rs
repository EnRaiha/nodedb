// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Kv`.
//!
//! Delegated from `decode/entry.rs`'s single grouped match arm. Unlike the
//! other engine groups this one returns `(PhysicalPlan, Option<u64>)`: the
//! seven TTL-bearing `Kv*` variants stamp `resolved_now_ms` from their wire
//! field so every replica installs the identical `expire_at_ms` instead of
//! reading its own wall clock at apply time. Every non-TTL arm leaves it
//! `None`. `write` is guaranteed by the caller to already be one of these
//! variants — see `entry_document::decode_arm` for the trailing-arm contract.

use super::super::types::ReplicatedWrite;
use super::ctx::DecodeCtx;
use super::kv;
use crate::bridge::envelope::PhysicalPlan;

pub(super) fn decode_arm(
    ctx: &DecodeCtx,
    write: &ReplicatedWrite,
) -> crate::Result<(PhysicalPlan, Option<u64>)> {
    let mut resolved_now_ms: Option<u64> = None;
    let plan = match write {
        ReplicatedWrite::KvTruncate { collection } => kv::truncate(collection),
        ReplicatedWrite::KvPut {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::put(ctx, collection, key, value, *ttl_ms, *surrogate)?
        }
        ReplicatedWrite::KvDelete { collection, keys } => kv::delete(collection, keys),
        ReplicatedWrite::KvInsert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::insert(ctx, collection, key, value, *ttl_ms, *surrogate)?
        }
        ReplicatedWrite::KvInsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::insert_if_absent(ctx, collection, key, value, *ttl_ms, *surrogate)?
        }
        ReplicatedWrite::KvInsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::insert_on_conflict_update(
                ctx, collection, key, value, *ttl_ms, updates, *surrogate,
            )?
        }
        ReplicatedWrite::KvBatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::batch_put(ctx, collection, entries, *ttl_ms, surrogates)?
        }
        ReplicatedWrite::KvExpire {
            collection,
            key,
            ttl_ms,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::expire(collection, key, *ttl_ms)
        }
        ReplicatedWrite::KvPersist { collection, key } => kv::persist(collection, key),
        ReplicatedWrite::KvIncr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
            resolved_now_ms: rn,
        } => {
            resolved_now_ms = *rn;
            kv::incr(ctx, collection, key, *delta, *ttl_ms, *surrogate)?
        }
        ReplicatedWrite::KvIncrFloat {
            collection,
            key,
            delta,
            surrogate,
        } => kv::incr_float(ctx, collection, key, *delta, *surrogate)?,
        ReplicatedWrite::KvCas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
        } => kv::cas(ctx, collection, key, expected, new_value, *surrogate)?,
        ReplicatedWrite::KvGetSet {
            collection,
            key,
            new_value,
            surrogate,
        } => kv::get_set(ctx, collection, key, new_value, *surrogate)?,
        ReplicatedWrite::KvRegisterSortedIndex {
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
        ReplicatedWrite::KvDropSortedIndex { index_name } => kv::drop_sorted_index(index_name),
        ReplicatedWrite::KvRegisterIndex {
            collection,
            field,
            field_position,
            backfill,
        } => kv::register_index(collection, field, *field_position, *backfill),
        ReplicatedWrite::KvDropIndex { collection, field } => kv::drop_index(collection, field),
        ReplicatedWrite::KvFieldSet {
            collection,
            key,
            updates,
            surrogate,
        } => kv::field_set(ctx, collection, key, updates, *surrogate)?,
        ReplicatedWrite::KvTransfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
        } => kv::transfer(
            ctx,
            kv::TransferFields {
                collection,
                source_key,
                dest_key,
                field,
                amount: *amount,
                debit_surrogate: *debit_surrogate,
                credit_surrogate: *credit_surrogate,
            },
        )?,
        ReplicatedWrite::KvTransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
        } => kv::transfer_item(
            ctx,
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            *surrogate,
        )?,
        _ => {
            return Err(crate::Error::Internal {
                detail: "entry_kv::decode_arm called with a non-Kv ReplicatedWrite variant \
                    (dispatch bug in decode/entry.rs's grouped Kv match arm)"
                    .into(),
            });
        }
    };
    Ok((plan, resolved_now_ms))
}
