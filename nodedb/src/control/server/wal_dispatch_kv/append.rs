// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of `KvOp` variants to WAL append calls.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::KvOp;

use super::encode::{
    KvRegisterSortedIndexFields, KvTransferFields, encode_kv_batch_put, encode_kv_cas,
    encode_kv_delete, encode_kv_drop_index, encode_kv_drop_sorted_index, encode_kv_expire,
    encode_kv_field_set, encode_kv_getset, encode_kv_incr, encode_kv_incr_float,
    encode_kv_insert_on_conflict_update, encode_kv_persist, encode_kv_put,
    encode_kv_register_index, encode_kv_register_sorted_index, encode_kv_transfer,
    encode_kv_transfer_item, encode_kv_truncate,
};

/// Outcome of [`wal_append_kv_op`]: the allocated WAL LSN (if a durable
/// record was appended) and, for a TTL-bearing write, the single wall-clock
/// instant this call resolved.
///
/// `resolved_now_ms` is the one value the durable WAL record and the live
/// Data-Plane apply must both use — resolving `now_ms` independently at WAL
/// append time and again at apply time lets the two disagree by the dispatch
/// latency (harmless day-to-day, but a crash between the two turns into
/// "replay recomputes `now_ms` at restart time", pushing a TTL's expiry
/// forward by the crash-to-restart delay). A plain struct rather than a
/// `(Option<Lsn>, Option<u64>)` tuple: the two `Option<u64>`/`Option<Lsn>`
/// fields are trivially swappable by position, and this outcome threads
/// through several more call sites on its way to the Data Plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvAppendOutcome {
    /// WAL LSN allocated for this write, or `None` for read-only / non-WAL ops.
    pub lsn: Option<crate::types::Lsn>,
    /// The wall-clock instant (ms since epoch) resolved for a TTL-bearing
    /// write's `expire_at_ms`. `None` for non-TTL writes and for ops that
    /// carry no TTL at all.
    pub resolved_now_ms: Option<u64>,
}

/// Resolve `now_ms` and the absolute expiry for a TTL-bearing write, exactly
/// once. Returns `(resolved_now_ms, expire_at_ms)`, both `None` when
/// `ttl_ms == 0` so the caller's encode call preserves the historical
/// no-TTL payload shape byte-for-byte.
///
/// `now_override` supplies the instant instead of this node's clock when it was
/// decided elsewhere and the durable record must carry that exact value — see
/// [`wal_append_kv_op`].
fn resolve_expiry(ttl_ms: u64, now_override: Option<u64>) -> (Option<u64>, Option<u64>) {
    if ttl_ms == 0 {
        (None, None)
    } else {
        let now_ms = now_override.unwrap_or_else(crate::engine::kv::current_ms);
        (Some(now_ms), Some(now_ms + ttl_ms))
    }
}

/// Serialize a KV operation and append to the WAL.
///
/// Returns the appended write's WAL LSN (`Some`) for KV writes, or `None` for
/// read-only / non-WAL KV ops, alongside the resolved TTL instant (if any) —
/// see [`KvAppendOutcome`].
///
/// `now_override` pins a TTL-bearing write's `expire_at_ms` to an instant
/// decided elsewhere instead of this node's clock. `Some` only when the durable
/// record must carry that exact value: a Raft-committed entry carries the
/// instant the proposing node resolved, and every replica's redo record — like
/// every replica's live apply — must install it verbatim, or a replica's WAL
/// replay resurrects a different `expire_at_ms` than its peers.
pub fn wal_append_kv_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &KvOp,
    now_override: Option<u64>,
) -> crate::Result<KvAppendOutcome> {
    let mut resolved_now_ms: Option<u64> = None;
    let lsn: Option<crate::types::Lsn> = match op {
        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        }
        | KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        }
        | KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
            ..
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let entry = encode_kv_put(
                collection,
                key,
                value,
                *ttl_ms,
                expire_at_ms,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate: _,
            // The compiled RLS predicate is a property of the requesting
            // session, not of the row, so it stays out of the durable record —
            // a replay re-applies an already-admitted write.
            rls_write_check: _,
            // The projection is answered from the Data Plane's response, not
            // from the journal: a replay re-applies the row, it does not answer
            // a client. Both slots stay out of the durable record.
            returning: _,
            rls_filters: _,
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let entry = encode_kv_insert_on_conflict_update(
                collection,
                key,
                value,
                *ttl_ms,
                updates,
                expire_at_ms,
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Delete {
            collection, keys, ..
        } => {
            let entry = encode_kv_delete(collection, keys)?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates,
            ..
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let raw: Vec<u32> = surrogates.iter().map(|s| s.as_u32()).collect();
            let entry = encode_kv_batch_put(collection, entries, *ttl_ms, expire_at_ms, &raw)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Expire {
            collection,
            key,
            ttl_ms,
            ..
        } => {
            // Unlike `Put`/`BatchPut`, `Expire` has no "no TTL" sentinel for
            // `ttl_ms == 0` (see `encode_kv_expire`'s doc comment) — the
            // absolute instant is always resolved, so this deliberately does
            // not route through `resolve_expiry`, which returns `None` on
            // `ttl_ms == 0` for the Put family's different semantics.
            let now_ms = now_override.unwrap_or_else(crate::engine::kv::current_ms);
            let expire_at_ms = now_ms + *ttl_ms;
            resolved_now_ms = Some(now_ms);
            let entry = encode_kv_expire(collection, key, *ttl_ms, expire_at_ms)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Persist {
            collection, key, ..
        } => {
            let entry = encode_kv_persist(collection, key)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::RegisterIndex {
            collection,
            field,
            field_position,
            backfill,
        } => {
            let entry = encode_kv_register_index(collection, field, *field_position, *backfill)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::DropIndex { collection, field } => {
            let entry = encode_kv_drop_index(collection, field)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::FieldSet {
            collection,
            key,
            updates,
            surrogate,
            ..
        } => {
            let entry = encode_kv_field_set(collection, key, updates, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
            ..
        } => {
            let (now_ms, expire_at_ms) = resolve_expiry(*ttl_ms, now_override);
            resolved_now_ms = now_ms;
            let entry = encode_kv_incr(
                collection,
                key,
                *delta,
                *ttl_ms,
                surrogate.as_u32(),
                expire_at_ms,
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
            ..
        } => {
            let entry = encode_kv_incr_float(collection, key, *delta, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
            ..
        } => {
            let entry = encode_kv_cas(collection, key, expected, new_value, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate,
            ..
        } => {
            let entry = encode_kv_getset(collection, key, new_value, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::RegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => {
            let entry = encode_kv_register_sorted_index(KvRegisterSortedIndexFields {
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms: *window_start_ms,
                window_end_ms: *window_end_ms,
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::DropSortedIndex { index_name } => {
            let entry = encode_kv_drop_sorted_index(index_name)?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Truncate { collection } => {
            let entry = encode_kv_truncate(collection)?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            ..
        } => {
            let entry = encode_kv_transfer(KvTransferFields {
                collection,
                source_key,
                dest_key,
                field,
                amount: *amount,
                debit_surrogate: debit_surrogate.as_u32(),
                credit_surrogate: credit_surrogate.as_u32(),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            ..
        } => {
            let entry = encode_kv_transfer_item(
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate.as_u32(),
            )?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Read-only or non-WAL KV ops.
        KvOp::Get { .. }
        | KvOp::BatchGet { .. }
        | KvOp::Scan { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::MaterializeScan { .. } => None,
    };
    Ok(KvAppendOutcome {
        lsn,
        resolved_now_ms,
    })
}
