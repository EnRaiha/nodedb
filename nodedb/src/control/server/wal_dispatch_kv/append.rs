// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of `KvOp` variants to WAL append calls.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::KvOp;

use super::encode::{
    KvRegisterSortedIndexFields, KvTransferFields, encode_kv_batch_put, encode_kv_cas,
    encode_kv_delete, encode_kv_drop_index, encode_kv_drop_sorted_index, encode_kv_expire,
    encode_kv_field_set, encode_kv_getset, encode_kv_incr, encode_kv_incr_float, encode_kv_persist,
    encode_kv_put, encode_kv_register_index, encode_kv_register_sorted_index, encode_kv_transfer,
    encode_kv_transfer_item, encode_kv_truncate,
};

/// Serialize a KV operation and append to the WAL.
///
/// Returns the appended write's WAL LSN (`Some`) for KV writes, or `None` for
/// read-only / non-WAL KV ops.
pub fn wal_append_kv_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &KvOp,
) -> crate::Result<Option<crate::types::Lsn>> {
    let lsn: Option<crate::types::Lsn> = match op {
        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
        } => {
            let entry = encode_kv_put(collection, key, value, *ttl_ms, None)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
        }
        | KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
        }
        | KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates: _,
            surrogate: _,
        } => {
            let entry = encode_kv_put(collection, key, value, *ttl_ms, None)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Delete { collection, keys } => {
            let entry = encode_kv_delete(collection, keys)?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates: _,
        } => {
            let entry = encode_kv_batch_put(collection, entries, *ttl_ms)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Expire {
            collection,
            key,
            ttl_ms,
        } => {
            let entry = encode_kv_expire(collection, key, *ttl_ms)?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Persist { collection, key } => {
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
        } => {
            let entry = encode_kv_incr(collection, key, *delta, *ttl_ms, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
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
        } => {
            let entry = encode_kv_cas(collection, key, expected, new_value, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate,
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
    Ok(lsn)
}
