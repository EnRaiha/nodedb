// SPDX-License-Identifier: BUSL-1.1

//! WAL append for KV engine operations.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::KvOp;

/// Serialize a KV operation and append to the WAL.
///
/// Returns `true` if the operation was handled (i.e. it is a KV write),
/// `false` if the caller should continue matching other plan variants.
pub fn wal_append_kv_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &KvOp,
) -> crate::Result<bool> {
    match op {
        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
        } => {
            let entry = zerompk::to_msgpack_vec(&("kv_put", collection, key, value, ttl_ms))
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv put: {e}"),
                })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
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
            let entry = zerompk::to_msgpack_vec(&("kv_put", collection, key, value, ttl_ms))
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv insert-like put: {e}"),
                })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::Delete { collection, keys } => {
            let entry = zerompk::to_msgpack_vec(&("kv_delete", collection, keys)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates: _,
        } => {
            let entry = zerompk::to_msgpack_vec(&("kv_batch_put", collection, entries, ttl_ms))
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv batch put: {e}"),
                })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::Expire {
            collection,
            key,
            ttl_ms,
        } => {
            let entry =
                zerompk::to_msgpack_vec(&("kv_expire", collection, key, ttl_ms)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv expire: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::Persist { collection, key } => {
            let entry = zerompk::to_msgpack_vec(&("kv_persist", collection, key)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv persist: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::RegisterIndex {
            collection,
            field,
            field_position,
            backfill: _,
        } => {
            let entry =
                zerompk::to_msgpack_vec(&("kv_register_index", collection, field, field_position))
                    .map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv register index: {e}"),
                    })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::DropIndex { collection, field } => {
            let entry =
                zerompk::to_msgpack_vec(&("kv_drop_index", collection, field)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv drop index: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::FieldSet {
            collection,
            key,
            updates,
            surrogate,
        } => {
            let entry = zerompk::to_msgpack_vec(&(
                "kv_field_set",
                collection,
                key,
                updates,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal kv field set: {e}"),
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
        } => {
            let entry = zerompk::to_msgpack_vec(&(
                "kv_incr",
                collection,
                key,
                delta,
                ttl_ms,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal kv incr: {e}"),
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
        } => {
            let entry = zerompk::to_msgpack_vec(&(
                "kv_incr_float",
                collection,
                key,
                delta,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal kv incr_float: {e}"),
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
        } => {
            let entry = zerompk::to_msgpack_vec(&(
                "kv_cas",
                collection,
                key,
                expected,
                new_value,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal kv cas: {e}"),
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate,
        } => {
            let entry = zerompk::to_msgpack_vec(&(
                "kv_getset",
                collection,
                key,
                new_value,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal kv getset: {e}"),
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
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
            let entry = zerompk::to_msgpack_vec(&(
                "kv_register_sorted_index",
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms,
                window_end_ms,
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal kv register sorted index: {e}"),
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::DropSortedIndex { index_name } => {
            let entry =
                zerompk::to_msgpack_vec(&("kv_drop_sorted_index", index_name)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv drop sorted index: {e}"),
                    }
                })?;
            wal.append_delete(tenant_id, vshard_id, database_id, &entry)?;
        }
        KvOp::Truncate { collection } => {
            let entry = zerompk::to_msgpack_vec(&("kv_truncate", collection)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv truncate: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, database_id, &entry)?;
        }
        // Read-only or non-WAL KV ops.
        KvOp::Get { .. }
        | KvOp::BatchGet { .. }
        | KvOp::Scan { .. }
        | KvOp::FieldGet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. }
        | KvOp::GetTtl { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::MaterializeScan { .. } => {
            return Ok(false);
        }
    }
    Ok(true)
}
