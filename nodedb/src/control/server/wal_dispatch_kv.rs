// SPDX-License-Identifier: BUSL-1.1

//! WAL append for KV engine operations.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::KvOp;

/// Encode a `kv_put` WAL payload in the shape the KV replay path decodes.
///
/// With `expire_at_ms = None` this produces the historical five-element tuple
/// `("kv_put", collection, key, value, ttl_ms)` byte-for-byte, so the autocommit
/// path's on-disk format is unchanged. With `Some(instant)` it appends the
/// resolved absolute expiry as a sixth element — an additive, trailing field a
/// redo sub-record uses to carry the exact expiry instant, so replay need not
/// recompute `now_ms + ttl_ms` (which would drift). Payloads without the sixth
/// element remain valid; the relative `ttl_ms` is always retained.
pub(crate) fn encode_kv_put(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    expire_at_ms: Option<u64>,
) -> crate::Result<Vec<u8>> {
    let result = match expire_at_ms {
        None => zerompk::to_msgpack_vec(&("kv_put", collection, key, value, ttl_ms)),
        Some(expire_at_ms) => {
            zerompk::to_msgpack_vec(&("kv_put", collection, key, value, ttl_ms, expire_at_ms))
        }
    };
    result.map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal kv put: {e}"),
    })
}

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
            let entry = zerompk::to_msgpack_vec(&("kv_delete", collection, keys)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv delete: {e}"),
                }
            })?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Persist { collection, key } => {
            let entry = zerompk::to_msgpack_vec(&("kv_persist", collection, key)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv persist: {e}"),
                }
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::DropIndex { collection, field } => {
            let entry =
                zerompk::to_msgpack_vec(&("kv_drop_index", collection, field)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv drop index: {e}"),
                    }
                })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::DropSortedIndex { index_name } => {
            let entry =
                zerompk::to_msgpack_vec(&("kv_drop_sorted_index", index_name)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv drop sorted index: {e}"),
                    }
                })?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        KvOp::Truncate { collection } => {
            let entry = zerompk::to_msgpack_vec(&("kv_truncate", collection)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv truncate: {e}"),
                }
            })?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
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
        | KvOp::MaterializeScan { .. } => None,
    };
    Ok(lsn)
}

#[cfg(test)]
mod tests {
    use super::encode_kv_put;

    #[test]
    fn kv_put_without_expire_at_matches_historical_shape() {
        let entry = encode_kv_put("users", b"k1", b"v1", 5_000, None).unwrap();

        // Byte-identical to the historical five-element tuple encoding.
        let expected =
            zerompk::to_msgpack_vec(&("kv_put", "users", b"k1", b"v1", 5_000u64)).unwrap();
        assert_eq!(entry, expected);

        // Decodes with the KV replay path's five-element tuple.
        let (disc, collection, key, value, ttl_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&entry).unwrap();
        assert_eq!(disc, "kv_put");
        assert_eq!(collection, "users");
        assert_eq!(key, b"k1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 5_000);
    }

    #[test]
    fn kv_put_with_expire_at_carries_absolute_instant() {
        let entry = encode_kv_put("users", b"k1", b"v1", 5_000, Some(1_700_000_000_000)).unwrap();

        // The six-element tuple carries the resolved absolute expiry.
        let (disc, collection, key, value, ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(&entry).unwrap();
        assert_eq!(disc, "kv_put");
        assert_eq!(collection, "users");
        assert_eq!(key, b"k1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, 1_700_000_000_000);

        // The historical five-element decode rejects the extended payload
        // (strict array-length check), so the two shapes never alias.
        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&entry).is_err(),
            "extended payload must not decode as the five-element tuple"
        );
    }
}
