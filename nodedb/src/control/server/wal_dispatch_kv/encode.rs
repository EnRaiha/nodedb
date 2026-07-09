// SPDX-License-Identifier: BUSL-1.1

//! Pure payload encoders for KV WAL records.

/// Serialize `value` to a MessagePack WAL payload, wrapping any encode error
/// into a `crate::Error::Serialization` tagged with `context`.
fn encode<T: zerompk::ToMessagePack>(context: &str, value: &T) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal kv {context}: {e}"),
    })
}

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
    match expire_at_ms {
        None => encode("put", &("kv_put", collection, key, value, ttl_ms)),
        Some(expire_at_ms) => encode(
            "put",
            &("kv_put", collection, key, value, ttl_ms, expire_at_ms),
        ),
    }
}

/// Fields of a `kv_transfer` WAL payload, bundled so [`encode_kv_transfer`]
/// stays under the `too_many_arguments` clippy threshold.
pub(crate) struct KvTransferFields<'a> {
    pub collection: &'a str,
    pub source_key: &'a [u8],
    pub dest_key: &'a [u8],
    pub field: &'a str,
    pub amount: f64,
    pub debit_surrogate: u32,
    pub credit_surrogate: u32,
}

/// Encode a `kv_transfer` delta WAL payload: `("kv_transfer", collection,
/// source_key, dest_key, field, amount, debit_surrogate, credit_surrogate)`.
///
/// This is a DELTA record, not a post-image: replay re-executes
/// `compute_transfer` against whatever source/dest values are present in the
/// KV engine at that point in the replay's LSN order (deterministic full
/// re-execution from empty), rather than trusting an absolute post-image
/// captured before dispatch.
pub(crate) fn encode_kv_transfer(f: KvTransferFields<'_>) -> crate::Result<Vec<u8>> {
    encode(
        "transfer",
        &(
            "kv_transfer",
            f.collection,
            f.source_key,
            f.dest_key,
            f.field,
            f.amount,
            f.debit_surrogate,
            f.credit_surrogate,
        ),
    )
}

/// Encode a `kv_transfer_item` delta WAL payload: `("kv_transfer_item",
/// source_collection, dest_collection, item_key, dest_key, surrogate)`.
///
/// Same delta-record rationale as [`encode_kv_transfer`]: replay re-verifies
/// source ownership and re-executes the delete+insert pair rather than
/// trusting a captured post-image.
pub(crate) fn encode_kv_transfer_item(
    source_collection: &str,
    dest_collection: &str,
    item_key: &[u8],
    dest_key: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "transfer item",
        &(
            "kv_transfer_item",
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
        ),
    )
}

/// Encode a `kv_cas` WAL payload: `("kv_cas", collection, key, expected,
/// new_value, surrogate)`.
///
/// This is a post-image-independent record: it carries the CAS inputs
/// (`expected`, `new_value`), not whether the compare succeeded live.
/// Replay re-runs the compare against whatever value is present in the KV
/// engine at that point in LSN order; a live-failed CAS replays to the same
/// no-op, and a live-succeeded CAS replays to the same write.
pub(crate) fn encode_kv_cas(
    collection: &str,
    key: &[u8],
    expected: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "cas",
        &("kv_cas", collection, key, expected, new_value, surrogate),
    )
}

/// Encode a `kv_incr_float` WAL payload: `("kv_incr_float", collection, key,
/// delta, surrogate)`.
///
/// Delta record: replay re-runs `incr_float` against whatever value is
/// present at that point in LSN order rather than trusting a captured
/// post-image.
pub(crate) fn encode_kv_incr_float(
    collection: &str,
    key: &[u8],
    delta: f64,
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "incr_float",
        &("kv_incr_float", collection, key, delta, surrogate),
    )
}

/// Encode a `kv_field_set` WAL payload: `("kv_field_set", collection, key,
/// updates, surrogate)`.
///
/// Delta record: `updates` carries the field-level inputs, not the
/// post-merge document. Replay re-reads whatever value is present in the KV
/// engine at that point in LSN order and re-runs the same
/// `merge_field_updates` computation the live handler uses, rather than
/// trusting a captured post-image.
pub(crate) fn encode_kv_field_set(
    collection: &str,
    key: &[u8],
    updates: &[(String, Vec<u8>)],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "field set",
        &("kv_field_set", collection, key, updates, surrogate),
    )
}

/// Encode a `kv_getset` WAL payload: `("kv_getset", collection, key,
/// new_value, surrogate)`.
pub(crate) fn encode_kv_getset(
    collection: &str,
    key: &[u8],
    new_value: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "getset",
        &("kv_getset", collection, key, new_value, surrogate),
    )
}

/// Encode a `kv_delete` WAL payload: `("kv_delete", collection, keys)`.
pub(crate) fn encode_kv_delete(collection: &str, keys: &[Vec<u8>]) -> crate::Result<Vec<u8>> {
    encode("delete", &("kv_delete", collection, keys))
}

/// Encode a `kv_batch_put` WAL payload: `("kv_batch_put", collection,
/// entries, ttl_ms)`.
pub(crate) fn encode_kv_batch_put(
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
) -> crate::Result<Vec<u8>> {
    encode("batch put", &("kv_batch_put", collection, entries, ttl_ms))
}

/// Encode a `kv_expire` WAL payload: `("kv_expire", collection, key,
/// ttl_ms)`.
pub(crate) fn encode_kv_expire(
    collection: &str,
    key: &[u8],
    ttl_ms: u64,
) -> crate::Result<Vec<u8>> {
    encode("expire", &("kv_expire", collection, key, ttl_ms))
}

/// Encode a `kv_persist` WAL payload: `("kv_persist", collection, key)`.
pub(crate) fn encode_kv_persist(collection: &str, key: &[u8]) -> crate::Result<Vec<u8>> {
    encode("persist", &("kv_persist", collection, key))
}

/// Encode a `kv_register_index` WAL payload: `("kv_register_index",
/// collection, field, field_position)`.
pub(crate) fn encode_kv_register_index(
    collection: &str,
    field: &str,
    field_position: usize,
) -> crate::Result<Vec<u8>> {
    encode(
        "register index",
        &("kv_register_index", collection, field, field_position),
    )
}

/// Encode a `kv_drop_index` WAL payload: `("kv_drop_index", collection,
/// field)`.
pub(crate) fn encode_kv_drop_index(collection: &str, field: &str) -> crate::Result<Vec<u8>> {
    encode("drop index", &("kv_drop_index", collection, field))
}

/// Encode a `kv_incr` WAL payload: `("kv_incr", collection, key, delta,
/// ttl_ms, surrogate)`.
pub(crate) fn encode_kv_incr(
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "incr",
        &("kv_incr", collection, key, delta, ttl_ms, surrogate),
    )
}

/// Fields of a `kv_register_sorted_index` WAL payload, bundled so
/// [`encode_kv_register_sorted_index`] stays under the `too_many_arguments`
/// clippy threshold.
pub(crate) struct KvRegisterSortedIndexFields<'a> {
    pub collection: &'a str,
    pub index_name: &'a str,
    pub sort_columns: &'a [(String, String)],
    pub key_column: &'a str,
    pub window_type: &'a str,
    pub window_timestamp_column: &'a str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

/// Encode a `kv_register_sorted_index` WAL payload: `("kv_register_sorted_index",
/// collection, index_name, sort_columns, key_column, window_type,
/// window_timestamp_column, window_start_ms, window_end_ms)`.
pub(crate) fn encode_kv_register_sorted_index(
    f: KvRegisterSortedIndexFields<'_>,
) -> crate::Result<Vec<u8>> {
    encode(
        "register sorted index",
        &(
            "kv_register_sorted_index",
            f.collection,
            f.index_name,
            f.sort_columns,
            f.key_column,
            f.window_type,
            f.window_timestamp_column,
            f.window_start_ms,
            f.window_end_ms,
        ),
    )
}

/// Encode a `kv_drop_sorted_index` WAL payload: `("kv_drop_sorted_index",
/// index_name)`.
pub(crate) fn encode_kv_drop_sorted_index(index_name: &str) -> crate::Result<Vec<u8>> {
    encode("drop sorted index", &("kv_drop_sorted_index", index_name))
}

/// Encode a `kv_truncate` WAL payload: `("kv_truncate", collection)`.
pub(crate) fn encode_kv_truncate(collection: &str) -> crate::Result<Vec<u8>> {
    encode("truncate", &("kv_truncate", collection))
}

#[cfg(test)]
mod tests {
    use super::{
        KvTransferFields, encode_kv_cas, encode_kv_field_set, encode_kv_getset,
        encode_kv_incr_float, encode_kv_put, encode_kv_transfer, encode_kv_transfer_item,
    };

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

    #[test]
    fn kv_transfer_encodes_delta_shape_with_both_surrogates() {
        let entry = encode_kv_transfer(KvTransferFields {
            collection: "accounts",
            source_key: b"alice",
            dest_key: b"bob",
            field: "balance",
            amount: 30.0,
            debit_surrogate: 7,
            credit_surrogate: 8,
        })
        .unwrap();

        let (
            disc,
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
        ) = zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, String, f64, u32, u32)>(
            &entry,
        )
        .unwrap();
        assert_eq!(disc, "kv_transfer");
        assert_eq!(collection, "accounts");
        assert_eq!(source_key, b"alice");
        assert_eq!(dest_key, b"bob");
        assert_eq!(field, "balance");
        assert_eq!(amount, 30.0);
        assert_eq!(debit_surrogate, 7);
        assert_eq!(credit_surrogate, 8);
    }

    #[test]
    fn kv_transfer_item_encodes_delta_shape_with_surrogate() {
        let entry =
            encode_kv_transfer_item("inventory", "trades", b"sword_1", b"sword_moved", 42).unwrap();

        let (disc, source_collection, dest_collection, item_key, dest_key, surrogate) =
            zerompk::from_msgpack::<(&str, String, String, Vec<u8>, Vec<u8>, u32)>(&entry).unwrap();
        assert_eq!(disc, "kv_transfer_item");
        assert_eq!(source_collection, "inventory");
        assert_eq!(dest_collection, "trades");
        assert_eq!(item_key, b"sword_1");
        assert_eq!(dest_key, b"sword_moved");
        assert_eq!(surrogate, 42);
    }

    #[test]
    fn kv_cas_encodes_expected_and_new_value_with_surrogate() {
        let entry = encode_kv_cas("state", b"p1", b"idle", b"in_match", 9).unwrap();

        let (disc, collection, key, expected, new_value, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, Vec<u8>, u32)>(&entry)
                .unwrap();
        assert_eq!(disc, "kv_cas");
        assert_eq!(collection, "state");
        assert_eq!(key, b"p1");
        assert_eq!(expected, b"idle");
        assert_eq!(new_value, b"in_match");
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn kv_incr_float_encodes_delta_with_surrogate() {
        let entry = encode_kv_incr_float("scores", b"dmg", 3.125, 5).unwrap();

        let (disc, collection, key, delta, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, f64, u32)>(&entry).unwrap();
        assert_eq!(disc, "kv_incr_float");
        assert_eq!(collection, "scores");
        assert_eq!(key, b"dmg");
        assert_eq!(delta, 3.125);
        assert_eq!(surrogate, 5);
    }

    #[test]
    fn kv_field_set_encodes_updates_with_surrogate() {
        let updates = vec![
            ("score".to_string(), b"42".to_vec()),
            ("name".to_string(), b"alice".to_vec()),
        ];
        let entry = encode_kv_field_set("players", b"p1", &updates, 11).unwrap();

        let (disc, collection, key, decoded_updates, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<(String, Vec<u8>)>, u32)>(&entry)
                .unwrap();
        assert_eq!(disc, "kv_field_set");
        assert_eq!(collection, "players");
        assert_eq!(key, b"p1");
        assert_eq!(decoded_updates, updates);
        assert_eq!(surrogate, 11);
    }

    #[test]
    fn kv_getset_encodes_new_value_with_surrogate() {
        let entry = encode_kv_getset("session", b"tok", b"new-token", 3).unwrap();

        let (disc, collection, key, new_value, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u32)>(&entry).unwrap();
        assert_eq!(disc, "kv_getset");
        assert_eq!(collection, "session");
        assert_eq!(key, b"tok");
        assert_eq!(new_value, b"new-token");
        assert_eq!(surrogate, 3);
    }
}
