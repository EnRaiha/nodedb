// SPDX-License-Identifier: BUSL-1.1

//! Pure payload encoders for KV WAL records.

use nodedb_physical::physical_plan::UpdateValue;

/// Serialize `value` to a MessagePack WAL payload, wrapping any encode error
/// into a `crate::Error::Serialization` tagged with `context`.
fn encode<T: zerompk::ToMessagePack>(context: &str, value: &T) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal kv {context}: {e}"),
    })
}

/// Encode a `kv_put` WAL payload in the shape the KV replay path decodes:
/// `("kv_put", collection, key, value, ttl_ms, expire_at_ms, surrogate)`.
///
/// `expire_at_ms` is the absolute instant the Control Plane resolved, carried
/// verbatim so replay need not recompute `now_ms + ttl_ms` (which would drift
/// the expiry forward by the crash-to-restart delay); `None` means the write
/// carried no TTL. `surrogate` is the row's stable cross-engine identity.
///
/// The surrogate travels in the record because replay runs on a Data Plane core
/// with no Control Plane catalog handle: without it a replayed row lands with
/// identity `0`, which `KvEngine::key_for_surrogate` cannot resolve and which
/// the clone-snapshot visibility rule reads as "always visible". The KV
/// checkpoint already persists real surrogates, so a crash would otherwise
/// leave one table holding both kinds of row.
///
/// This seven-element shape is the only one written. Two shorter shapes remain
/// decodable on replay because a WAL tail written before the surrogate was
/// carried can still be retained across the upgrade; zerompk's strict
/// array-length check means the three never alias.
pub(crate) fn encode_kv_put(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    expire_at_ms: Option<u64>,
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    encode(
        "put",
        &(
            "kv_put",
            collection,
            key,
            value,
            ttl_ms,
            expire_at_ms,
            surrogate,
        ),
    )
}

/// Encode a `kv_insert_on_conflict_update` WAL payload in the shape the KV
/// replay path decodes.
///
/// This is a DELTA record, not a post-image: `value` is the pre-merge
/// incoming (`EXCLUDED`) row and `updates` carries the `DO UPDATE SET`
/// assignment inputs — the Control Plane cannot know the merged document
/// before dispatch. Replay re-reads whatever value is present in the KV
/// engine at that point in LSN order and re-runs the same
/// `apply_on_conflict_updates` merge the live handler uses, rather than
/// trusting a captured post-image. This is the same rationale as
/// [`encode_kv_field_set`], applied to the `INSERT ... ON CONFLICT DO
/// UPDATE` RMW instead of `HSET`-style field merge.
///
/// With `expire_at_ms = None` this produces the six-element tuple
/// `("kv_insert_on_conflict_update", collection, key, value, ttl_ms,
/// updates)`. With `Some(instant)` it appends the resolved absolute expiry
/// as a seventh element — the same additive, trailing-field convention
/// `encode_kv_put` uses — so replay installs the exact instant the Control
/// Plane resolved instead of recomputing `now_ms + ttl_ms` (which would
/// drift). zerompk's strict array-length check means the two shapes never
/// alias.
pub(crate) fn encode_kv_insert_on_conflict_update(
    collection: &str,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
    updates: &[(String, UpdateValue)],
    expire_at_ms: Option<u64>,
) -> crate::Result<Vec<u8>> {
    match expire_at_ms {
        None => encode(
            "insert on conflict update",
            &(
                "kv_insert_on_conflict_update",
                collection,
                key,
                value,
                ttl_ms,
                updates,
            ),
        ),
        Some(expire_at_ms) => encode(
            "insert on conflict update",
            &(
                "kv_insert_on_conflict_update",
                collection,
                key,
                value,
                ttl_ms,
                updates,
                expire_at_ms,
            ),
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

/// Encode a `kv_batch_put` WAL payload in the shape the KV replay path decodes:
/// `("kv_batch_put", collection, entries, ttl_ms, expire_at_ms, surrogates)`.
///
/// `surrogates` is positional against `entries` — one stable cross-engine
/// identity per entry, for the same reason [`encode_kv_put`] carries one. The
/// two shorter shapes stay decodable on replay for a tail written before the
/// surrogates were carried; zerompk's strict array-length check means the three
/// never alias.
pub(crate) fn encode_kv_batch_put(
    collection: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    ttl_ms: u64,
    expire_at_ms: Option<u64>,
    surrogates: &[u32],
) -> crate::Result<Vec<u8>> {
    encode(
        "batch put",
        &(
            "kv_batch_put",
            collection,
            entries,
            ttl_ms,
            expire_at_ms,
            surrogates,
        ),
    )
}

/// Encode a `kv_expire` WAL payload: `("kv_expire", collection, key, ttl_ms,
/// expire_at_ms)`.
///
/// Unlike `kv_put` / `kv_batch_put`, `kv_expire` has exactly one shape: `EXPIRE`
/// has no "no TTL" sentinel value for `ttl_ms` — `ttl_ms == 0` is a legitimate,
/// distinct request ("expire this key right now"), reachable through the
/// native-protocol builder, not a flag meaning "skip resolving an instant". So
/// the absolute instant is always resolved and always carried, and there was
/// never a historical shape without it: `replay_kv_wal` had no `kv_expire`
/// decode arm at all before this record gained one, so there is no prior
/// on-disk shape to stay compatible with.
pub(crate) fn encode_kv_expire(
    collection: &str,
    key: &[u8],
    ttl_ms: u64,
    expire_at_ms: u64,
) -> crate::Result<Vec<u8>> {
    encode(
        "expire",
        &("kv_expire", collection, key, ttl_ms, expire_at_ms),
    )
}

/// Encode a `kv_persist` WAL payload: `("kv_persist", collection, key)`.
pub(crate) fn encode_kv_persist(collection: &str, key: &[u8]) -> crate::Result<Vec<u8>> {
    encode("persist", &("kv_persist", collection, key))
}

/// Encode a `kv_register_index` WAL payload: `("kv_register_index",
/// collection, field, field_position, backfill)`.
///
/// `backfill` is a live-registration input, not a derivable fact: `true`
/// scans existing rows at registration time and populates the index, `false`
/// indexes only rows written afterwards. Replay must reproduce whichever the
/// user chose, so `backfill` travels in the record rather than being
/// inferred or defaulted at replay time.
pub(crate) fn encode_kv_register_index(
    collection: &str,
    field: &str,
    field_position: usize,
    backfill: bool,
) -> crate::Result<Vec<u8>> {
    encode(
        "register index",
        &(
            "kv_register_index",
            collection,
            field,
            field_position,
            backfill,
        ),
    )
}

/// Encode a `kv_drop_index` WAL payload: `("kv_drop_index", collection,
/// field)`.
pub(crate) fn encode_kv_drop_index(collection: &str, field: &str) -> crate::Result<Vec<u8>> {
    encode("drop index", &("kv_drop_index", collection, field))
}

/// Encode a `kv_incr` WAL payload in the shape the KV replay path decodes.
///
/// With `expire_at_ms = None` this produces the historical six-element tuple
/// `("kv_incr", collection, key, delta, ttl_ms, surrogate)` byte-for-byte —
/// `ttl_ms == 0` means "preserve whatever TTL the key already had" (see
/// `atomic_put`'s preserve branch), and there is no clock-derived instant to
/// carry for that case. With `Some(instant)` it appends the resolved
/// absolute expiry as a seventh element, the same additive trailing-field
/// convention `encode_kv_put` uses — recorded only when the live write's
/// `ttl_ms > 0`, so replay installs the exact instant the Control Plane
/// resolved instead of recomputing `now_ms + ttl_ms` (which would drift by
/// the crash-to-restart delay). Both shapes are genuinely produced in
/// production (one per `ttl_ms` case), so replay must decode both; zerompk's
/// strict array-length check means the two never alias.
pub(crate) fn encode_kv_incr(
    collection: &str,
    key: &[u8],
    delta: i64,
    ttl_ms: u64,
    surrogate: u32,
    expire_at_ms: Option<u64>,
) -> crate::Result<Vec<u8>> {
    match expire_at_ms {
        None => encode(
            "incr",
            &("kv_incr", collection, key, delta, ttl_ms, surrogate),
        ),
        Some(expire_at_ms) => encode(
            "incr",
            &(
                "kv_incr",
                collection,
                key,
                delta,
                ttl_ms,
                surrogate,
                expire_at_ms,
            ),
        ),
    }
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
    use nodedb_physical::physical_plan::UpdateValue;

    use super::{
        KvTransferFields, encode_kv_batch_put, encode_kv_cas, encode_kv_expire,
        encode_kv_field_set, encode_kv_getset, encode_kv_incr, encode_kv_incr_float,
        encode_kv_insert_on_conflict_update, encode_kv_put, encode_kv_register_index,
        encode_kv_transfer, encode_kv_transfer_item,
    };

    #[test]
    fn kv_put_carries_the_row_surrogate() {
        let entry = encode_kv_put("users", b"k1", b"v1", 5_000, None, 77).unwrap();

        let (disc, collection, key, value, ttl_ms, expire_at_ms, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
                &entry,
            )
            .unwrap();
        assert_eq!(disc, "kv_put");
        assert_eq!(collection, "users");
        assert_eq!(key, b"k1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, None);
        assert_eq!(
            surrogate, 77,
            "the row's cross-engine identity must survive a crash, not be \
             re-derived as zero on replay"
        );

        // Neither pre-surrogate shape may alias the current one — replay tries
        // all three and must never mistake one for another.
        assert!(zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&entry).is_err());
        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(&entry).is_err()
        );
    }

    #[test]
    fn kv_put_with_expire_at_carries_absolute_instant() {
        let entry =
            encode_kv_put("users", b"k1", b"v1", 5_000, Some(1_700_000_000_000), 9).unwrap();

        let (disc, collection, key, value, ttl_ms, expire_at_ms, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
                &entry,
            )
            .unwrap();
        assert_eq!(disc, "kv_put");
        assert_eq!(collection, "users");
        assert_eq!(key, b"k1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, Some(1_700_000_000_000));
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn kv_batch_put_carries_one_surrogate_per_entry() {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let entry = encode_kv_batch_put("users", &entries, 5_000, None, &[3, 4]).unwrap();

        let (disc, collection, decoded_entries, ttl_ms, expire_at_ms, surrogates) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<(Vec<u8>, Vec<u8>)>,
                u64,
                Option<u64>,
                Vec<u32>,
            )>(&entry)
            .unwrap();
        assert_eq!(disc, "kv_batch_put");
        assert_eq!(collection, "users");
        assert_eq!(decoded_entries, entries);
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, None);
        assert_eq!(
            surrogates,
            vec![3, 4],
            "surrogates are positional against entries"
        );

        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(&entry).is_err()
        );
        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64, u64)>(&entry)
                .is_err()
        );
    }

    #[test]
    fn kv_batch_put_with_expire_at_carries_absolute_instant() {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let entry = encode_kv_batch_put("users", &entries, 5_000, Some(1_700_000_000_000), &[3, 4])
            .unwrap();

        let (disc, collection, decoded_entries, ttl_ms, expire_at_ms, surrogates) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<(Vec<u8>, Vec<u8>)>,
                u64,
                Option<u64>,
                Vec<u32>,
            )>(&entry)
            .unwrap();
        assert_eq!(disc, "kv_batch_put");
        assert_eq!(collection, "users");
        assert_eq!(decoded_entries, entries);
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, Some(1_700_000_000_000));
        assert_eq!(surrogates, vec![3, 4]);
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
    fn kv_insert_on_conflict_update_without_expire_at_carries_updates() {
        let updates = vec![("score".to_string(), UpdateValue::Literal(b"42".to_vec()))];
        let entry =
            encode_kv_insert_on_conflict_update("players", b"p1", b"excluded", 0, &updates, None)
                .unwrap();

        let (disc, collection, key, value, ttl_ms, decoded_updates) = zerompk::from_msgpack::<(
            &str,
            String,
            Vec<u8>,
            Vec<u8>,
            u64,
            Vec<(String, UpdateValue)>,
        )>(&entry)
        .unwrap();
        assert_eq!(disc, "kv_insert_on_conflict_update");
        assert_eq!(collection, "players");
        assert_eq!(key, b"p1");
        assert_eq!(value, b"excluded");
        assert_eq!(ttl_ms, 0);
        assert_eq!(decoded_updates, updates);

        // The extended (with-expiry) shape must not alias this one.
        assert!(
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<u8>,
                Vec<u8>,
                u64,
                Vec<(String, UpdateValue)>,
                u64
            )>(&entry)
            .is_err(),
            "six-element payload must not decode as the seven-element tuple"
        );
    }

    #[test]
    fn kv_insert_on_conflict_update_with_expire_at_carries_absolute_instant() {
        let updates = vec![("score".to_string(), UpdateValue::Literal(b"42".to_vec()))];
        let entry = encode_kv_insert_on_conflict_update(
            "players",
            b"p1",
            b"excluded",
            5_000,
            &updates,
            Some(1_700_000_000_000),
        )
        .unwrap();

        let (disc, collection, key, value, ttl_ms, decoded_updates, expire_at_ms) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<u8>,
                Vec<u8>,
                u64,
                Vec<(String, UpdateValue)>,
                u64,
            )>(&entry)
            .unwrap();
        assert_eq!(disc, "kv_insert_on_conflict_update");
        assert_eq!(collection, "players");
        assert_eq!(key, b"p1");
        assert_eq!(value, b"excluded");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(decoded_updates, updates);
        assert_eq!(expire_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn kv_register_index_round_trips_backfill_flag() {
        let entry_backfill_true = encode_kv_register_index("players", "name", 2, true).unwrap();
        let (disc, collection, field, field_position, backfill) =
            zerompk::from_msgpack::<(&str, String, String, usize, bool)>(&entry_backfill_true)
                .unwrap();
        assert_eq!(disc, "kv_register_index");
        assert_eq!(collection, "players");
        assert_eq!(field, "name");
        assert_eq!(field_position, 2);
        assert!(backfill);

        let entry_backfill_false = encode_kv_register_index("players", "name", 2, false).unwrap();
        let (_, _, _, _, backfill_false) =
            zerompk::from_msgpack::<(&str, String, String, usize, bool)>(&entry_backfill_false)
                .unwrap();
        assert!(!backfill_false);

        // The two payloads must not be byte-identical: the backfill flag is
        // the only difference and it must actually change the encoded bytes.
        assert_ne!(entry_backfill_true, entry_backfill_false);
    }

    #[test]
    fn kv_expire_always_carries_the_resolved_absolute_instant() {
        let entry = encode_kv_expire("sessions", b"tok1", 5_000, 6_000).unwrap();

        let (disc, collection, key, ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, u64, u64)>(&entry).unwrap();
        assert_eq!(disc, "kv_expire");
        assert_eq!(collection, "sessions");
        assert_eq!(key, b"tok1");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, 6_000);
    }

    #[test]
    fn kv_expire_with_zero_ttl_still_carries_an_absolute_instant() {
        // ttl_ms == 0 is a legitimate "expire right now" request for EXPIRE,
        // not a "no TTL" sentinel the way it is for PUT — the shape must not
        // special-case it away.
        let entry = encode_kv_expire("sessions", b"tok2", 0, 1_234).unwrap();

        let (disc, collection, key, ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, u64, u64)>(&entry).unwrap();
        assert_eq!(disc, "kv_expire");
        assert_eq!(collection, "sessions");
        assert_eq!(key, b"tok2");
        assert_eq!(ttl_ms, 0);
        assert_eq!(expire_at_ms, 1_234);
    }

    #[test]
    fn kv_incr_without_expire_at_matches_historical_shape() {
        let entry = encode_kv_incr("counters", b"hits", 3, 0, 7, None).unwrap();

        // Byte-identical to the historical six-element tuple encoding.
        let expected =
            zerompk::to_msgpack_vec(&("kv_incr", "counters", b"hits", 3i64, 0u64, 7u32)).unwrap();
        assert_eq!(entry, expected);

        let (disc, collection, key, delta, ttl_ms, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, i64, u64, u32)>(&entry).unwrap();
        assert_eq!(disc, "kv_incr");
        assert_eq!(collection, "counters");
        assert_eq!(key, b"hits");
        assert_eq!(delta, 3);
        assert_eq!(ttl_ms, 0);
        assert_eq!(surrogate, 7);
    }

    #[test]
    fn kv_incr_with_expire_at_carries_absolute_instant() {
        let entry = encode_kv_incr(
            "counters",
            b"daily",
            1,
            86_400_000,
            9,
            Some(1_700_000_000_000),
        )
        .unwrap();

        let (disc, collection, key, delta, ttl_ms, surrogate, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, i64, u64, u32, u64)>(&entry).unwrap();
        assert_eq!(disc, "kv_incr");
        assert_eq!(collection, "counters");
        assert_eq!(key, b"daily");
        assert_eq!(delta, 1);
        assert_eq!(ttl_ms, 86_400_000);
        assert_eq!(surrogate, 9);
        assert_eq!(expire_at_ms, 1_700_000_000_000);

        // The historical six-element decode rejects the extended payload
        // (strict array-length check), so the two shapes never alias.
        assert!(
            zerompk::from_msgpack::<(&str, String, Vec<u8>, i64, u64, u32)>(&entry).is_err(),
            "extended payload must not decode as the six-element tuple"
        );
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
