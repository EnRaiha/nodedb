// SPDX-License-Identifier: BUSL-1.1

/// Serializable snapshot of a tenant's Data Plane state.
///
/// Shared between Control Plane (backup/restore DDL) and Data Plane
/// (snapshot creation/restoration). Lives in `types` to avoid
/// cross-plane module visibility leaks.
///
/// Map-encoded (`#[msgpack(map)]`) so fields can be added with
/// `#[msgpack(default)]` and older snapshots (serialized before the field
/// existed) still decode without a migration — new fields appear with their
/// `Default` value. This is the same evolution pattern used by
/// `ContinuousAggregateDef` and `RetentionPolicyDef`.
#[derive(
    serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack, Default,
)]
#[msgpack(map)]
pub struct TenantDataSnapshot {
    /// Sparse engine documents: `[("{tid}:{collection}:{doc_id}", value_bytes), ...]`
    pub documents: Vec<(String, Vec<u8>)>,
    /// Sparse engine index entries: `[("{tid}:{collection}:{field}:{value}:{doc_id}", []), ...]`
    pub indexes: Vec<(String, Vec<u8>)>,
    /// Graph edges: `[("{tid}:{src}\x00{label}\x00{tid}:{dst}", properties), ...]`
    pub edges: Vec<(String, Vec<u8>)>,
    /// Vector collections: `[("{tid}:{collection}", serialized_vectors_msgpack), ...]`
    /// Each value is a MessagePack-serialized list of `(vector_id, f32_data, doc_id)`.
    /// HNSW graph is NOT serialized — it's rebuilt on restore from raw vectors.
    pub vectors: Vec<(String, Vec<u8>)>,
    /// KV tables: `[("{tid}:{collection}", serialized_entries_msgpack), ...]`
    /// Each value is a MessagePack-serialized list of `(key_bytes, value_bytes, expire_at_ms)`.
    pub kv_tables: Vec<(String, Vec<u8>)>,
    /// CRDT state: `[("{tid}", loro_export_bytes), ...]`
    pub crdt_state: Vec<(String, Vec<u8>)>,
    /// Timeseries memtable data: `[("{tid}:{collection}", serialized_columns_msgpack), ...]`
    pub timeseries: Vec<(String, Vec<u8>)>,
    /// Flushed on-disk timeseries segments per collection.
    ///
    /// `#[msgpack(default)]`: snapshots created before this field was added
    /// decode with an empty Vec — the restore path treats an empty slice as
    /// "no flushed segments to restore", which is safe and correct.
    #[msgpack(default)]
    #[serde(default)]
    pub flushed_ts_segments: Vec<TsFlushedCollectionBlob>,
}

/// Wire blob for all flushed partitions of one timeseries collection.
///
/// The `collection_key` uses the same `"{db}:{tid}:{collection}"` format
/// as the `timeseries` field's keys so the key parsers are shared.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Default,
)]
pub struct TsFlushedCollectionBlob {
    /// `"{database_id}:{tenant_id}:{collection}"` — the same scoped key format
    /// used throughout the timeseries snapshot fields.
    pub collection_key: String,
    /// One blob per flushed partition directory.
    pub partitions: Vec<TsFlushedPartitionBlob>,
}

/// Wire blob for one flushed partition directory.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Default,
)]
pub struct TsFlushedPartitionBlob {
    /// Directory name of the partition (e.g. `"ts-20240101-000000_20240102-000000"`).
    pub dir_name: String,
    /// Partition metadata — captured directly from `PartitionEntry::meta`.
    /// Embedded as a nested msgpack blob to avoid coupling `TsFlushedPartitionBlob`
    /// to `PartitionMeta`'s zerompk wire layout at the outer struct level.
    pub meta_bytes: Vec<u8>,
    /// All files in the partition directory: `(filename, raw_bytes)`.
    pub files: Vec<(String, Vec<u8>)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Backward-compat: a `TenantDataSnapshot` serialized WITHOUT the
    /// `flushed_ts_segments` field (simulating a snapshot from before this
    /// field was added) must decode successfully with `flushed_ts_segments`
    /// defaulting to `Vec::new()`.
    ///
    /// We simulate the "old" wire format by defining a 7-field map-encoded
    /// struct that matches the original schema, serialising it, then decoding
    /// as the new 8-field `TenantDataSnapshot`. zerompk's `#[msgpack(default)]`
    /// fills in the missing `flushed_ts_segments` with `Vec::new()`.
    #[test]
    fn backward_compat_missing_flushed_ts_segments_defaults_to_empty() {
        /// Mirrors the original 7-field schema — no `flushed_ts_segments`.
        #[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
        #[msgpack(map)]
        struct OldSnapshot {
            documents: Vec<(String, Vec<u8>)>,
            indexes: Vec<(String, Vec<u8>)>,
            edges: Vec<(String, Vec<u8>)>,
            vectors: Vec<(String, Vec<u8>)>,
            kv_tables: Vec<(String, Vec<u8>)>,
            crdt_state: Vec<(String, Vec<u8>)>,
            timeseries: Vec<(String, Vec<u8>)>,
        }

        let old = OldSnapshot {
            documents: vec![("k".to_string(), b"v".to_vec())],
            indexes: vec![],
            edges: vec![],
            vectors: vec![],
            kv_tables: vec![],
            crdt_state: vec![],
            timeseries: vec![("ts:c".to_string(), b"data".to_vec())],
        };
        let bytes = zerompk::to_msgpack_vec(&old).expect("encode old snapshot");

        // Decode as new schema — flushed_ts_segments must default to empty.
        let decoded: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode old snapshot as new schema");
        assert_eq!(decoded.documents.len(), 1);
        assert_eq!(decoded.timeseries.len(), 1);
        assert!(
            decoded.flushed_ts_segments.is_empty(),
            "expected flushed_ts_segments to default to empty for old snapshot"
        );
    }

    /// Blob round-trip: `TsFlushedCollectionBlob` with one partition containing
    /// fake files and meta_bytes survives msgpack encode → decode intact.
    #[test]
    fn flushed_collection_blob_round_trips() {
        let partition = TsFlushedPartitionBlob {
            dir_name: "ts-20240101-000000_20240102-000000".to_string(),
            meta_bytes: b"fake-meta".to_vec(),
            files: vec![
                ("schema.json".to_string(), b"{\"v\":1}".to_vec()),
                ("col_ts.col".to_string(), b"\x00\x01\x02".to_vec()),
            ],
        };
        let blob = TsFlushedCollectionBlob {
            collection_key: "1:42:metrics".to_string(),
            partitions: vec![partition],
        };

        let bytes = zerompk::to_msgpack_vec(&blob).expect("encode blob");
        let decoded: TsFlushedCollectionBlob = zerompk::from_msgpack(&bytes).expect("decode blob");

        assert_eq!(decoded.collection_key, "1:42:metrics");
        assert_eq!(decoded.partitions.len(), 1);
        let p = &decoded.partitions[0];
        assert_eq!(p.dir_name, "ts-20240101-000000_20240102-000000");
        assert_eq!(p.meta_bytes, b"fake-meta");
        assert_eq!(p.files.len(), 2);
        assert_eq!(p.files[0].0, "schema.json");
        assert_eq!(p.files[1].0, "col_ts.col");
        assert_eq!(p.files[1].1, b"\x00\x01\x02");
    }
}
