// SPDX-License-Identifier: BUSL-1.1

//! Canonical rows of the KV, vector, timeseries, columnar and CRDT sections.
//!
//! Timeseries and columnar rows decode with the same decoders RESTORE re-issues
//! them from, so a row restore writes hashes the same on both sides.

use std::collections::BTreeMap;

use loro::{LoroDoc, LoroValue};
use nodedb_types::backup_envelope::VerifiedPart;
use nodedb_types::surrogate::Surrogate;

use crate::Error;
use crate::control::backup::restore::columnar_reissue::decode_snapshot_live_rows;
use crate::control::backup::restore::timeseries_reissue::decode_timeseries_live_rows;
use crate::control::backup::restore::vector_reissue::split_vector_coll_key;
use crate::types::TsFlushedCollectionBlob;

use super::canonical::{Row, RowHasher, key_hash};
use super::walk::Walk;

/// One KV table's rows, in the KV snapshot shape.
type KvRows = Vec<crate::engine::kv::hash_table::KvSnapshotRow>;

/// One vector index's export rows: `(node_id, vector, surrogate)`.
type VectorRows = Vec<(u32, Vec<f32>, Option<Surrogate>)>;

/// How a vector row is keyed.
const VECTOR_BOUND: u8 = b'p';
/// A vector with no surrogate. Every stored vector is bound, so a capture that
/// holds one is corrupt, and RESTORE refuses it (`group_restored_vectors`).
const VECTOR_UNBOUND: u8 = b'u';
/// A vector whose surrogate binds no primary key.
const VECTOR_UNKEYED: u8 = b's';

fn decode_error(what: &str, collection: &str, e: impl std::fmt::Display) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("backup verification: decode {what} of '{collection}': {e}"),
    }
}

impl Walk<'_> {
    pub(super) fn kv_tables(
        &self,
        tables: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (table_key, bytes) in tables {
            let bare = self.bare(self.scoped_rest(table_key)?);
            let rows: KvRows =
                zerompk::from_msgpack(bytes).map_err(|e| decode_error("KV table", &bare, e))?;
            // The surrogate is not hashed: RESTORE binds each row in the
            // destination catalog, so source and destination identities differ.
            for (key, value, expire_at_ms, _surrogate) in rows {
                let row_key: [&[u8]; 1] = [key.as_slice()];
                let mut hasher = RowHasher::new(VerifiedPart::KeyValue, &row_key);
                hasher.bytes(b'v', &value);
                sink(Row {
                    collection: bare.clone(),
                    part: VerifiedPart::KeyValue,
                    key: Some(key_hash(VerifiedPart::KeyValue, &row_key)),
                    hash: hasher.finish(),
                    expire_at_ms,
                });
            }
        }
        Ok(())
    }

    pub(super) fn vectors(
        &self,
        indexes: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (index_key, bytes) in indexes {
            let (stored, field) = split_vector_coll_key(self.scoped_rest(index_key)?);
            let bare = self.bare(stored);
            let rows: VectorRows =
                zerompk::from_msgpack(bytes).map_err(|e| decode_error("vector index", &bare, e))?;
            for (_node_id, vector, surrogate) in rows {
                let bound = surrogate
                    .filter(|s| *s != Surrogate::ZERO)
                    .map(|s| self.binds.pk(&bare, s.as_u32()));
                let (kind, pk): (u8, &[u8]) = match bound {
                    Some(Some(pk)) => (VECTOR_BOUND, pk),
                    None => (VECTOR_UNBOUND, &[]),
                    Some(None) => (VECTOR_UNKEYED, &[]),
                };
                let kind = [kind];
                let key: [&[u8]; 3] = [field.as_bytes(), &kind, pk];
                let mut hasher = RowHasher::new(VerifiedPart::Vectors, &key);
                let data: Vec<u8> = vector.iter().flat_map(|v| v.to_le_bytes()).collect();
                hasher.bytes(b'f', &data);
                sink(Row {
                    collection: bare.clone(),
                    part: VerifiedPart::Vectors,
                    key: Some(key_hash(VerifiedPart::Vectors, &key)),
                    hash: hasher.finish(),
                    expire_at_ms: 0,
                });
            }
        }
        Ok(())
    }

    /// Each collection's memtable rows plus every flushed partition's rows.
    pub(super) fn timeseries(
        &self,
        memtables: &[(String, Vec<u8>)],
        flushed: &[TsFlushedCollectionBlob],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        let mut by_key: BTreeMap<&str, (Option<&[u8]>, Option<&TsFlushedCollectionBlob>)> =
            BTreeMap::new();
        for (key, bytes) in memtables {
            by_key.entry(key.as_str()).or_default().0 = Some(bytes.as_slice());
        }
        for blob in flushed {
            by_key.entry(blob.collection_key.as_str()).or_default().1 = Some(blob);
        }
        let empty = TsFlushedCollectionBlob::default();
        for (key, (memtable, flushed)) in by_key {
            let bare = self.bare(self.scoped_rest(key)?);
            let rows =
                decode_timeseries_live_rows(&bare, memtable, flushed.unwrap_or(&empty), self.kek)?;
            for row in rows {
                let mut hasher = RowHasher::new(VerifiedPart::Timeseries, &[]);
                hasher.value(&row)?;
                sink(keyless(bare.clone(), VerifiedPart::Timeseries, hasher));
            }
        }
        Ok(())
    }

    pub(super) fn columnar(
        &self,
        engines: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (key, bytes) in engines {
            let bare = self.bare(self.scoped_rest(key)?);
            let snap: nodedb_columnar::ColumnarEngineSnapshot = zerompk::from_msgpack(bytes)
                .map_err(|e| decode_error("columnar snapshot", &bare, e))?;
            let decoded = decode_snapshot_live_rows(&bare, snap, self.kek)?;
            for row in decoded.rows {
                let mut hasher = RowHasher::new(VerifiedPart::Columnar, &[]);
                hasher.value(&row)?;
                sink(keyless(bare.clone(), VerifiedPart::Columnar, hasher));
            }
        }
        Ok(())
    }

    /// Each collection's Loro document, one row per entry of each root map.
    pub(super) fn crdt(
        &self,
        states: &[(u64, u64, String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (_database_id, _tenant_id, stored, bytes) in states {
            let bare = self.bare(stored);
            let doc = LoroDoc::new();
            doc.import(bytes)
                .map_err(|e| decode_error("CRDT state", &bare, e))?;
            let mut emit = |key: &[&[u8]], value: &LoroValue| {
                let mut hasher = RowHasher::new(VerifiedPart::Crdt, key);
                hasher.loro(value);
                sink(Row {
                    collection: bare.clone(),
                    part: VerifiedPart::Crdt,
                    key: Some(key_hash(VerifiedPart::Crdt, key)),
                    hash: hasher.finish(),
                    expire_at_ms: 0,
                });
            };
            match doc.get_deep_value() {
                LoroValue::Map(roots) => {
                    for (root, value) in roots.iter() {
                        match value {
                            LoroValue::Map(rows) => {
                                for (id, row) in rows.iter() {
                                    emit(&[root.as_bytes(), id.as_bytes()][..], row);
                                }
                            }
                            other => emit(&[root.as_bytes()][..], other),
                        }
                    }
                }
                other => emit(&[][..], &other),
            }
        }
        Ok(())
    }
}

fn keyless(collection: String, part: VerifiedPart, hasher: RowHasher) -> Row {
    Row {
        collection,
        part,
        key: None,
        hash: hasher.finish(),
        expire_at_ms: 0,
    }
}
