// SPDX-License-Identifier: BUSL-1.1

//! Construction of the MERGE source side: `join_val → source_document`.
//!
//! Separate from the target scan and from every apply pass because this is the
//! one place two physically different sources — a local storage read and rows
//! shipped across cores by the Control Plane — must be proven to produce the
//! same map. Both go through a single decode closure here; splitting them
//! across files is how they drift, and a source map that differs by even one
//! key silently reclassifies matched rows as NOT MATCHED.

use redb::ReadableDatabase;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;

use super::super::merge_helpers::json_to_str;

impl CoreLoop {
    /// Resolve a collection's strict Binary-Tuple schema, if it is a strict
    /// document collection. `None` for schemaless collections.
    pub(in crate::data::executor) fn merge_strict_schema(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<nodedb_types::columnar::StrictSchema> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }

    /// Build the source join map `join_val → document`.
    ///
    /// Two sources of source rows, selected by `source_rows`:
    /// - `Some(rows)` (cross-core): the Control Plane scanned the source on its
    ///   OWN Data-Plane core and shipped the RAW stored bytes here. This core
    ///   does not hold the source's storage, but `Register` is broadcast so it
    ///   DOES hold the source's strict schema — the shipped bytes are decoded
    ///   with the exact same schema-aware logic the local scan uses, so the
    ///   resulting map is byte-for-byte identical to a co-resident local read.
    /// - `None` (legacy co-resident / in-txn buffered replay): read the source
    ///   from this core's local storage.
    pub(in crate::data::executor) fn build_merge_source_map(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        join_col: &str,
        source_rows: Option<&[(String, Vec<u8>)]>,
    ) -> crate::Result<std::collections::HashMap<String, serde_json::Value>> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Decode one raw stored source document and extract its non-empty join
        // key. Shared by the shipped-rows path and the local-scan path so both
        // derive an identical `join_val → document` mapping from identical bytes.
        //
        // `Ok(None)` is the domain answer "this source row has no usable join
        // key"; a body that will not decode is not that answer. Dropping it
        // silently would leave its key unmatched, so the MERGE would classify
        // matched target rows as NOT MATCHED and insert duplicates.
        let decode_and_key =
            |value_bytes: &[u8]| -> crate::Result<Option<(String, serde_json::Value)>> {
                let doc = doc_format::decode_document_or_binary_tuple(
                    value_bytes,
                    strict_schema.as_ref(),
                    "MERGE source row",
                )?;
                let key = doc.get(join_col).map(json_to_str).unwrap_or_default();
                if key.is_empty() {
                    return Ok(None);
                }
                Ok(Some((key, doc)))
            };

        let mut map = std::collections::HashMap::new();

        if let Some(rows) = source_rows {
            for (_source_doc_id, value_bytes) in rows {
                if let Some((key, doc)) = decode_and_key(value_bytes)? {
                    map.insert(key, doc);
                }
            }
            return Ok(map);
        }

        let prefix = crate::engine::sparse::btree::coll_prefix(database_id, tid, collection);
        let end = format!("{prefix}\u{ffff}");

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("read txn for merge source: {e}"),
            })?;
        let table = read_txn
            .open_table(crate::engine::sparse::btree::DOCUMENTS)
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("open merge source table: {e}"),
            })?;

        if let Ok(range) = table.range(prefix.as_str()..end.as_str()) {
            for entry in range.flatten() {
                if let Some((key, doc)) = decode_and_key(entry.1.value())? {
                    map.insert(key, doc);
                }
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use nodedb_types::Value;

    const DB: u64 = 0;
    const TID: u64 = 1;
    const SRC: &str = "merge_src";
    const JOIN: &str = "id";

    /// Build a schemaless source doc as the RAW stored bytes a plain insert
    /// would write (`nodedb_types::Value` msgpack).
    fn src_doc(id: &str, name: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert("id".to_string(), Value::String(id.into()));
        obj.insert("name".to_string(), Value::String(name.into()));
        nodedb_types::value_to_msgpack(&Value::Object(obj)).unwrap()
    }

    /// Write raw schemaless docs directly into a core's sparse DOCUMENTS table,
    /// mirroring the on-disk shape `build_merge_source_map`'s local scan reads.
    fn seed_source(core: &CoreLoop, rows: &[(&str, Vec<u8>)]) {
        use crate::engine::sparse::btree::{DOCUMENTS, coll_prefix};
        let prefix = coll_prefix(DB, TID, SRC);
        let txn = core.sparse.db().begin_write().unwrap();
        {
            let mut table = txn.open_table(DOCUMENTS).unwrap();
            for (doc_id, bytes) in rows {
                let key = format!("{prefix}{doc_id}");
                table.insert(key.as_str(), bytes.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    /// Cross-core MERGE source-shipping: the join-map the Data Plane builds from
    /// Control-Plane-shipped source rows on a core that does NOT hold the source
    /// locally is IDENTICAL to the map a co-resident local read produces — and
    /// WITHOUT the shipped rows that same non-owning core reads an empty map,
    /// which is exactly the silent-wrong-result the source-ship path fixes.
    #[test]
    fn shipped_source_rows_match_local_join_map() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let (core_a, _tx_a, _rx_a) = make_core_with_dir(dir_a.path());
        let (core_b, _tx_b, _rx_b) = make_core_with_dir(dir_b.path());

        let docs = vec![
            ("d1", src_doc("k1", "alpha")),
            ("d2", src_doc("k2", "bravo")),
            ("d3", src_doc("k3", "charlie")),
        ];
        // The source collection lives ONLY on core A (its owning core).
        seed_source(&core_a, &docs);

        // Co-resident (legacy) path on core A: read the source locally.
        let map_local = core_a
            .build_merge_source_map(DB, TID, SRC, JOIN, None)
            .unwrap();
        assert_eq!(map_local.len(), 3, "local read must see all source rows");

        // Cross-core: core B does NOT hold the source. A local read there is
        // empty — the exact silent-wrong-result the guard used to fail-close on.
        let map_b_local = core_b
            .build_merge_source_map(DB, TID, SRC, JOIN, None)
            .unwrap();
        assert!(
            map_b_local.is_empty(),
            "a non-owning core has no source rows to read locally"
        );

        // Ship core A's raw stored rows into core B's handler: the join-map now
        // matches core A's local map byte-for-byte.
        let shipped: Vec<(String, Vec<u8>)> = docs
            .iter()
            .map(|(id, b)| (id.to_string(), b.clone()))
            .collect();
        let map_b_shipped = core_b
            .build_merge_source_map(DB, TID, SRC, JOIN, Some(&shipped))
            .unwrap();

        assert_eq!(
            map_local, map_b_shipped,
            "shipped-source join-map must equal the co-resident local join-map"
        );
    }
}
