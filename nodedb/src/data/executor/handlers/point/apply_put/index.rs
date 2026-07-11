// SPDX-License-Identifier: BUSL-1.1

//! Spatial R-tree + columnar ingest side-effect for `apply_point_put`:
//! geometry-field detection, per-field R-tree insert, reverse entry→doc map,
//! and columnar-memtable ingest. HNSW vector indexing lives in the sibling
//! `vector` module. Split out of `apply_put.rs` to keep that file focused on
//! the core document-write transaction.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;

impl CoreLoop {
    /// Spatial R-tree + columnar ingest side-effect: parse geometry fields,
    /// insert into the per-field R-tree, maintain the reverse entry→doc map,
    /// and (when geometry present) ingest into the columnar memtable so bare
    /// scans/aggregates over spatial collections work.
    ///
    /// Returns the `(spatial_index_key, entry_id)` pairs inserted so a
    /// transactional caller can push `UndoEntry::SpatialInsert` reversals. The
    /// spatial writes are in-memory (an aborted redb txn does not reverse them),
    /// so explicit undo is required. Empty when no geometry fields are present.
    pub(in crate::data::executor) fn apply_point_put_spatial(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Vec<(
        (
            nodedb_types::DatabaseId,
            crate::types::TenantId,
            String,
            String,
        ),
        u64,
    )> {
        let mut inserts = Vec::new();
        // Spatial index: detect geometry fields and insert into R-tree.
        // Tries to parse each object field as a GeoJSON Geometry.
        // If successful, computes bbox and inserts into the per-field R-tree.
        // Also writes the document to columnar_memtables so that bare table scans
        // and aggregates on spatial collections read from columnar (spatial extends columnar).
        if let Some(doc) = doc_format::decode_document(value)
            && let Some(obj) = doc.as_object()
        {
            let mut has_geometry = false;
            for (field_name, field_value) in obj {
                if let Ok(geom) =
                    serde_json::from_value::<nodedb_types::geometry::Geometry>(field_value.clone())
                {
                    has_geometry = true;
                    let bbox = nodedb_types::bbox::geometry_bbox(&geom);
                    let db_id = nodedb_types::DatabaseId::new(database_id);
                    let tid_id = crate::types::TenantId::new(tid);
                    let spatial_key = (db_id, tid_id, collection.to_string(), field_name.clone());
                    let entry_id = crate::util::fnv1a_hash(document_id.as_bytes());
                    let rtree = self.spatial_indexes.entry(spatial_key.clone()).or_default();
                    rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                    // Maintain reverse map: entry_id → document_id.
                    self.spatial_doc_map.insert(
                        (
                            db_id,
                            tid_id,
                            collection.to_string(),
                            field_name.clone(),
                            entry_id,
                        ),
                        document_id.to_string(),
                    );
                    inserts.push((spatial_key, entry_id));
                }
            }

            // If document has geometry, also write to columnar memtable.
            // This ensures bare scans + aggregates work via columnar path.
            if has_geometry {
                self.ingest_doc_to_columnar(database_id, tid, collection, obj);
            }
        }

        inserts
    }
}
