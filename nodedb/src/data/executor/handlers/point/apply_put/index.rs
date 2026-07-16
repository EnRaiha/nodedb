// SPDX-License-Identifier: BUSL-1.1

//! Spatial R-tree + columnar ingest side-effect for `apply_point_put`:
//! geometry-field detection, per-field R-tree insert, reverse entry→doc map,
//! and columnar-memtable ingest. HNSW vector indexing lives in the sibling
//! `vector` module. Split out of `apply_put.rs` to keep that file focused on
//! the core document-write transaction.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::spatial_key::SpatialIndexKey;

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
        // Re-indexing a document must REPLACE, not append: `RTree::insert`
        // blindly pushes a fresh entry even when one with this `entry_id`
        // already exists, so a live geometry UPDATE, a WAL replay, or the
        // crash-recovery rebuild would otherwise leave stale duplicate bbox
        // entries scoring alongside the new one. Clear any prior geometry for
        // this document first (idempotent — a no-op on a genuine first insert).
        // The removed tuples are discarded here, mirroring the vector put path:
        // only the new inserts are captured for transactional undo.
        let _ = self.remove_document_spatial_indexes(database_id, tid, collection, document_id);
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

    /// Remove every R-tree entry (and its paired `spatial_doc_map` reverse
    /// entry) this document produced across all of the collection's per-field
    /// spatial indexes, keyed by `fnv1a_hash(document_id)` — the same hash the
    /// insert path uses. Shared by the PointDelete cascade (which orphans the
    /// geometry of a removed row) and `apply_point_put_spatial` (which must
    /// clear a document's prior geometry before re-inserting, since
    /// `RTree::insert` appends rather than replaces).
    ///
    /// The bbox is read BEFORE the R-tree `delete` (which does not return the
    /// removed geometry) so a transactional caller can push
    /// `UndoEntry::SpatialDelete` re-insert reversals — the reverse
    /// `spatial_doc_map` stores only the doc id. Returns the removed
    /// `(spatial_index_key, entry_id, bbox, document_id)` tuples; empty when the
    /// document had no spatial fields.
    pub(in crate::data::executor) fn remove_document_spatial_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        document_id: &str,
    ) -> Vec<(SpatialIndexKey, u64, nodedb_types::BoundingBox, String)> {
        let mut spatial_deletes = Vec::new();
        let entry_id = crate::util::fnv1a_hash(document_id.as_bytes());
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let tid_id = crate::types::TenantId::new(tid);
        let spatial_fields: Vec<String> = self
            .spatial_indexes
            .keys()
            .filter(|(d, t, c, _)| *d == db_id && *t == tid_id && c == collection)
            .map(|(_, _, _, f)| f.clone())
            .collect();
        for field in spatial_fields {
            let skey = (db_id, tid_id, collection.to_string(), field.clone());
            // Read the bbox BEFORE deleting — the R-tree `delete` does not
            // return the removed geometry, so a reversible undo must capture
            // it here (the reverse `spatial_doc_map` stores only the doc id).
            let bbox = self
                .spatial_indexes
                .get(&skey)
                .and_then(|rtree| rtree.entries().into_iter().find(|e| e.id == entry_id))
                .map(|e| e.bbox);
            if let Some(rtree) = self.spatial_indexes.get_mut(&skey) {
                rtree.delete(entry_id);
            }
            let removed_doc = self.spatial_doc_map.remove(&(
                db_id,
                tid_id,
                collection.to_string(),
                field,
                entry_id,
            ));
            if let (Some(bbox), Some(doc)) = (bbox, removed_doc) {
                spatial_deletes.push((skey, entry_id, bbox, doc));
            }
        }
        spatial_deletes
    }
}
