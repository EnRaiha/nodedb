// SPDX-License-Identifier: BUSL-1.1

//! Post-insert R-tree maintenance for `Geometry`-typed columnar columns, so
//! spatial predicates can find newly-inserted rows.

use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Populate the R-tree for any `Geometry` columns on `schema`, from the
    /// rows just inserted (`ndb_rows`).
    ///
    /// `schema` is threaded in from the caller rather than re-read from
    /// `columnar_engines` here: the caller already resolved it, and taking it
    /// as a parameter means there is no "engine missing" case to invent an
    /// answer for. Re-looking it up would force a silent early return on a
    /// state the caller has already ruled out, and a geometry index that
    /// silently skips maintenance drifts out of sync with its rows.
    pub(in crate::data::executor) fn index_columnar_geometry_columns(
        &mut self,
        task: &ExecutionTask,
        schema: &ColumnarSchema,
        collection: &str,
        ndb_rows: &[nodedb_types::Value],
    ) {
        let db_id = task.request.database_id;
        let tid = task.request.tenant_id;
        let geom_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.column_type == ColumnType::Geometry)
            .map(|(i, _)| i)
            .collect();

        if geom_cols.is_empty() {
            return;
        }
        for row in ndb_rows {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };
            let doc_id = obj
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if doc_id.is_empty() {
                continue;
            }
            // Re-indexing this doc_id (e.g. a live geometry UPDATE) must
            // REPLACE, not append: `RTree::insert` blindly pushes a new
            // entry even when one with this id already exists, so
            // without clearing first the tree would carry a stale bbox
            // alongside the fresh one and a scan would match/emit the
            // document twice. Mirrors `apply_point_put_spatial`'s use of
            // the same helper; the removed tuples aren't needed here
            // since this insert path has no transactional undo to feed.
            let _ = self.remove_document_spatial_indexes(
                db_id.as_u64(),
                tid.as_u64(),
                collection,
                &doc_id,
            );
            for &col_idx in &geom_cols {
                let col_def = &schema.columns[col_idx];
                let field_val = match obj.get(&col_def.name) {
                    Some(v) => v,
                    None => continue,
                };
                // Geometry may be stored as Value::Geometry or Value::String (GeoJSON).
                // See `nodedb_types::geometry::from_geojson_str` — shared with the
                // document index path (`apply_point_put_spatial`) and the read path
                // (`extract_geometry`); keep all three in sync.
                let geom: nodedb_types::geometry::Geometry = match field_val {
                    Value::Geometry(g) => g.clone(),
                    Value::String(s) => match nodedb_types::geometry::from_geojson_str(s) {
                        Some(g) => g,
                        None => continue,
                    },
                    _ => continue,
                };
                let bbox = nodedb_types::bbox::geometry_bbox(&geom);
                let index_key = (db_id, tid, collection.to_string(), col_def.name.clone());
                let entry_id = crate::util::fnv1a_hash(doc_id.as_bytes());
                let rtree = self.spatial_indexes.entry(index_key.clone()).or_default();
                rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                self.spatial_doc_map.insert(
                    (
                        db_id,
                        tid,
                        collection.to_string(),
                        col_def.name.clone(),
                        entry_id,
                    ),
                    doc_id.clone(),
                );
            }
        }
    }
}
