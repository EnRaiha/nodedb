// SPDX-License-Identifier: BUSL-1.1

//! Index side-effects for `apply_point_put`: spatial R-tree + columnar
//! ingest for geometry fields, and HNSW vector indexing (strict-schema and
//! schemaless). Split out of `apply_put.rs` to keep that file focused on the
//! core document-write transaction.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;

/// Capture of a single HNSW vector index mutation (insert or soft-delete),
/// carrying everything needed to both key the `VectorCollection` (`index_key`,
/// `vector_id`) AND reverse the paired `vector_doc_map` entry on rollback
/// (`collection`, `field`, `doc_id`). Replaces a raw `(index_key, vector_id)`
/// tuple so undo can restore/remove the reverse-lookup map symmetrically with
/// the R-tree's `SpatialInsert`/`SpatialDelete` undo pattern.
pub(in crate::data::executor) struct VectorIndexDelta {
    pub index_key: (nodedb_types::DatabaseId, crate::types::TenantId, String),
    pub vector_id: u32,
    pub collection: String,
    pub field: String,
    pub doc_id: String,
}

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

    /// Strict-schema `Vector(dim)` column names + dims declared on
    /// `collection`, or empty if the collection has no strict schema / no
    /// vector columns. Shared by `apply_point_put_vector_indexes` (which
    /// needs `dim` to validate extracted float arrays) and
    /// `apply_point_delete`'s vector cleanup (which only needs the field
    /// names to construct exact `vector_doc_map` keys without a full-map
    /// scan).
    pub(in crate::data::executor) fn strict_vector_fields(
        &self,
        tid: u64,
        collection: &str,
    ) -> Vec<(String, u32)> {
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        self.doc_configs
            .get(&config_key)
            .and_then(|config| {
                if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
                {
                    let fields: Vec<_> = schema
                        .columns
                        .iter()
                        .filter_map(|col| {
                            if let nodedb_types::columnar::ColumnType::Vector(dim) = col.column_type
                            {
                                Some((col.name.clone(), dim))
                            } else {
                                None
                            }
                        })
                        .collect();
                    if fields.is_empty() {
                        None
                    } else {
                        Some(fields)
                    }
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    /// Schemaless vector field names registered via `vector_params` for
    /// `collection` (named-field entries `"{collection}:{field}"`, plus the
    /// bare `"{collection}"` key defaulting to `"embedding"`). Shared by the
    /// put path's schemaless indexing branch and the delete cleanup's exact
    /// key construction.
    pub(in crate::data::executor) fn schemaless_vector_field_names(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<String> {
        let db_key = nodedb_types::DatabaseId::new(database_id);
        let tid_key = crate::types::TenantId::new(tid);
        let field_prefix = format!("{collection}:");
        let bare_key = (db_key, tid_key, collection.to_string());

        let mut names: Vec<String> = self
            .vector_params
            .keys()
            .filter(|(d, t, coll_key)| {
                *d == bare_key.0 && *t == bare_key.1 && coll_key.starts_with(&field_prefix)
            })
            .map(|k| k.2[field_prefix.len()..].to_string())
            .collect();
        if names.is_empty() && self.vector_params.contains_key(&bare_key) {
            names.push("embedding".to_string());
        }
        names
    }

    /// HNSW vector indexing side-effect: index declared strict-schema
    /// `Vector(dim)` columns, or (schemaless) fields matched by registered
    /// `vector_params`, into the corresponding `VectorCollection`.
    ///
    /// Returns the `(index_key, vector_id)` pairs inserted so a transactional
    /// caller can push `UndoEntry::InsertVector` reversals. Each inserted
    /// vector is also recorded in `vector_doc_map` keyed by the hex surrogate
    /// row key, so `apply_point_delete` can soft-delete it when the owning
    /// document is removed (closing the vector-orphan leak).
    pub(in crate::data::executor) fn apply_point_put_vector_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Vec<VectorIndexDelta> {
        let mut inserts: Vec<VectorIndexDelta> = Vec::new();

        // Vector index: if the strict schema declares Vector(dim) columns,
        // extract float arrays and insert into HNSW so KNN search works.
        let vector_fields = self.strict_vector_fields(tid, collection);

        if !vector_fields.is_empty() {
            // Decode from MessagePack (internal format) — not JSON.
            if let Ok(ndb_val) = nodedb_types::value_from_msgpack(value)
                && let nodedb_types::Value::Object(ref obj) = ndb_val
            {
                for (field_name, dim) in &vector_fields {
                    if let Some(nodedb_types::Value::Array(arr)) = obj.get(field_name) {
                        let floats: Vec<f32> = arr
                            .iter()
                            .filter_map(|v| match v {
                                nodedb_types::Value::Float(f) => Some(*f as f32),
                                nodedb_types::Value::Integer(i) => Some(*i as f32),
                                nodedb_types::Value::Decimal(d) => {
                                    use rust_decimal::prelude::ToPrimitive;
                                    d.to_f32()
                                }
                                nodedb_types::Value::String(s) => s.parse::<f32>().ok(),
                                _ => None,
                            })
                            .collect();
                        if floats.len() == *dim as usize {
                            let index_key =
                                Self::vector_index_key(database_id, tid, collection, field_name);
                            let params = self
                                .vector_params
                                .get(&index_key)
                                .cloned()
                                .unwrap_or_default();
                            let coll = self
                                .vector_collections
                                .entry(index_key.clone())
                                .or_insert_with(|| {
                                    nodedb_vector::VectorCollection::new(*dim as usize, params)
                                });
                            // Document-engine-owned auto-indexing: surrogate
                            // routing for these implicit vector binds rides
                            // with the document engine retrofit.
                            let vector_id =
                                coll.insert_with_surrogate(floats, nodedb_types::Surrogate::ZERO);
                            self.vector_doc_map.insert(
                                (
                                    index_key.0,
                                    index_key.1,
                                    collection.to_string(),
                                    field_name.clone(),
                                    document_id.to_string(),
                                ),
                                vector_id,
                            );
                            inserts.push(VectorIndexDelta {
                                index_key,
                                vector_id,
                                collection: collection.to_string(),
                                field: field_name.clone(),
                                doc_id: document_id.to_string(),
                            });
                        }
                    }
                }
            }
        }

        // Schemaless vector indexing: if no strict schema but vector_params exist
        // for this collection, extract matching fields and index them.
        if vector_fields.is_empty() {
            // Named-field keys have the shape `(DatabaseId, TenantId, "{collection}:{field}")`.
            // The bare (no-field) key is `(DatabaseId, TenantId, "{collection}")`.
            let db_key = nodedb_types::DatabaseId::new(database_id);
            let tid_key = crate::types::TenantId::new(tid);
            let field_prefix = format!("{collection}:");
            let bare_key = (db_key, tid_key, collection.to_string());
            let field_names = self.schemaless_vector_field_names(database_id, tid, collection);

            // Each field name maps back to its `vector_params` map key: either
            // the field-qualified key (if one was registered) or the bare key
            // (single default-"embedding" field, no per-field registration).
            let schemaless_keys: Vec<(
                (nodedb_types::DatabaseId, crate::types::TenantId, String),
                String,
            )> = field_names
                .into_iter()
                .map(|field| {
                    let qualified = (db_key, tid_key, format!("{field_prefix}{field}"));
                    let params_key = if self.vector_params.contains_key(&qualified) {
                        qualified
                    } else {
                        bare_key.clone()
                    };
                    (params_key, field)
                })
                .collect();

            if !schemaless_keys.is_empty()
                && let Ok(ndb_val) = nodedb_types::value_from_msgpack(value)
                && let nodedb_types::Value::Object(ref obj) = ndb_val
            {
                for (params_key, field_name) in &schemaless_keys {
                    if let Some(nodedb_types::Value::Array(arr)) = obj.get(field_name) {
                        let floats: Vec<f32> = arr
                            .iter()
                            .filter_map(|v| match v {
                                nodedb_types::Value::Float(f) => Some(*f as f32),
                                nodedb_types::Value::Integer(i) => Some(*i as f32),
                                nodedb_types::Value::Decimal(d) => {
                                    use rust_decimal::prelude::ToPrimitive;
                                    d.to_f32()
                                }
                                nodedb_types::Value::String(s) => s.parse::<f32>().ok(),
                                _ => None,
                            })
                            .collect();
                        if !floats.is_empty() {
                            let params = self
                                .vector_params
                                .get(params_key)
                                .cloned()
                                .unwrap_or_default();
                            // Use field-qualified key so search can find it.
                            let store_key =
                                Self::vector_index_key(database_id, tid, collection, field_name);
                            let coll = self
                                .vector_collections
                                .entry(store_key.clone())
                                .or_insert_with(|| {
                                    nodedb_vector::VectorCollection::new(floats.len(), params)
                                });
                            // Document-engine-owned auto-indexing: surrogate
                            // routing for these implicit vector binds rides
                            // with the document engine retrofit.
                            let vector_id =
                                coll.insert_with_surrogate(floats, nodedb_types::Surrogate::ZERO);
                            self.vector_doc_map.insert(
                                (
                                    store_key.0,
                                    store_key.1,
                                    collection.to_string(),
                                    field_name.clone(),
                                    document_id.to_string(),
                                ),
                                vector_id,
                            );
                            inserts.push(VectorIndexDelta {
                                index_key: store_key,
                                vector_id,
                                collection: collection.to_string(),
                                field: field_name.clone(),
                                doc_id: document_id.to_string(),
                            });
                        }
                    }
                }
            }
        }

        inserts
    }
}
