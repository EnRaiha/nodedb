// SPDX-License-Identifier: BUSL-1.1

//! Index side-effects for `apply_point_put`: spatial R-tree + columnar
//! ingest for geometry fields, and HNSW vector indexing (strict-schema and
//! schemaless). Split out of `apply_put.rs` to keep that file focused on the
//! core document-write transaction.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Spatial R-tree + columnar ingest side-effect: parse geometry fields,
    /// insert into the per-field R-tree, maintain the reverse entry→doc map,
    /// and (when geometry present) ingest into the columnar memtable so bare
    /// scans/aggregates over spatial collections work.
    pub(in crate::data::executor) fn apply_point_put_spatial(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) {
        // Spatial index: detect geometry fields and insert into R-tree.
        // Tries to parse each object field as a GeoJSON Geometry.
        // If successful, computes bbox and inserts into the per-field R-tree.
        // Also writes the document to columnar_memtables so that bare table scans
        // and aggregates on spatial collections read from columnar (spatial extends columnar).
        if let Some(doc) = super::super::super::doc_format::decode_document(value)
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
                }
            }

            // If document has geometry, also write to columnar memtable.
            // This ensures bare scans + aggregates work via columnar path.
            if has_geometry {
                self.ingest_doc_to_columnar(database_id, tid, collection, obj);
            }
        }
    }

    /// HNSW vector indexing side-effect: index declared strict-schema
    /// `Vector(dim)` columns, or (schemaless) fields matched by registered
    /// `vector_params`, into the corresponding `VectorCollection`.
    pub(in crate::data::executor) fn apply_point_put_vector_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        value: &[u8],
    ) {
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());

        // Vector index: if the strict schema declares Vector(dim) columns,
        // extract float arrays and insert into HNSW so KNN search works.
        // Collect vector fields from schema first (avoids borrow conflict).
        let vector_fields: Vec<(String, u32)> = self
            .doc_configs
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
            .unwrap_or_default();

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
                            let coll =
                                self.vector_collections.entry(index_key).or_insert_with(|| {
                                    nodedb_vector::VectorCollection::new(*dim as usize, params)
                                });
                            // Document-engine-owned auto-indexing: surrogate
                            // routing for these implicit vector binds rides
                            // with the document engine retrofit.
                            coll.insert_with_surrogate(floats, nodedb_types::Surrogate::ZERO);
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

            // Collect all vector_params entries for this database+tenant+collection.
            // Each entry maps to a (params_map_key, field_name) pair.
            let mut schemaless_keys: Vec<(
                (nodedb_types::DatabaseId, crate::types::TenantId, String),
                String,
            )> = self
                .vector_params
                .keys()
                .filter(|(d, t, coll_key)| {
                    *d == bare_key.0 && *t == bare_key.1 && coll_key.starts_with(&field_prefix)
                })
                .map(|k| {
                    let field = k.2[field_prefix.len()..].to_string();
                    (k.clone(), field)
                })
                .collect();
            // Also check for bare key (no field name) — default to "embedding".
            if schemaless_keys.is_empty() && self.vector_params.contains_key(&bare_key) {
                schemaless_keys.push((bare_key.clone(), "embedding".to_string()));
            }

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
                            let coll =
                                self.vector_collections.entry(store_key).or_insert_with(|| {
                                    nodedb_vector::VectorCollection::new(floats.len(), params)
                                });
                            // Document-engine-owned auto-indexing: surrogate
                            // routing for these implicit vector binds rides
                            // with the document engine retrofit.
                            coll.insert_with_surrogate(floats, nodedb_types::Surrogate::ZERO);
                        }
                    }
                }
            }
        }
    }
}
