// SPDX-License-Identifier: BUSL-1.1

//! Geometry / predicate / projection helpers shared by the spatial scan
//! handler (`spatial.rs`) and the transaction overlay merge
//! (`transaction/overlay/spatial_merge.rs`).
//!
//! These are pure, storage-agnostic functions: they operate on already-decoded
//! `nodedb_types::Value` documents, so the same refinement logic applies to a
//! document-collection row (fetched from the sparse engine) and a
//! `spatial` / columnar-family row (fetched from columnar) alike.

use nodedb_physical::physical_plan::SpatialPredicate;
use nodedb_types::Value;

/// Extract geometry from a document field.
///
/// Handles three storage forms:
/// - `Value::Geometry(g)` — native geometry (columnar path preserves type)
/// - `Value::String(s)` — GeoJSON string (from SQL ST_Point → serialized)
/// - `Value::Object(_)` — GeoJSON object (from schemaless doc storage)
pub(in crate::data::executor) fn extract_geometry(
    doc: &Value,
    field: &str,
) -> Option<nodedb_types::geometry::Geometry> {
    let field_val = doc.get(field)?;
    match field_val {
        Value::Geometry(g) => Some(g.clone()),
        Value::String(s) => nodedb_types::geometry::from_geojson_str(s),
        Value::Object(map) => {
            // GeoJSON object stored as Value::Object — serialize to JSON then parse.
            let json = serde_json::Value::from(Value::Object(map.clone()));
            serde_json::from_value(json).ok()
        }
        _ => None,
    }
}

/// Apply the spatial predicate.
pub(in crate::data::executor) fn apply_predicate(
    predicate: &SpatialPredicate,
    query: &nodedb_types::geometry::Geometry,
    doc: &nodedb_types::geometry::Geometry,
    distance_meters: f64,
) -> bool {
    match predicate {
        SpatialPredicate::DWithin => {
            crate::engine::spatial::st_dwithin(query, doc, distance_meters)
        }
        // `ST_Contains(loc, q)` asks whether the *stored* geometry contains
        // the query geometry — the geofencing shape, where `loc` is a zone
        // polygon and `q` a point. `ST_Within(loc, q)` is its converse. Both
        // pass the stored geometry in the position SQL named first.
        SpatialPredicate::Contains => crate::engine::spatial::st_contains(doc, query),
        SpatialPredicate::Intersects => crate::engine::spatial::st_intersects(query, doc),
        SpatialPredicate::Within => crate::engine::spatial::st_within(doc, query),
    }
}

/// One spatial scan result: the projected row and the surrogate it was read
/// under. The overlay merge keys rows by `surrogate`, never by a row field,
/// so the row names its identity by the collection's identity column alone.
pub(in crate::data::executor) struct SpatialHit {
    /// The row's surrogate, `None` when its `doc_id` is no hex surrogate.
    pub surrogate: Option<u32>,
    pub row: Value,
}

impl SpatialHit {
    /// A hit read under `doc_id`: a document row's hex storage key, or a
    /// columnar-family row's `id` value.
    pub(in crate::data::executor) fn new(doc_id: &str, row: Value) -> Self {
        Self {
            surrogate: u32::from_str_radix(doc_id, 16).ok(),
            row,
        }
    }
}

/// The rows of `hits`, in order, for the response encoder.
pub(in crate::data::executor) fn hit_rows(hits: Vec<SpatialHit>) -> Vec<Value> {
    hits.into_iter().map(|hit| hit.row).collect()
}

/// Apply projection to a document, returning `nodedb_types::Value`.
///
/// The row names its identity under `identity_column`: the collection's
/// declared key, else `id`. A document that holds that column keeps its own
/// value. One that lacks it gains `doc_id` there. A declared-key row never
/// gains an `id` beside its key.
pub(in crate::data::executor) fn project_doc(
    doc: &Value,
    doc_id: &str,
    projection: &[String],
    identity_column: &str,
) -> Value {
    let identity = doc
        .get(identity_column)
        .cloned()
        .unwrap_or_else(|| Value::String(doc_id.to_string()));
    if projection.is_empty() {
        if let Value::Object(mut map) = doc.clone() {
            map.entry(identity_column.to_string()).or_insert(identity);
            Value::Object(map)
        } else {
            doc.clone()
        }
    } else {
        let mut map = std::collections::HashMap::new();
        map.insert(identity_column.to_string(), identity);
        for col in projection {
            if let Some(v) = doc.get(col) {
                map.insert(col.clone(), v.clone());
            }
        }
        Value::Object(map)
    }
}

/// Expand a bounding box by a distance in meters.
pub(in crate::data::executor) fn expand_bbox(
    bbox: &nodedb_types::BoundingBox,
    meters: f64,
) -> nodedb_types::BoundingBox {
    let lat_delta = meters / 111_320.0;
    let avg_lat = ((bbox.min_lat + bbox.max_lat) / 2.0).to_radians();
    let lng_delta = meters / (111_320.0 * avg_lat.cos().max(0.001));

    nodedb_types::BoundingBox::new(
        bbox.min_lng - lng_delta,
        bbox.min_lat - lat_delta,
        bbox.max_lng + lng_delta,
        bbox.max_lat + lat_delta,
    )
}
