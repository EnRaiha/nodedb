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

/// Apply projection to a document, returning `nodedb_types::Value`.
pub(in crate::data::executor) fn project_doc(
    doc: &Value,
    doc_id: &str,
    projection: &[String],
) -> Value {
    if projection.is_empty() {
        // Add id if not present.
        if let Value::Object(mut map) = doc.clone() {
            map.entry("id".to_string())
                .or_insert(Value::String(doc_id.to_string()));
            Value::Object(map)
        } else {
            doc.clone()
        }
    } else {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::String(doc_id.to_string()));
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
