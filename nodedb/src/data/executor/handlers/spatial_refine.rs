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
use nodedb_spatial::SridMismatch;
use nodedb_types::geometry::SridGeometry;
use nodedb_types::{BboxSpace, Value};

/// Extract geometry from a document field.
///
/// Handles three storage forms:
/// - `Value::Geometry(g)` — native geometry (columnar path preserves type)
/// - `Value::String(s)` — GeoJSON string (from SQL ST_Point → serialized)
/// - `Value::Object(_)` — GeoJSON object (from schemaless doc storage)
///
/// GeoJSON keeps the SRID its `crs` member names; no member is WGS 84.
pub(in crate::data::executor) fn extract_geometry(
    doc: &Value,
    field: &str,
) -> Option<SridGeometry> {
    SridGeometry::from_value(doc.get(field)?)
}

/// Apply the spatial predicate.
///
/// The stored and query geometries must share an SRID: a mismatch is
/// [`SridMismatch`] (`index` the stored SRID, `entry` the query's), which
/// the caller raises as `Error::MixedSrid` instead of skipping the row.
/// `ST_DWithin` measures geodesic metres for 4326 and planar metres for a
/// projected SRID.
pub(in crate::data::executor) fn apply_predicate(
    predicate: &SpatialPredicate,
    query: &SridGeometry,
    doc: &SridGeometry,
    distance_meters: f64,
) -> Result<bool, SridMismatch> {
    if query.srid != doc.srid {
        return Err(SridMismatch {
            index: doc.srid,
            entry: query.srid,
        });
    }
    let (q, d) = (&query.geometry, &doc.geometry);
    Ok(match predicate {
        SpatialPredicate::DWithin => {
            if doc.srid.is_geographic() {
                crate::engine::spatial::st_dwithin(q, d, distance_meters)
            } else {
                nodedb_spatial::planar_dwithin(q, d, distance_meters)
            }
        }
        // `ST_Contains(loc, q)` asks whether the *stored* geometry contains
        // the query geometry — the geofencing shape, where `loc` is a zone
        // polygon and `q` a point. `ST_Within(loc, q)` is its converse. Both
        // pass the stored geometry in the position SQL named first.
        SpatialPredicate::Contains => crate::engine::spatial::st_contains(d, q),
        SpatialPredicate::Intersects => crate::engine::spatial::st_intersects(q, d),
        SpatialPredicate::Within => crate::engine::spatial::st_within(d, q),
    })
}

/// The `Error::MixedSrid` a spatial scan of `collection.field` raises for
/// `mismatch`.
pub(in crate::data::executor) fn mixed_srid_error(
    collection: &str,
    field: &str,
    mismatch: SridMismatch,
) -> crate::Error {
    crate::Error::MixedSrid {
        collection: collection.to_string(),
        field: field.to_string(),
        index_srid: mismatch.index.code(),
        row_srid: mismatch.entry.code(),
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

/// Metres per degree used to widen a geographic search box: the WGS 84
/// equatorial degree of longitude, rounded. Using it for latitude too keeps
/// the box at least as wide as the true distance.
const METRES_PER_DEGREE: f64 = 111_320.0;

/// Floor on cos(latitude) so a box near a pole stays finite.
const MIN_COS_LAT: f64 = 0.001;

/// Expand a bounding box by a distance in meters, in `space`.
///
/// Geographic space converts metres to degrees at the box's mean latitude.
/// Planar space adds the metres directly.
pub(in crate::data::executor) fn expand_bbox(
    bbox: &nodedb_types::BoundingBox,
    meters: f64,
    space: BboxSpace,
) -> nodedb_types::BoundingBox {
    let (lng_delta, lat_delta) = match space {
        BboxSpace::Geographic => {
            let avg_lat = ((bbox.min_lat + bbox.max_lat) / 2.0).to_radians();
            (
                meters / (METRES_PER_DEGREE * avg_lat.cos().max(MIN_COS_LAT)),
                meters / METRES_PER_DEGREE,
            )
        }
        BboxSpace::Planar => (meters, meters),
    };

    nodedb_types::BoundingBox::new(
        bbox.min_lng - lng_delta,
        bbox.min_lat - lat_delta,
        bbox.max_lng + lng_delta,
        bbox.max_lat + lat_delta,
    )
}
