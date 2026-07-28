// SPDX-License-Identifier: Apache-2.0

//! The geospatial function catalog.
//!
//! This table is the single source of truth for the geo/spatial SQL surface.
//! It drives three consumers that previously each kept their own partial list:
//!
//! * `nodedb-sql`'s function registry (the plan-time existence gate, arity
//!   check, and return typing) is generated from it;
//! * `nodedb-sql`'s geometry-expression resolver reads it to decide whether a
//!   call is geometry-valued and may appear in geometry position;
//! * this crate's evaluator dispatches on the canonical name it declares.
//!
//! Because all three read the same rows, a function cannot resolve in one
//! syntactic position and be unknown in another — the failure mode that let
//! `ST_GeomFromText(...)` work in `INSERT ... VALUES` while being rejected
//! inside `ST_DWithin(...)`, and let `geo_x` evaluate while `ST_X` did not
//! exist. Adding a capability means adding one row here.

use super::spec::{GeoArgShape::*, GeoFunctionSpec, GeoReturn::*, f};

/// Every geospatial SQL function, with all of its accepted spellings.
pub static GEO_FUNCTIONS: &[GeoFunctionSpec] = &[
    // ── Topological predicates ──
    f("st_contains", &[], Geometry2, Bool),
    f("st_intersects", &[], Geometry2, Bool),
    f("st_within", &[], Geometry2, Bool),
    f("st_disjoint", &[], Geometry2, Bool),
    f("st_dwithin", &[], Geometry2Distance, Bool),
    f("st_isvalid", &["geo_is_valid"], Geometry1, Bool),
    // ── Measures ──
    f("st_distance", &[], Geometry2, Float),
    f("st_length", &["geo_length"], Geometry1, Float),
    f("st_perimeter", &["geo_perimeter"], Geometry1, Float),
    f("st_area", &[], Geometry1, Float),
    f("geo_distance", &["haversine_distance"], Haversine4, Float),
    f("geo_bearing", &["haversine_bearing"], Haversine4, Float),
    // ── Accessors ──
    f("st_x", &["geo_x"], Geometry1, Float),
    f("st_y", &["geo_y"], Geometry1, Float),
    f("st_astext", &["geo_as_wkt"], Geometry1, Text),
    f("st_asgeojson", &["geo_as_geojson"], Geometry1, Text),
    f("st_geometrytype", &["geo_type"], Geometry1, Text),
    f("st_npoints", &["geo_num_points"], Geometry1, Int),
    f("st_srid", &[], Geometry1, Int),
    // ── Constructors ──
    f("st_point", &["geo_point"], LngLat, Geometry),
    f("st_makepoint", &[], LngLatZ, Geometry),
    f("st_geomfromtext", &["geo_from_wkt"], TextSrid, Geometry),
    f("st_geomfromgeojson", &["geo_from_geojson"], Text1, Geometry),
    f("st_geomfromwkb", &[], BytesSrid, Geometry),
    f("st_makeline", &["geo_line"], PointsVariadic, Geometry),
    f("st_makepolygon", &["geo_polygon"], RingsVariadic, Geometry),
    f("st_makeenvelope", &["geo_bbox"], Bbox, Geometry),
    f("geo_circle", &[], Circle, Geometry),
    // ── Geometry-returning operations ──
    f("st_buffer", &[], GeometryBuffer, Geometry),
    f("st_envelope", &[], Geometry1, Geometry),
    f("st_union", &[], Geometry2, Geometry),
    f("st_intersection", &[], Geometry2, Geometry),
    f("st_centroid", &[], Geometry1, Geometry),
    // ── Geohash ──
    f("st_geohash", &["geo_geohash"], LngLatPrecision, Text),
    f("st_geohashdecode", &["geo_geohash_decode"], Text1, Object),
    f("geo_geohash_neighbors", &[], Text1, Array),
    // ── H3 ──
    // `geo_h3` and `h3_latlngtocell` are NOT aliases: `geo_h3` takes
    // (lng, lat, resolution) while the H3 surface name takes (lat, lng,
    // resolution). Folding them together would silently transpose
    // coordinates, so they stay separate rows with separate shapes.
    f("geo_h3", &[], LngLatPrecision, Text),
    f("h3_latlngtocell", &[], LatLngResolution, Text),
    f("h3_celltolatlng", &[], Text1, Object),
    f("geo_h3_to_boundary", &[], Text1, Geometry),
    f("geo_h3_resolution", &[], Text1, Int),
];
