// SPDX-License-Identifier: Apache-2.0

//! Argument and return typing for the geospatial function surface.
//!
//! The catalog in `nodedb-query` declares each function's argument *shape*;
//! this module is the one place those shapes are given SQL types. Both
//! mappings are exhaustive matches, so a shape or return kind added to the
//! catalog fails to compile here until the planner is taught its types —
//! which is what keeps the registry from drifting behind the evaluator.

use nodedb_query::geo_functions::{GeoArgShape, GeoReturn};
use nodedb_types::columnar::ColumnType;

use crate::functions::registry::ArgTypeSpec;

const FLOAT: &[ColumnType] = &[ColumnType::Float64];
const INT: &[ColumnType] = &[ColumnType::Int64];
const TEXT: &[ColumnType] = &[ColumnType::String];
const BYTES: &[ColumnType] = &[ColumnType::Bytes];
const GEOMETRY: &[ColumnType] = &[ColumnType::Geometry];
const ANY: &[ColumnType] = &[];

const fn arg(name: &'static str, accepted: &'static [ColumnType]) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted,
        variadic: false,
    }
}

const fn variadic(name: &'static str, accepted: &'static [ColumnType]) -> ArgTypeSpec {
    ArgTypeSpec {
        name,
        accepted,
        variadic: true,
    }
}

static GEOMETRY_1: &[ArgTypeSpec] = &[arg("geom", GEOMETRY)];
static GEOMETRY_2: &[ArgTypeSpec] = &[arg("geom1", GEOMETRY), arg("geom2", GEOMETRY)];
static GEOMETRY_2_DISTANCE: &[ArgTypeSpec] = &[
    arg("geom1", GEOMETRY),
    arg("geom2", GEOMETRY),
    arg("distance_m", FLOAT),
];
static GEOMETRY_BUFFER: &[ArgTypeSpec] = &[
    arg("geom", GEOMETRY),
    arg("distance_m", FLOAT),
    arg("segments", INT),
];
static LNG_LAT: &[ArgTypeSpec] = &[arg("lng", FLOAT), arg("lat", FLOAT)];
static LNG_LAT_Z: &[ArgTypeSpec] = &[arg("x", FLOAT), arg("y", FLOAT), arg("z", FLOAT)];
static LNG_LAT_PRECISION: &[ArgTypeSpec] =
    &[arg("lng", FLOAT), arg("lat", FLOAT), arg("precision", INT)];
static LAT_LNG_RESOLUTION: &[ArgTypeSpec] =
    &[arg("lat", FLOAT), arg("lng", FLOAT), arg("resolution", INT)];
static TEXT_1: &[ArgTypeSpec] = &[arg("text", TEXT)];
static TEXT_SRID: &[ArgTypeSpec] = &[arg("text", TEXT), arg("srid", INT)];
static BYTES_SRID: &[ArgTypeSpec] = &[arg("wkb", BYTES), arg("srid", INT)];
static HAVERSINE_4: &[ArgTypeSpec] = &[
    arg("lng1", FLOAT),
    arg("lat1", FLOAT),
    arg("lng2", FLOAT),
    arg("lat2", FLOAT),
];
static CIRCLE: &[ArgTypeSpec] = &[
    arg("lng", FLOAT),
    arg("lat", FLOAT),
    arg("radius_m", FLOAT),
    arg("segments", INT),
];
static BBOX: &[ArgTypeSpec] = &[
    arg("min_lng", FLOAT),
    arg("min_lat", FLOAT),
    arg("max_lng", FLOAT),
    arg("max_lat", FLOAT),
];
static POINTS_VARIADIC: &[ArgTypeSpec] = &[arg("point1", GEOMETRY), variadic("point", GEOMETRY)];
static RINGS_VARIADIC: &[ArgTypeSpec] = &[variadic("ring", ANY)];

/// SQL argument types for a catalog argument shape.
pub(super) fn arg_types(shape: GeoArgShape) -> &'static [ArgTypeSpec] {
    match shape {
        GeoArgShape::Geometry1 => GEOMETRY_1,
        GeoArgShape::Geometry2 => GEOMETRY_2,
        GeoArgShape::Geometry2Distance => GEOMETRY_2_DISTANCE,
        GeoArgShape::GeometryBuffer => GEOMETRY_BUFFER,
        GeoArgShape::LngLat => LNG_LAT,
        GeoArgShape::LngLatZ => LNG_LAT_Z,
        GeoArgShape::LngLatPrecision => LNG_LAT_PRECISION,
        GeoArgShape::LatLngResolution => LAT_LNG_RESOLUTION,
        GeoArgShape::Text1 => TEXT_1,
        GeoArgShape::TextSrid => TEXT_SRID,
        GeoArgShape::BytesSrid => BYTES_SRID,
        GeoArgShape::Haversine4 => HAVERSINE_4,
        GeoArgShape::Circle => CIRCLE,
        GeoArgShape::Bbox => BBOX,
        GeoArgShape::PointsVariadic => POINTS_VARIADIC,
        GeoArgShape::RingsVariadic => RINGS_VARIADIC,
    }
}

/// Plan-time return type for a catalog return kind.
///
/// `Object` and `Array` results (decoded geohash bounds, H3 cell centres,
/// neighbour lists) have no scalar column type; `None` marks them as resolved
/// at runtime, the same convention the rest of the registry uses.
pub(super) fn return_type(returns: GeoReturn) -> Option<ColumnType> {
    match returns {
        GeoReturn::Bool => Some(ColumnType::Bool),
        GeoReturn::Float => Some(ColumnType::Float64),
        GeoReturn::Int => Some(ColumnType::Int64),
        GeoReturn::Text => Some(ColumnType::String),
        GeoReturn::Geometry => Some(ColumnType::Geometry),
        GeoReturn::Array => Some(ColumnType::Array),
        GeoReturn::Object => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_query::geo_functions::GEO_FUNCTIONS;

    /// A declared arity must be describable by the shape's type specs:
    /// enough positions for the maximum, and no fewer than the minimum.
    /// A variadic tail covers any maximum.
    #[test]
    fn arg_specs_cover_every_declared_arity() {
        for spec in GEO_FUNCTIONS {
            let (min, max) = spec.args.arity();
            let types = arg_types(spec.args);
            assert!(
                types.len() >= min,
                "'{}' accepts {min} args but declares only {} type specs",
                spec.canonical,
                types.len()
            );
            let is_variadic = types.last().is_some_and(|t| t.variadic);
            assert!(
                is_variadic || types.len() >= max,
                "'{}' accepts up to {max} args but declares only {} type specs",
                spec.canonical,
                types.len()
            );
        }
    }
}
