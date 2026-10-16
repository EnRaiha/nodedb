// SPDX-License-Identifier: Apache-2.0

//! GeoJSON-compatible geometry types, spatial reference systems, and
//! spherical geo math.

pub mod math;
pub mod shape;
pub mod srid;
pub mod tagged;

pub use math::{haversine_bearing, haversine_distance, point_in_polygon};
pub use shape::{Coord, Geometry};
pub use srid::{SUPPORTED_SRID_CODES, Srid, UnsupportedSrid};
pub use tagged::{
    CrsMember, CrsProperties, GeoJsonError, SridGeometry, from_geojson_str, parse_geojson,
    parse_geojson_slice, srid_from_crs,
};
