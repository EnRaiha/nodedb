// SPDX-License-Identifier: Apache-2.0

//! GeoJSON-compatible geometry types.
//!
//! Supports Point, LineString, Polygon, MultiPoint, MultiLineString,
//! MultiPolygon, and GeometryCollection. Stored as GeoJSON for JSON
//! compatibility. Includes distance (Haversine), area, bearing, and
//! centroid calculations.

use serde::{Deserialize, Serialize};

/// A 2D coordinate (longitude, latitude) following GeoJSON convention.
/// Note: GeoJSON uses [lng, lat] order, NOT [lat, lng].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Coord {
    pub lng: f64,
    pub lat: f64,
}

impl Coord {
    pub fn new(lng: f64, lat: f64) -> Self {
        Self { lng, lat }
    }
}

/// GeoJSON-compatible geometry types.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum Geometry {
    Point {
        coordinates: [f64; 2],
    },
    LineString {
        coordinates: Vec<[f64; 2]>,
    },
    Polygon {
        coordinates: Vec<Vec<[f64; 2]>>,
    },
    MultiPoint {
        coordinates: Vec<[f64; 2]>,
    },
    MultiLineString {
        coordinates: Vec<Vec<[f64; 2]>>,
    },
    MultiPolygon {
        coordinates: Vec<Vec<Vec<[f64; 2]>>>,
    },
    GeometryCollection {
        geometries: Vec<Geometry>,
    },
}

impl Geometry {
    /// Create a Point from (longitude, latitude).
    pub fn point(lng: f64, lat: f64) -> Self {
        Geometry::Point {
            coordinates: [lng, lat],
        }
    }

    /// Create a LineString from a series of [lng, lat] pairs.
    pub fn line_string(coords: Vec<[f64; 2]>) -> Self {
        Geometry::LineString {
            coordinates: coords,
        }
    }

    /// Create a Polygon from exterior ring (and optional holes).
    ///
    /// The first ring is the exterior, subsequent rings are holes.
    /// Each ring must be a closed loop (first point == last point).
    pub fn polygon(rings: Vec<Vec<[f64; 2]>>) -> Self {
        Geometry::Polygon { coordinates: rings }
    }

    /// Get the type name of this geometry.
    pub fn geometry_type(&self) -> &'static str {
        match self {
            Geometry::Point { .. } => "Point",
            Geometry::LineString { .. } => "LineString",
            Geometry::Polygon { .. } => "Polygon",
            Geometry::MultiPoint { .. } => "MultiPoint",
            Geometry::MultiLineString { .. } => "MultiLineString",
            Geometry::MultiPolygon { .. } => "MultiPolygon",
            Geometry::GeometryCollection { .. } => "GeometryCollection",
        }
    }
}

/// Parse a GeoJSON string into a [`Geometry`].
///
/// Shared by every storage/read path that may encounter geometry stored as a
/// JSON string rather than a native object — SQL `ST_Point(...)` inserts
/// serialize to a GeoJSON string, while schemaless document writes and
/// `Value::Geometry` keep the native form. Centralizing the string-parse core
/// here keeps the three call sites (document index build, spatial read path,
/// columnar geometry index) from drifting on which parser they use.
///
/// Uses `sonic_rs` per workspace policy (never `serde_json::from_str` for
/// runtime JSON parsing). Returns `None` on malformed/non-geometry JSON.
pub fn from_geojson_str(s: &str) -> Option<Geometry> {
    sonic_rs::from_str(s).ok()
}

// ── Geo math functions ──

const EARTH_RADIUS_M: f64 = 6_371_000.0;

/// Haversine distance between two points in meters.
///
/// Input: (lng1, lat1) and (lng2, lat2) in degrees.
pub fn haversine_distance(lng1: f64, lat1: f64, lng2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}

/// Haversine bearing from point A to point B in degrees (0-360).
pub fn haversine_bearing(lng1: f64, lat1: f64, lng2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlng = (lng2 - lng1).to_radians();

    let y = dlng.sin() * lat2_r.cos();
    let x = lat1_r.cos() * lat2_r.sin() - lat1_r.sin() * lat2_r.cos() * dlng.cos();
    let bearing = y.atan2(x).to_degrees();
    (bearing + 360.0) % 360.0
}

/// Check if a point is inside a polygon (ray casting algorithm).
pub fn point_in_polygon(lng: f64, lat: f64, ring: &[[f64; 2]]) -> bool {
    let mut inside = false;
    let n = ring.len();
    let mut j = n.wrapping_sub(1);
    for i in 0..n {
        let yi = ring[i][1];
        let yj = ring[j][1];
        if ((yi > lat) != (yj > lat))
            && (lng < (ring[j][0] - ring[i][0]) * (lat - yi) / (yj - yi) + ring[i][0])
        {
            inside = !inside;
        }
        j = i;
    }
    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_creation() {
        let p = Geometry::point(-73.9857, 40.7484);
        assert_eq!(p.geometry_type(), "Point");
        if let Geometry::Point { coordinates } = &p {
            assert!((coordinates[0] - (-73.9857)).abs() < 1e-6);
            assert!((coordinates[1] - 40.7484).abs() < 1e-6);
        }
    }

    #[test]
    fn haversine_nyc_to_london() {
        // NYC: -74.006, 40.7128 → London: -0.1278, 51.5074
        let d = haversine_distance(-74.006, 40.7128, -0.1278, 51.5074);
        // ~5,570 km
        assert!((d - 5_570_000.0).abs() < 50_000.0, "got {d}m");
    }

    #[test]
    fn haversine_same_point() {
        let d = haversine_distance(0.0, 0.0, 0.0, 0.0);
        assert!(d.abs() < 1e-6);
    }

    #[test]
    fn bearing_north() {
        let b = haversine_bearing(0.0, 0.0, 0.0, 1.0);
        assert!((b - 0.0).abs() < 1.0, "expected ~0, got {b}");
    }

    #[test]
    fn bearing_east() {
        let b = haversine_bearing(0.0, 0.0, 1.0, 0.0);
        assert!((b - 90.0).abs() < 1.0, "expected ~90, got {b}");
    }

    #[test]
    fn point_in_polygon_inside() {
        let ring = vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ];
        assert!(point_in_polygon(5.0, 5.0, &ring));
        assert!(!point_in_polygon(15.0, 5.0, &ring));
    }

    #[test]
    fn geojson_serialize() {
        let p = Geometry::point(1.0, 2.0);
        let json = sonic_rs::to_string(&p).unwrap();
        assert!(json.contains("\"type\":\"Point\""));
        assert!(json.contains("\"coordinates\":[1.0,2.0]"));
    }

    #[test]
    fn geojson_roundtrip() {
        let original = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]]);
        let json = sonic_rs::to_string(&original).unwrap();
        let parsed: Geometry = sonic_rs::from_str(&json).unwrap();
        assert_eq!(original, parsed);
    }
}
