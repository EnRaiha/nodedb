// SPDX-License-Identifier: Apache-2.0

//! GeoJSON-compatible geometry shapes.
//!
//! Supports Point, LineString, Polygon, MultiPoint, MultiLineString,
//! MultiPolygon, and GeometryCollection. A shape holds coordinates only. The
//! reference system those coordinates are in lives on
//! [`SridGeometry`](super::SridGeometry).

use serde::{Deserialize, Serialize};

/// A 2D coordinate (x, y) following GeoJSON order.
///
/// In a geographic reference system x is longitude and y is latitude, in
/// degrees. In a projected one x is easting and y is northing, in metres.
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
    /// Create a Point from (x, y).
    pub fn point(lng: f64, lat: f64) -> Self {
        Geometry::Point {
            coordinates: [lng, lat],
        }
    }

    /// Create a LineString from a series of [x, y] pairs.
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

    /// Map every coordinate through `f`, keeping the shape.
    ///
    /// Stops at the first coordinate `f` rejects and returns its error.
    pub fn try_map_coords<E>(
        &self,
        f: &mut impl FnMut([f64; 2]) -> Result<[f64; 2], E>,
    ) -> Result<Geometry, E> {
        Ok(match self {
            Geometry::Point { coordinates } => Geometry::Point {
                coordinates: f(*coordinates)?,
            },
            Geometry::LineString { coordinates } => Geometry::LineString {
                coordinates: map_path(coordinates, f)?,
            },
            Geometry::Polygon { coordinates } => Geometry::Polygon {
                coordinates: map_rings(coordinates, f)?,
            },
            Geometry::MultiPoint { coordinates } => Geometry::MultiPoint {
                coordinates: map_path(coordinates, f)?,
            },
            Geometry::MultiLineString { coordinates } => Geometry::MultiLineString {
                coordinates: map_rings(coordinates, f)?,
            },
            Geometry::MultiPolygon { coordinates } => Geometry::MultiPolygon {
                coordinates: coordinates
                    .iter()
                    .map(|polygon| map_rings(polygon, f))
                    .collect::<Result<_, E>>()?,
            },
            Geometry::GeometryCollection { geometries } => Geometry::GeometryCollection {
                geometries: geometries
                    .iter()
                    .map(|g| g.try_map_coords(f))
                    .collect::<Result<_, E>>()?,
            },
        })
    }
}

fn map_path<E>(
    path: &[[f64; 2]],
    f: &mut impl FnMut([f64; 2]) -> Result<[f64; 2], E>,
) -> Result<Vec<[f64; 2]>, E> {
    path.iter().map(|c| f(*c)).collect()
}

fn map_rings<E>(
    rings: &[Vec<[f64; 2]>],
    f: &mut impl FnMut([f64; 2]) -> Result<[f64; 2], E>,
) -> Result<Vec<Vec<[f64; 2]>>, E> {
    rings.iter().map(|ring| map_path(ring, f)).collect()
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

    #[test]
    fn try_map_coords_keeps_shape() {
        let gc = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(1.0, 2.0),
                Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]]]),
            ],
        };
        let shifted = gc
            .try_map_coords(&mut |[x, y]| Ok::<_, ()>([x + 10.0, y]))
            .unwrap();
        let Geometry::GeometryCollection { geometries } = shifted else {
            panic!("shape changed");
        };
        assert_eq!(geometries[0], Geometry::point(11.0, 2.0));
    }

    #[test]
    fn try_map_coords_stops_at_first_error() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]);
        let r = line.try_map_coords(&mut |[x, _]| if x > 0.5 { Err(x) } else { Ok([x, 0.0]) });
        assert_eq!(r, Err(1.0));
    }
}
