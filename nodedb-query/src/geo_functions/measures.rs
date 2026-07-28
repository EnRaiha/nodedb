// SPDX-License-Identifier: Apache-2.0

//! Distance, length, perimeter, area, and bearing.
//!
//! Every measure is geodesic and reported in meters (or square meters), the
//! same basis as `nodedb_spatial::st_distance`.

use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

use super::helpers::{geom_arg, num_arg};
use crate::value_ops::to_value_number;

pub(super) fn eval(canonical: &str, args: &[Value]) -> Option<Value> {
    let result = match canonical {
        "st_distance" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(Value::Null);
            };
            to_value_number(nodedb_spatial::st_distance(&a, &b))
        }
        "st_length" => unary(args, linear_length),
        "st_perimeter" => unary(args, perimeter),
        "st_area" => unary(args, nodedb_spatial::st_area),
        "geo_distance" => haversine(args, nodedb_types::geometry::haversine_distance),
        "geo_bearing" => haversine(args, nodedb_types::geometry::haversine_bearing),
        _ => return None,
    };
    Some(result)
}

fn unary(args: &[Value], f: fn(&Geometry) -> f64) -> Value {
    match geom_arg(args, 0) {
        Some(geom) => to_value_number(f(&geom)),
        None => Value::Null,
    }
}

fn haversine(args: &[Value], f: fn(f64, f64, f64, f64) -> f64) -> Value {
    let (Some(lng1), Some(lat1), Some(lng2), Some(lat2)) = (
        num_arg(args, 0),
        num_arg(args, 1),
        num_arg(args, 2),
        num_arg(args, 3),
    ) else {
        return Value::Null;
    };
    to_value_number(f(lng1, lat1, lng2, lat2))
}

/// Total geodesic length of every linear component. Areal geometries have no
/// length in PostGIS (their boundary is `ST_Perimeter`), and points have none.
fn linear_length(geom: &Geometry) -> f64 {
    match geom {
        Geometry::LineString { coordinates } => path_length(coordinates),
        Geometry::MultiLineString { coordinates } => {
            coordinates.iter().map(|line| path_length(line)).sum()
        }
        Geometry::GeometryCollection { geometries } => geometries.iter().map(linear_length).sum(),
        _ => 0.0,
    }
}

/// Total geodesic length of every areal component's rings, holes included —
/// the boundary length of the geometry.
fn perimeter(geom: &Geometry) -> f64 {
    match geom {
        Geometry::Polygon { coordinates } => coordinates
            .iter()
            .map(|ring| closed_ring_length(ring))
            .sum(),
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .flat_map(|rings| rings.iter())
            .map(|ring| closed_ring_length(ring))
            .sum(),
        Geometry::GeometryCollection { geometries } => geometries.iter().map(perimeter).sum(),
        _ => 0.0,
    }
}

fn path_length(coords: &[[f64; 2]]) -> f64 {
    coords
        .windows(2)
        .map(|pair| {
            nodedb_types::geometry::haversine_distance(
                pair[0][0], pair[0][1], pair[1][0], pair[1][1],
            )
        })
        .sum()
}

/// Length of a ring, closing it if the source omitted the repeated last
/// vertex — GeoJSON requires closure but stored data is not always well-formed.
fn closed_ring_length(ring: &[[f64; 2]]) -> f64 {
    let mut total = path_length(ring);
    if let (Some(first), Some(last)) = (ring.first(), ring.last())
        && first != last
    {
        total += nodedb_types::geometry::haversine_distance(last[0], last[1], first[0], first[1]);
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value_ops::value_to_f64;

    fn geom(v: Geometry) -> Vec<Value> {
        vec![Value::Geometry(v)]
    }

    #[test]
    fn length_measures_linestrings_only() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [0.0, 1.0]]);
        let measured = eval("st_length", &geom(line)).and_then(|v| value_to_f64(&v, true));
        // One degree of latitude is ~111.19 km.
        let Some(m) = measured else {
            panic!("expected a numeric length");
        };
        assert!((m - 111_195.0).abs() < 500.0, "got {m}");

        let point = geom(Geometry::point(1.0, 2.0));
        assert_eq!(eval("st_length", &point), Some(to_value_number(0.0)));
    }

    #[test]
    fn perimeter_closes_an_unclosed_ring() {
        let closed = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
            [1.0, 0.0],
            [0.0, 0.0],
        ]]);
        let unclosed =
            Geometry::polygon(vec![vec![[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0]]]);
        assert_eq!(
            eval("st_perimeter", &geom(closed)),
            eval("st_perimeter", &geom(unclosed)),
            "an unclosed ring must measure the same boundary as its closed form"
        );
    }

    #[test]
    fn area_of_a_line_is_zero() {
        let line = geom(Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]));
        assert_eq!(eval("st_area", &line), Some(to_value_number(0.0)));
    }

    #[test]
    fn unreadable_geometry_yields_null_not_zero() {
        let bad = vec![Value::String("not a geometry".into())];
        assert_eq!(eval("st_area", &bad), Some(Value::Null));
        assert_eq!(eval("st_length", &bad), Some(Value::Null));
        assert_eq!(eval("st_distance", &bad), Some(Value::Null));
    }

    #[test]
    fn unknown_name_falls_through() {
        assert_eq!(eval("st_contains", &[]), None);
    }
}
