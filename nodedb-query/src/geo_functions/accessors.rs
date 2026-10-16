// SPDX-License-Identifier: Apache-2.0

//! Readers over a geometry's own contents: ordinates, type name, vertex count,
//! and the WKT / GeoJSON renderings. The SRID reader lives in `crs`.

use nodedb_types::Value;
use nodedb_types::geometry::{Geometry, SridGeometry};

use super::helpers::{GeoEval, geom_arg};
use crate::value_ops::to_value_number;

pub(super) fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
    let result = match canonical {
        "st_x" => ordinate(args, 0),
        "st_y" => ordinate(args, 1),
        // WKT stays plain: it has no SRID slot.
        "st_astext" => unary(args, |g| {
            Value::String(nodedb_spatial::geometry_to_wkt(&g.geometry))
        }),
        // GeoJSON carries the `crs` member for any SRID other than 4326.
        "st_asgeojson" => unary(args, |g| match sonic_rs::to_string(g) {
            Ok(json) => Value::String(json),
            Err(_) => Value::Null,
        }),
        "st_geometrytype" => unary(args, |g| {
            Value::String(g.geometry.geometry_type().to_string())
        }),
        "st_npoints" => unary(args, |g| Value::Integer(count_points(&g.geometry) as i64)),
        _ => return None,
    };
    Some(Ok(result))
}

fn unary(args: &[Value], f: fn(&SridGeometry) -> Value) -> Value {
    match geom_arg(args, 0) {
        Some(geom) => f(&geom),
        None => Value::Null,
    }
}

/// Read one ordinate of a point. PostGIS defines `ST_X`/`ST_Y` on points only;
/// any other geometry yields NULL rather than an arbitrary vertex.
fn ordinate(args: &[Value], index: usize) -> Value {
    match geom_arg(args, 0).map(|g| g.geometry) {
        Some(Geometry::Point { coordinates }) => to_value_number(coordinates[index]),
        _ => Value::Null,
    }
}

fn count_points(geom: &Geometry) -> usize {
    match geom {
        Geometry::Point { .. } => 1,
        Geometry::LineString { coordinates } | Geometry::MultiPoint { coordinates } => {
            coordinates.len()
        }
        Geometry::Polygon { coordinates } | Geometry::MultiLineString { coordinates } => {
            coordinates.iter().map(|ring| ring.len()).sum()
        }
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .flat_map(|poly| poly.iter())
            .map(|ring| ring.len())
            .sum(),
        Geometry::GeometryCollection { geometries } => geometries.iter().map(count_points).sum(),
        // `Geometry` is `#[non_exhaustive]`; a kind added upstream exposes no
        // coordinates this crate can count.
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The evaluator with its infallible accessors unwrapped.
    fn eval(canonical: &'static str, args: &[Value]) -> Option<Value> {
        super::eval(canonical, args).map(|r| r.expect("accessors never fail"))
    }

    fn geom(v: Geometry) -> Vec<Value> {
        vec![Value::Geometry(SridGeometry::wgs84(v))]
    }

    #[test]
    fn ordinates_read_a_point() {
        let point = geom(Geometry::point(1.0, 2.0));
        assert_eq!(eval("st_x", &point), Some(to_value_number(1.0)));
        assert_eq!(eval("st_y", &point), Some(to_value_number(2.0)));
    }

    /// A non-point has no single X; returning a vertex would be a guess.
    #[test]
    fn ordinates_of_a_non_point_are_null() {
        let line = geom(Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]));
        assert_eq!(eval("st_x", &line), Some(Value::Null));
        assert_eq!(eval("st_y", &line), Some(Value::Null));
    }

    #[test]
    fn astext_renders_wkt() {
        let Some(Value::String(wkt)) = eval("st_astext", &geom(Geometry::point(1.0, 2.0))) else {
            panic!("expected WKT text");
        };
        assert!(wkt.to_uppercase().starts_with("POINT"), "got {wkt}");
        assert!(wkt.contains('1') && wkt.contains('2'), "got {wkt}");
    }

    #[test]
    fn asgeojson_renders_geojson() {
        let Some(Value::String(json)) = eval("st_asgeojson", &geom(Geometry::point(1.0, 2.0)))
        else {
            panic!("expected GeoJSON text");
        };
        assert!(
            json.contains("\"Point\"") && json.contains("coordinates"),
            "got {json}"
        );
    }

    /// The two renderings must describe the same geometry — a reader that
    /// round-trips one must get the other's geometry back.
    #[test]
    fn wkt_and_geojson_renderings_agree() {
        let original = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]);
        let Some(Value::String(wkt)) = eval("st_astext", &geom(original.clone())) else {
            panic!("expected WKT");
        };
        let Some(Value::String(json)) = eval("st_asgeojson", &geom(original.clone())) else {
            panic!("expected GeoJSON");
        };
        assert_eq!(
            super::super::helpers::geometry_from_text(&wkt),
            Some(SridGeometry::wgs84(original.clone()))
        );
        assert_eq!(
            super::super::helpers::geometry_from_text(&json),
            Some(SridGeometry::wgs84(original))
        );
    }

    /// GeoJSON keeps a projected SRID in its `crs` member; WKT stays plain.
    #[test]
    fn projected_renderings() {
        let g = SridGeometry::new(
            nodedb_types::geometry::Srid::Utm48N,
            Geometry::point(1.0, 2.0),
        );
        let args = vec![Value::Geometry(g.clone())];
        let Some(Value::String(json)) = eval("st_asgeojson", &args) else {
            panic!("expected GeoJSON");
        };
        assert!(json.contains("EPSG:32648"), "{json}");
        assert_eq!(super::super::helpers::geometry_from_text(&json), Some(g));
        let Some(Value::String(wkt)) = eval("st_astext", &args) else {
            panic!("expected WKT");
        };
        assert!(!wkt.contains("SRID"), "{wkt}");
    }

    #[test]
    fn geometrytype_names_the_variant() {
        assert_eq!(
            eval("st_geometrytype", &geom(Geometry::point(0.0, 0.0))),
            Some(Value::String("Point".into()))
        );
    }

    #[test]
    fn npoints_counts_every_vertex() {
        let line = geom(Geometry::line_string(vec![
            [0.0, 0.0],
            [1.0, 1.0],
            [2.0, 2.0],
        ]));
        assert_eq!(eval("st_npoints", &line), Some(Value::Integer(3)));
    }

    #[test]
    fn unreadable_geometry_yields_null() {
        let bad = vec![Value::String("not a geometry".into())];
        assert_eq!(eval("st_astext", &bad), Some(Value::Null));
        assert_eq!(eval("st_npoints", &bad), Some(Value::Null));
    }

    /// A WKT string argument is a geometry, so accessors read it directly.
    #[test]
    fn wkt_string_argument_is_accepted() {
        let wkt = vec![Value::String("POINT(3 4)".into())];
        assert_eq!(eval("st_x", &wkt), Some(to_value_number(3.0)));
        assert_eq!(eval("st_y", &wkt), Some(to_value_number(4.0)));
    }
}
