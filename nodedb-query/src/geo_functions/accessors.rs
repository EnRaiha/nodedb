// SPDX-License-Identifier: Apache-2.0

//! Readers over a geometry's own contents: ordinates, type name, vertex count,
//! reference system, and the WKT / GeoJSON renderings.

use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

use super::helpers::geom_arg;
use crate::value_ops::to_value_number;

/// Geometry in NodeDB is stored as GeoJSON, whose coordinate reference system
/// is defined by RFC 7946 to be WGS 84 — EPSG:4326. There is no per-geometry
/// SRID to carry, so every stored geometry reports the same one.
const WGS84_SRID: i64 = 4326;

pub(super) fn eval(canonical: &str, args: &[Value]) -> Option<Value> {
    let result = match canonical {
        "st_x" => ordinate(args, 0),
        "st_y" => ordinate(args, 1),
        "st_astext" => unary(args, |g| Value::String(nodedb_spatial::geometry_to_wkt(g))),
        "st_asgeojson" => unary(args, |g| match sonic_rs::to_string(g) {
            Ok(json) => Value::String(json),
            Err(_) => Value::Null,
        }),
        "st_geometrytype" => unary(args, |g| Value::String(g.geometry_type().to_string())),
        "st_npoints" => unary(args, |g| Value::Integer(count_points(g) as i64)),
        "st_srid" => unary(args, |_| Value::Integer(WGS84_SRID)),
        _ => return None,
    };
    Some(result)
}

fn unary(args: &[Value], f: fn(&Geometry) -> Value) -> Value {
    match geom_arg(args, 0) {
        Some(geom) => f(&geom),
        None => Value::Null,
    }
}

/// Read one ordinate of a point. PostGIS defines `ST_X`/`ST_Y` on points only;
/// any other geometry yields NULL rather than an arbitrary vertex.
fn ordinate(args: &[Value], index: usize) -> Value {
    match geom_arg(args, 0) {
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

    fn geom(v: Geometry) -> Vec<Value> {
        vec![Value::Geometry(v)]
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
            Some(original.clone())
        );
        assert_eq!(
            super::super::helpers::geometry_from_text(&json),
            Some(original)
        );
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
    fn srid_reports_wgs84() {
        assert_eq!(
            eval("st_srid", &geom(Geometry::point(0.0, 0.0))),
            Some(Value::Integer(4326))
        );
    }

    #[test]
    fn unreadable_geometry_yields_null() {
        let bad = vec![Value::String("not a geometry".into())];
        assert_eq!(eval("st_astext", &bad), Some(Value::Null));
        assert_eq!(eval("st_npoints", &bad), Some(Value::Null));
        assert_eq!(eval("st_srid", &bad), Some(Value::Null));
    }

    /// A WKT string argument is a geometry, so accessors read it directly.
    #[test]
    fn wkt_string_argument_is_accepted() {
        let wkt = vec![Value::String("POINT(3 4)".into())];
        assert_eq!(eval("st_x", &wkt), Some(to_value_number(3.0)));
        assert_eq!(eval("st_y", &wkt), Some(to_value_number(4.0)));
    }
}
