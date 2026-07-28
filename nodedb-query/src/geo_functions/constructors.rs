// SPDX-License-Identifier: Apache-2.0

//! Geometry constructors and geometry-returning operations.

use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

use super::helpers::{geom_arg, geometry_from_text, num_arg, str_arg};

/// Default segment count for the circular approximations `ST_Buffer` and
/// `geo_circle` produce when the caller does not specify one.
const DEFAULT_BUFFER_SEGMENTS: usize = 32;

pub(super) fn eval(canonical: &str, args: &[Value]) -> Option<Value> {
    let result = match canonical {
        "st_point" => point(args),
        "st_makepoint" => point(args),
        "st_geomfromtext" | "st_geomfromgeojson" => match str_arg(args, 0) {
            Some(text) => match geometry_from_text(&text) {
                Some(geom) => Value::Geometry(geom),
                None => Value::Null,
            },
            None => Value::Null,
        },
        "st_geomfromwkb" => match wkb_bytes(args) {
            Some(bytes) => match nodedb_spatial::geometry_from_wkb(&bytes) {
                Some(geom) => Value::Geometry(geom),
                None => Value::Null,
            },
            None => Value::Null,
        },
        "st_makeline" => make_line(args),
        "st_makepolygon" => make_polygon(args),
        "st_makeenvelope" => make_envelope(args),
        "geo_circle" => circle(args),
        "st_buffer" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            let Some(distance) = num_arg(args, 1) else {
                return Some(Value::Null);
            };
            let segments = segments_arg(args, 2);
            Value::Geometry(nodedb_spatial::st_buffer(&geom, distance, segments))
        }
        "st_envelope" => unary(args, |g| Some(nodedb_spatial::st_envelope(g))),
        "st_centroid" => unary(args, nodedb_spatial::st_centroid),
        "st_union" => binary(args, nodedb_spatial::st_union),
        "st_intersection" => binary(args, nodedb_spatial::st_intersection),
        _ => return None,
    };
    Some(result)
}

fn unary(args: &[Value], f: fn(&Geometry) -> Option<Geometry>) -> Value {
    match geom_arg(args, 0).as_ref().and_then(f) {
        Some(geom) => Value::Geometry(geom),
        None => Value::Null,
    }
}

fn binary(args: &[Value], f: fn(&Geometry, &Geometry) -> Geometry) -> Value {
    let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
        return Value::Null;
    };
    Value::Geometry(f(&a, &b))
}

/// Segment count for a circular approximation, floored at 3 — fewer than three
/// segments cannot enclose an area, and a caller asking for 0 would otherwise
/// silently receive a degenerate geometry.
fn segments_arg(args: &[Value], idx: usize) -> usize {
    num_arg(args, idx)
        .filter(|n| n.is_finite() && *n >= 3.0)
        .map_or(DEFAULT_BUFFER_SEGMENTS, |n| n as usize)
}

/// WKB bytes for `ST_GeomFromWKB`, from either an `X'...'` byte literal or the
/// equivalent hex string. Both spellings reach this function — a client that
/// cannot emit a byte literal passes the same bytes as text.
fn wkb_bytes(args: &[Value]) -> Option<Vec<u8>> {
    match args.first()? {
        Value::Bytes(bytes) => Some(bytes.clone()),
        Value::String(hex) => decode_hex(hex.trim()),
        _ => None,
    }
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

fn point(args: &[Value]) -> Value {
    let (Some(lng), Some(lat)) = (num_arg(args, 0), num_arg(args, 1)) else {
        return Value::Null;
    };
    Value::Geometry(Geometry::point(lng, lat))
}

fn circle(args: &[Value]) -> Value {
    let (Some(lng), Some(lat), Some(radius)) =
        (num_arg(args, 0), num_arg(args, 1), num_arg(args, 2))
    else {
        return Value::Null;
    };
    let segments = segments_arg(args, 3);
    Value::Geometry(nodedb_spatial::st_buffer(
        &Geometry::point(lng, lat),
        radius,
        segments,
    ))
}

/// `ST_MakeLine(point, point, ...)` — every argument must be a readable point;
/// a single unreadable one makes the whole line NULL rather than silently
/// dropping a vertex and shortening the line.
fn make_line(args: &[Value]) -> Value {
    let mut coords = Vec::with_capacity(args.len());
    for idx in 0..args.len() {
        match geom_arg(args, idx) {
            Some(Geometry::Point { coordinates }) => coords.push(coordinates),
            _ => return Value::Null,
        }
    }
    if coords.len() < 2 {
        return Value::Null;
    }
    Value::Geometry(Geometry::line_string(coords))
}

/// `ST_MakePolygon(ring, ...)` — each argument is an array of coordinate
/// pairs. As with `ST_MakeLine`, a malformed vertex fails the whole call.
fn make_polygon(args: &[Value]) -> Value {
    let mut rings = Vec::with_capacity(args.len());
    for arg in args {
        let Some(points) = arg.as_array() else {
            return Value::Null;
        };
        let mut ring = Vec::with_capacity(points.len());
        for point in points {
            let Some(pair) = point.as_array() else {
                return Value::Null;
            };
            let (Some(lng), Some(lat)) = (
                pair.first().and_then(Value::as_f64),
                pair.get(1).and_then(Value::as_f64),
            ) else {
                return Value::Null;
            };
            ring.push([lng, lat]);
        }
        if ring.is_empty() {
            return Value::Null;
        }
        rings.push(ring);
    }
    if rings.is_empty() {
        return Value::Null;
    }
    Value::Geometry(Geometry::polygon(rings))
}

fn make_envelope(args: &[Value]) -> Value {
    let (Some(min_lng), Some(min_lat), Some(max_lng), Some(max_lat)) = (
        num_arg(args, 0),
        num_arg(args, 1),
        num_arg(args, 2),
        num_arg(args, 3),
    ) else {
        return Value::Null;
    };
    Value::Geometry(Geometry::polygon(vec![vec![
        [min_lng, min_lat],
        [max_lng, min_lat],
        [max_lng, max_lat],
        [min_lng, max_lat],
        [min_lng, min_lat],
    ]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_constructors_agree() {
        let args = vec![Value::Float(1.0), Value::Float(2.0)];
        let expected = Some(Value::Geometry(Geometry::point(1.0, 2.0)));
        assert_eq!(eval("st_point", &args), expected);
        assert_eq!(eval("st_makepoint", &args), expected);
    }

    #[test]
    fn geomfromtext_accepts_wkt_and_geojson() {
        let wkt = vec![Value::String("POINT(1 2)".into())];
        let json = vec![Value::String(
            r#"{"type":"Point","coordinates":[1.0,2.0]}"#.into(),
        )];
        let expected = Some(Value::Geometry(Geometry::point(1.0, 2.0)));
        assert_eq!(eval("st_geomfromtext", &wkt), expected);
        assert_eq!(eval("st_geomfromgeojson", &json), expected);
    }

    /// A byte literal and its hex-string spelling must decode identically.
    #[test]
    fn geomfromwkb_accepts_bytes_and_hex_string() {
        // WKB for POINT(1 2): little-endian, type 1, x=1.0, y=2.0.
        let hex = "0101000000000000000000F03F0000000000000040";
        let bytes = decode_hex(hex).expect("test vector must decode");
        let from_bytes = eval("st_geomfromwkb", &[Value::Bytes(bytes)]);
        let from_hex = eval("st_geomfromwkb", &[Value::String(hex.into())]);
        assert_eq!(from_bytes, from_hex);
        assert_eq!(from_bytes, Some(Value::Geometry(Geometry::point(1.0, 2.0))));
    }

    #[test]
    fn malformed_hex_yields_null() {
        assert_eq!(
            eval("st_geomfromwkb", &[Value::String("zz".into())]),
            Some(Value::Null)
        );
        assert_eq!(
            eval("st_geomfromwkb", &[Value::String("010".into())]),
            Some(Value::Null)
        );
    }

    #[test]
    fn malformed_text_yields_null() {
        let bad = vec![Value::String("POINT(".into())];
        assert_eq!(eval("st_geomfromtext", &bad), Some(Value::Null));
    }

    /// A dropped vertex would silently shorten the line; the call must fail
    /// whole instead.
    #[test]
    fn makeline_rejects_a_non_point_argument() {
        let args = vec![
            Value::Geometry(Geometry::point(0.0, 0.0)),
            Value::String("not a geometry".into()),
        ];
        assert_eq!(eval("st_makeline", &args), Some(Value::Null));
    }

    #[test]
    fn makeline_needs_at_least_two_points() {
        let one = vec![Value::Geometry(Geometry::point(0.0, 0.0))];
        assert_eq!(eval("st_makeline", &one), Some(Value::Null));
    }

    #[test]
    fn makeenvelope_closes_its_ring() {
        let args = vec![
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(1.0),
            Value::Float(1.0),
        ];
        let Some(Value::Geometry(Geometry::Polygon { coordinates })) =
            eval("st_makeenvelope", &args)
        else {
            panic!("expected a polygon");
        };
        let ring = &coordinates[0];
        assert_eq!(ring.first(), ring.last(), "envelope ring must be closed");
    }

    /// A caller asking for a degenerate segment count must get the default,
    /// not a geometry that encloses nothing.
    #[test]
    fn buffer_segment_count_is_floored() {
        let args = vec![
            Value::Geometry(Geometry::point(0.0, 0.0)),
            Value::Float(1000.0),
            Value::Integer(0),
        ];
        let Some(Value::Geometry(Geometry::Polygon { coordinates })) = eval("st_buffer", &args)
        else {
            panic!("expected a buffered polygon");
        };
        assert!(
            coordinates[0].len() > 3,
            "a zero segment count must fall back to the default"
        );
    }

    #[test]
    fn centroid_of_unreadable_geometry_is_null() {
        let bad = vec![Value::String("not a geometry".into())];
        assert_eq!(eval("st_centroid", &bad), Some(Value::Null));
    }

    #[test]
    fn unknown_name_falls_through() {
        assert_eq!(eval("st_x", &[]), None);
    }
}
