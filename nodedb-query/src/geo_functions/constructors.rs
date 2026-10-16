// SPDX-License-Identifier: Apache-2.0

//! Geometry constructors and geometry-returning operations.
//!
//! A constructor from bare numbers builds a WGS 84 geometry. An operation on
//! geometries keeps its input SRID. A binary operation needs both inputs in
//! one SRID.

use nodedb_types::Value;
use nodedb_types::geometry::{Geometry, Srid, SridGeometry};

use super::helpers::{
    GeoEval, SridArg, geom_arg, num_arg, parse_text_geometry, same_srid, srid_arg, str_arg,
    text_fault,
};
use crate::expr::EvalError;

/// Default segment count for the circular approximations `ST_Buffer` and
/// `geo_circle` produce when the caller does not specify one.
const DEFAULT_BUFFER_SEGMENTS: usize = 32;

pub(super) fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
    let result = match canonical {
        "st_point" | "st_makepoint" => Ok(point(args)),
        "st_geomfromtext" => from_text(canonical, args, true),
        "st_geomfromgeojson" => from_text(canonical, args, false),
        "st_geomfromwkb" => from_wkb(canonical, args),
        "st_makeline" => make_line(canonical, args),
        "st_makepolygon" => Ok(make_polygon(args)),
        "st_makeenvelope" => Ok(make_envelope(args)),
        "geo_circle" => Ok(circle(args)),
        "st_buffer" => Ok(buffer(args)),
        "st_envelope" => Ok(unary(args, |g| {
            Some(nodedb_spatial::st_envelope_in(
                &g.geometry,
                g.srid.bbox_space(),
            ))
        })),
        "st_centroid" => Ok(unary(args, |g| {
            nodedb_spatial::st_centroid_in(&g.geometry, g.srid.bbox_space())
        })),
        "st_union" => binary(canonical, args, nodedb_spatial::st_union),
        "st_intersection" => binary(canonical, args, nodedb_spatial::st_intersection),
        _ => return None,
    };
    Some(result)
}

/// A unary geometry operation whose result keeps the input SRID.
fn unary(args: &[Value], f: fn(&SridGeometry) -> Option<Geometry>) -> Value {
    match geom_arg(args, 0) {
        Some(g) => match f(&g) {
            Some(out) => Value::Geometry(g.with_geometry(out)),
            None => Value::Null,
        },
        None => Value::Null,
    }
}

/// A binary geometry operation over two inputs in one SRID.
fn binary(
    function: &'static str,
    args: &[Value],
    f: fn(&Geometry, &Geometry) -> Geometry,
) -> Result<Value, EvalError> {
    let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
        return Ok(Value::Null);
    };
    let srid = same_srid(function, &a, &b)?;
    Ok(Value::Geometry(SridGeometry::new(
        srid,
        f(&a.geometry, &b.geometry),
    )))
}

/// `ST_GeomFromText(text [, srid])` and `ST_GeomFromGeoJSON(text)`. A GeoJSON
/// text keeps the SRID its `crs` member names. An explicit SRID argument
/// relabels the result.
fn from_text(function: &'static str, args: &[Value], takes_srid: bool) -> Result<Value, EvalError> {
    let explicit = if takes_srid {
        srid_arg(args, 1, function)?
    } else {
        SridArg::Absent
    };
    let Some(text) = str_arg(args, 0) else {
        return Ok(Value::Null);
    };
    let mut geom = match parse_text_geometry(&text) {
        Ok(g) => g,
        Err(e) => return text_fault(function, e),
    };
    match explicit {
        SridArg::Absent => {}
        SridArg::Null => return Ok(Value::Null),
        SridArg::Srid(srid) => geom.srid = srid,
    }
    Ok(Value::Geometry(geom))
}

/// `ST_GeomFromWKB(wkb [, srid])`. An explicit SRID argument wins over the
/// SRID an EWKB header carries. ISO WKB with no SRID argument is WGS 84.
fn from_wkb(function: &'static str, args: &[Value]) -> Result<Value, EvalError> {
    let explicit = srid_arg(args, 1, function)?;
    let Some(bytes) = wkb_bytes(args) else {
        return Ok(Value::Null);
    };
    let Some((geometry, header_srid)) = nodedb_spatial::geometry_from_wkb(&bytes) else {
        return Ok(Value::Null);
    };
    let srid = match (explicit, header_srid) {
        (SridArg::Null, _) => return Ok(Value::Null),
        (SridArg::Srid(srid), _) => srid,
        (SridArg::Absent, Some(code)) => {
            Srid::from_code(i64::from(code)).map_err(|e| EvalError::UnsupportedSrid {
                function,
                srid: e.code,
            })?
        }
        (SridArg::Absent, None) => Srid::Wgs84,
    };
    Ok(Value::Geometry(SridGeometry::new(srid, geometry)))
}

/// `ST_Buffer(geom, distance [, segments])`. Distance is metres in either
/// space: geodesic-approximate for 4326, planar for a projected SRID.
fn buffer(args: &[Value]) -> Value {
    let (Some(g), Some(distance)) = (geom_arg(args, 0), num_arg(args, 1)) else {
        return Value::Null;
    };
    let segments = segments_arg(args, 2);
    let out = if g.srid.is_geographic() {
        nodedb_spatial::st_buffer(&g.geometry, distance, segments)
    } else {
        nodedb_spatial::planar_buffer(&g.geometry, distance, segments)
    };
    Value::Geometry(g.with_geometry(out))
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
    Value::Geometry(SridGeometry::wgs84(Geometry::point(lng, lat)))
}

fn circle(args: &[Value]) -> Value {
    let (Some(lng), Some(lat), Some(radius)) =
        (num_arg(args, 0), num_arg(args, 1), num_arg(args, 2))
    else {
        return Value::Null;
    };
    let segments = segments_arg(args, 3);
    Value::Geometry(SridGeometry::wgs84(nodedb_spatial::st_buffer(
        &Geometry::point(lng, lat),
        radius,
        segments,
    )))
}

/// `ST_MakeLine(point, point, ...)` — every argument must be a readable point;
/// a single unreadable one makes the whole line NULL rather than silently
/// dropping a vertex and shortening the line. Every point must share one
/// SRID, which the line keeps.
fn make_line(function: &'static str, args: &[Value]) -> Result<Value, EvalError> {
    let mut coords = Vec::with_capacity(args.len());
    let mut first: Option<SridGeometry> = None;
    for idx in 0..args.len() {
        let Some(g) = geom_arg(args, idx) else {
            return Ok(Value::Null);
        };
        let Geometry::Point { coordinates } = g.geometry else {
            return Ok(Value::Null);
        };
        if let Some(head) = &first {
            same_srid(function, head, &g)?;
        }
        coords.push(coordinates);
        first.get_or_insert(g);
    }
    if coords.len() < 2 {
        return Ok(Value::Null);
    }
    let srid = first.map_or(Srid::Wgs84, |g| g.srid);
    Ok(Value::Geometry(SridGeometry::new(
        srid,
        Geometry::line_string(coords),
    )))
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
    Value::Geometry(SridGeometry::wgs84(Geometry::polygon(rings)))
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
    Value::Geometry(SridGeometry::wgs84(Geometry::polygon(vec![vec![
        [min_lng, min_lat],
        [max_lng, min_lat],
        [max_lng, max_lat],
        [min_lng, max_lat],
        [min_lng, min_lat],
    ]])))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The evaluator, with a fault turned into a test failure.
    fn run(canonical: &'static str, args: &[Value]) -> Option<Value> {
        eval(canonical, args).map(|r| r.expect("no fault expected"))
    }

    fn wgs84(g: Geometry) -> Value {
        Value::Geometry(SridGeometry::wgs84(g))
    }

    fn projected(srid: Srid, g: Geometry) -> Value {
        Value::Geometry(SridGeometry::new(srid, g))
    }

    #[test]
    fn point_constructors_agree() {
        let args = vec![Value::Float(1.0), Value::Float(2.0)];
        let expected = Some(wgs84(Geometry::point(1.0, 2.0)));
        assert_eq!(run("st_point", &args), expected);
        assert_eq!(run("st_makepoint", &args), expected);
    }

    #[test]
    fn geomfromtext_accepts_wkt_and_geojson() {
        let wkt = vec![Value::String("POINT(1 2)".into())];
        let json = vec![Value::String(
            r#"{"type":"Point","coordinates":[1.0,2.0]}"#.into(),
        )];
        let expected = Some(wgs84(Geometry::point(1.0, 2.0)));
        assert_eq!(run("st_geomfromtext", &wkt), expected);
        assert_eq!(run("st_geomfromgeojson", &json), expected);
    }

    #[test]
    fn geomfromtext_srid_argument_sets_srid() {
        let args = vec![Value::String("POINT(1 2)".into()), Value::Integer(3375)];
        assert_eq!(
            run("st_geomfromtext", &args),
            Some(projected(Srid::PeninsularRso, Geometry::point(1.0, 2.0)))
        );
    }

    #[test]
    fn geomfromtext_unsupported_srid_fails() {
        let args = vec![Value::String("POINT(1 2)".into()), Value::Integer(2154)];
        assert_eq!(
            eval("st_geomfromtext", &args),
            Some(Err(EvalError::UnsupportedSrid {
                function: "st_geomfromtext",
                srid: 2154
            }))
        );
    }

    /// A byte literal and its hex-string spelling must decode identically.
    #[test]
    fn geomfromwkb_accepts_bytes_and_hex_string() {
        // WKB for POINT(1 2): little-endian, type 1, x=1.0, y=2.0.
        let hex = "0101000000000000000000F03F0000000000000040";
        let bytes = decode_hex(hex).expect("test vector must decode");
        let from_bytes = run("st_geomfromwkb", &[Value::Bytes(bytes)]);
        let from_hex = run("st_geomfromwkb", &[Value::String(hex.into())]);
        assert_eq!(from_bytes, from_hex);
        assert_eq!(from_bytes, Some(wgs84(Geometry::point(1.0, 2.0))));
    }

    #[test]
    fn geomfromwkb_reads_ewkb_srid_and_explicit_srid_wins() {
        let g = SridGeometry::new(Srid::Utm48N, Geometry::point(1.0, 2.0));
        let ewkb = nodedb_spatial::geometry_to_ewkb(&g).expect("encode");
        assert_eq!(
            run("st_geomfromwkb", &[Value::Bytes(ewkb.clone())]),
            Some(Value::Geometry(g))
        );
        assert_eq!(
            run(
                "st_geomfromwkb",
                &[Value::Bytes(ewkb), Value::Integer(3376)]
            ),
            Some(projected(Srid::BorneoRso, Geometry::point(1.0, 2.0)))
        );
    }

    #[test]
    fn malformed_hex_yields_null() {
        assert_eq!(
            run("st_geomfromwkb", &[Value::String("zz".into())]),
            Some(Value::Null)
        );
        assert_eq!(
            run("st_geomfromwkb", &[Value::String("010".into())]),
            Some(Value::Null)
        );
    }

    #[test]
    fn malformed_text_yields_null() {
        let bad = vec![Value::String("POINT(".into())];
        assert_eq!(run("st_geomfromtext", &bad), Some(Value::Null));
    }

    /// A dropped vertex would silently shorten the line; the call must fail
    /// whole instead.
    #[test]
    fn makeline_rejects_a_non_point_argument() {
        let args = vec![
            wgs84(Geometry::point(0.0, 0.0)),
            Value::String("not a geometry".into()),
        ];
        assert_eq!(run("st_makeline", &args), Some(Value::Null));
    }

    #[test]
    fn makeline_needs_at_least_two_points() {
        let one = vec![wgs84(Geometry::point(0.0, 0.0))];
        assert_eq!(run("st_makeline", &one), Some(Value::Null));
    }

    #[test]
    fn makeline_rejects_mixed_srids() {
        let args = vec![
            wgs84(Geometry::point(0.0, 0.0)),
            projected(Srid::Utm48N, Geometry::point(1.0, 1.0)),
        ];
        assert_eq!(
            eval("st_makeline", &args),
            Some(Err(EvalError::MixedSrid {
                function: "st_makeline",
                left: 4326,
                right: 32648
            }))
        );
    }

    #[test]
    fn makeenvelope_closes_its_ring() {
        let args = vec![
            Value::Float(0.0),
            Value::Float(0.0),
            Value::Float(1.0),
            Value::Float(1.0),
        ];
        let Some(Value::Geometry(SridGeometry {
            geometry: Geometry::Polygon { coordinates },
            ..
        })) = run("st_makeenvelope", &args)
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
            wgs84(Geometry::point(0.0, 0.0)),
            Value::Float(1000.0),
            Value::Integer(0),
        ];
        let Some(Value::Geometry(SridGeometry {
            geometry: Geometry::Polygon { coordinates },
            ..
        })) = run("st_buffer", &args)
        else {
            panic!("expected a buffered polygon");
        };
        assert!(
            coordinates[0].len() > 3,
            "a zero segment count must fall back to the default"
        );
    }

    #[test]
    fn projected_buffer_is_planar_and_keeps_srid() {
        let args = vec![
            projected(Srid::Utm48N, Geometry::point(500_000.0, 100_000.0)),
            Value::Float(100.0),
        ];
        let Some(Value::Geometry(g)) = run("st_buffer", &args) else {
            panic!("expected a geometry");
        };
        assert_eq!(g.srid, Srid::Utm48N);
        let Geometry::Polygon { coordinates } = &g.geometry else {
            panic!("expected a polygon");
        };
        let [x, y] = coordinates[0][0];
        assert!(((x - 500_000.0).hypot(y - 100_000.0) - 100.0).abs() < 1e-6);
    }

    #[test]
    fn union_rejects_mixed_srids() {
        let args = vec![
            wgs84(Geometry::point(0.0, 0.0)),
            projected(Srid::PeninsularRso, Geometry::point(1.0, 1.0)),
        ];
        assert!(matches!(
            eval("st_union", &args),
            Some(Err(EvalError::MixedSrid { .. }))
        ));
    }

    #[test]
    fn centroid_keeps_srid() {
        let args = vec![projected(Srid::BorneoRso, Geometry::point(5.0, 6.0))];
        assert_eq!(
            run("st_centroid", &args),
            Some(projected(Srid::BorneoRso, Geometry::point(5.0, 6.0)))
        );
    }

    #[test]
    fn centroid_of_unreadable_geometry_is_null() {
        let bad = vec![Value::String("not a geometry".into())];
        assert_eq!(run("st_centroid", &bad), Some(Value::Null));
    }

    #[test]
    fn unknown_name_falls_through() {
        assert_eq!(eval("st_x", &[]), None);
    }
}
