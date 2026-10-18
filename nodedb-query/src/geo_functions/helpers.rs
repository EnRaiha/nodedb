// SPDX-License-Identifier: Apache-2.0

//! Argument coercion shared by the geo function evaluators.

use crate::expr::EvalError;
use crate::value_ops::value_to_f64;
use nodedb_types::Value;
use nodedb_types::geometry::{GeoJsonError, Srid, SridGeometry, parse_geojson};

/// What a geo family evaluator returns: `None` when the canonical name is
/// not its own, else the call's value or the fault that fails the statement.
pub(super) type GeoEval = Option<Result<Value, EvalError>>;

pub(super) fn str_arg(args: &[Value], idx: usize) -> Option<String> {
    args.get(idx)?.as_str().map(|s| s.to_string())
}

pub(super) fn num_arg(args: &[Value], idx: usize) -> Option<f64> {
    args.get(idx).and_then(|v| value_to_f64(v, true))
}

/// Extract a geometry argument.
///
/// Accepts every representation geometry takes on its way through the system:
/// a native `Value::Geometry`, a GeoJSON object decoded from a document, a
/// GeoJSON string (the form a constant-folded geometry is stored and passed
/// as), and a WKT string. Accepting WKT here is what makes a bare
/// `'POINT(1 2)'` literal usable wherever a geometry is expected, matching
/// PostGIS's treatment of an unknown-typed literal in geometry position.
/// GeoJSON keeps the SRID its `crs` member names. WKT is WGS 84.
pub(super) fn geom_arg(args: &[Value], idx: usize) -> Option<SridGeometry> {
    match args.get(idx)? {
        Value::Geometry(g) => Some(g.clone()),
        Value::String(s) => geometry_from_text(s),
        obj @ Value::Object(_) => SridGeometry::from_value(obj),
        _ => None,
    }
}

/// Parse a textual geometry: GeoJSON first, then WKT.
///
/// The two are unambiguous — GeoJSON always starts with `{`, WKT with a type
/// keyword — so trying them in order cannot misread one as the other.
pub fn geometry_from_text(text: &str) -> Option<SridGeometry> {
    parse_text_geometry(text).ok()
}

/// [`geometry_from_text`] that reports why a text is not a geometry. A
/// GeoJSON `crs` naming an unsupported SRID gives
/// [`GeoJsonError::UnsupportedSrid`]. Any other unreadable text gives
/// [`GeoJsonError::Malformed`].
pub(super) fn parse_text_geometry(text: &str) -> Result<SridGeometry, GeoJsonError> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        parse_geojson(trimmed)
    } else {
        nodedb_spatial::geometry_from_wkt(trimmed)
            .map(SridGeometry::wgs84)
            .ok_or(GeoJsonError::Malformed)
    }
}

/// An optional SRID argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SridArg {
    /// The call has no argument at this position.
    Absent,
    /// The argument is SQL NULL.
    Null,
    Srid(Srid),
}

/// Read the SRID argument at `idx`.
///
/// An integer, an integral float, or integer text resolves through
/// [`Srid::from_code`]. A code outside the supported set fails with
/// [`EvalError::UnsupportedSrid`]. A value of any other kind fails with
/// [`EvalError::ArgumentType`].
pub(super) fn srid_arg(
    args: &[Value],
    idx: usize,
    function: &'static str,
) -> Result<SridArg, EvalError> {
    let Some(value) = args.get(idx) else {
        return Ok(SridArg::Absent);
    };
    let code = match value {
        Value::Null => return Ok(SridArg::Null),
        Value::Integer(i) => Some(*i),
        Value::Float(f) if f.is_finite() && f.fract() == 0.0 => Some(*f as i64),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    };
    let Some(code) = code else {
        return Err(EvalError::ArgumentType {
            function,
            position: idx + 1,
            expected: "an integer SRID",
            got: value.type_name(),
        });
    };
    Srid::from_code(code)
        .map(SridArg::Srid)
        .map_err(|e| EvalError::UnsupportedSrid {
            function,
            srid: e.code,
        })
}

/// The shared SRID of two operands, or [`EvalError::MixedSrid`].
pub(super) fn same_srid(
    function: &'static str,
    a: &SridGeometry,
    b: &SridGeometry,
) -> Result<Srid, EvalError> {
    if a.srid == b.srid {
        Ok(a.srid)
    } else {
        Err(EvalError::MixedSrid {
            function,
            left: a.srid.code(),
            right: b.srid.code(),
        })
    }
}

/// Map a GeoJSON parse fault: an unsupported SRID fails the statement,
/// anything else is an unreadable argument and yields NULL.
pub(super) fn text_fault(function: &'static str, e: GeoJsonError) -> Result<Value, EvalError> {
    match e {
        GeoJsonError::UnsupportedSrid(u) => Err(EvalError::UnsupportedSrid {
            function,
            srid: u.code,
        }),
        GeoJsonError::Malformed | GeoJsonError::UnknownCrsName { .. } => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::Geometry;

    #[test]
    fn geojson_and_wkt_strings_both_parse() {
        let geojson = geometry_from_text(r#"{"type":"Point","coordinates":[1.0,2.0]}"#);
        let wkt = geometry_from_text("POINT(1 2)");
        assert_eq!(geojson, wkt);
        assert_eq!(
            geojson,
            Some(SridGeometry::wgs84(Geometry::point(1.0, 2.0)))
        );
    }

    #[test]
    fn geojson_crs_member_sets_srid() {
        let g = geometry_from_text(
            r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"name","properties":{"name":"EPSG:3375"}}}"#,
        );
        assert_eq!(g.map(|g| g.srid), Some(Srid::PeninsularRso));
    }

    #[test]
    fn surrounding_whitespace_does_not_defeat_parsing() {
        assert!(geometry_from_text("  POINT(1 2)  ").is_some());
        assert!(geometry_from_text("  {\"type\":\"Point\",\"coordinates\":[1,2]}  ").is_some());
    }

    #[test]
    fn non_geometry_text_is_rejected() {
        assert!(geometry_from_text("not a geometry").is_none());
        assert!(geometry_from_text("").is_none());
    }

    #[test]
    fn srid_arg_forms() {
        let f = "st_setsrid";
        let args = [
            Value::Integer(32648),
            Value::Float(3375.0),
            Value::String("3376".into()),
            Value::Null,
        ];
        assert_eq!(srid_arg(&args, 0, f), Ok(SridArg::Srid(Srid::Utm48N)));
        assert_eq!(
            srid_arg(&args, 1, f),
            Ok(SridArg::Srid(Srid::PeninsularRso))
        );
        assert_eq!(srid_arg(&args, 2, f), Ok(SridArg::Srid(Srid::BorneoRso)));
        assert_eq!(srid_arg(&args, 3, f), Ok(SridArg::Null));
        assert_eq!(srid_arg(&args, 4, f), Ok(SridArg::Absent));
        assert_eq!(
            srid_arg(&[Value::Integer(2154)], 0, f),
            Err(EvalError::UnsupportedSrid {
                function: f,
                srid: 2154
            })
        );
        assert!(matches!(
            srid_arg(&[Value::Bool(true)], 0, f),
            Err(EvalError::ArgumentType { .. })
        ));
    }
}
