// SPDX-License-Identifier: Apache-2.0

//! Spatial reference system functions: read, set, and transform a
//! geometry's SRID.

use nodedb_spatial::CrsError;
use nodedb_types::Value;

use super::helpers::{GeoEval, SridArg, geom_arg, srid_arg};
use crate::expr::EvalError;

pub(super) fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
    let result = match canonical {
        "st_srid" => Ok(match geom_arg(args, 0) {
            Some(g) => Value::Integer(i64::from(g.srid.code())),
            None => Value::Null,
        }),
        "st_setsrid" => set_srid(canonical, args),
        "st_transform" => transform(canonical, args),
        _ => return None,
    };
    Some(result)
}

/// `ST_SetSRID(geom, srid)`: relabel the geometry's SRID. Coordinates are
/// kept as they are.
fn set_srid(function: &'static str, args: &[Value]) -> Result<Value, EvalError> {
    let srid = match srid_arg(args, 1, function)? {
        SridArg::Srid(srid) => srid,
        SridArg::Absent | SridArg::Null => return Ok(Value::Null),
    };
    Ok(match geom_arg(args, 0) {
        Some(mut g) => {
            g.srid = srid;
            Value::Geometry(g)
        }
        None => Value::Null,
    })
}

/// `ST_Transform(geom, srid)`: reproject every coordinate into `srid`.
fn transform(function: &'static str, args: &[Value]) -> Result<Value, EvalError> {
    let target = match srid_arg(args, 1, function)? {
        SridArg::Srid(srid) => srid,
        SridArg::Absent | SridArg::Null => return Ok(Value::Null),
    };
    let Some(g) = geom_arg(args, 0) else {
        return Ok(Value::Null);
    };
    nodedb_spatial::transform_geometry(&g, target)
        .map(Value::Geometry)
        .map_err(|e| projection_fault(function, e))
}

/// A projection that cannot map a coordinate fails the statement.
pub(super) fn projection_fault(function: &'static str, e: CrsError) -> EvalError {
    EvalError::ProjectionDomain {
        function,
        srid: e.srid().code(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Geometry, Srid, SridGeometry};

    fn point(srid: Srid, x: f64, y: f64) -> Value {
        Value::Geometry(SridGeometry::new(srid, Geometry::point(x, y)))
    }

    #[test]
    fn srid_reads_the_geometry_srid() {
        assert_eq!(
            eval("st_srid", &[point(Srid::Wgs84, 0.0, 0.0)]),
            Some(Ok(Value::Integer(4326)))
        );
        assert_eq!(
            eval("st_srid", &[point(Srid::BorneoRso, 0.0, 0.0)]),
            Some(Ok(Value::Integer(3376)))
        );
        assert_eq!(
            eval("st_srid", &[Value::String("not a geometry".into())]),
            Some(Ok(Value::Null))
        );
    }

    #[test]
    fn setsrid_relabels_without_moving() {
        let got = eval(
            "st_setsrid",
            &[point(Srid::Wgs84, 1.0, 2.0), Value::Integer(32648)],
        );
        assert_eq!(got, Some(Ok(point(Srid::Utm48N, 1.0, 2.0))));
    }

    #[test]
    fn transform_projects_klcc_into_utm47n() {
        let Some(Ok(Value::Geometry(g))) = eval(
            "st_transform",
            &[point(Srid::Wgs84, 101.7117, 3.1579), Value::Integer(32647)],
        ) else {
            panic!("expected a geometry");
        };
        assert_eq!(g.srid, Srid::Utm47N);
        let Geometry::Point { coordinates } = g.geometry else {
            panic!("expected a point");
        };
        assert!((coordinates[0] - 801_401.794_9).abs() < 1e-3);
        assert!((coordinates[1] - 349_439.510_6).abs() < 1e-3);
    }

    #[test]
    fn transform_to_unsupported_srid_names_every_supported_code() {
        let Some(Err(err)) = eval(
            "st_transform",
            &[point(Srid::Wgs84, 101.7, 3.1), Value::Integer(2154)],
        ) else {
            panic!("expected an error");
        };
        assert_eq!(
            err,
            EvalError::UnsupportedSrid {
                function: "st_transform",
                srid: 2154
            }
        );
        let text = err.to_string();
        for code in ["4326", "32647", "32648", "3375", "3376"] {
            assert!(text.contains(code), "{text}");
        }
    }

    #[test]
    fn transform_out_of_domain_is_a_projection_fault() {
        let got = eval(
            "st_transform",
            &[point(Srid::Wgs84, 179.0, 3.0), Value::Integer(32648)],
        );
        assert_eq!(
            got,
            Some(Err(EvalError::ProjectionDomain {
                function: "st_transform",
                srid: 32648
            }))
        );
    }

    #[test]
    fn null_srid_yields_null() {
        assert_eq!(
            eval("st_setsrid", &[point(Srid::Wgs84, 1.0, 2.0), Value::Null]),
            Some(Ok(Value::Null))
        );
    }
}
