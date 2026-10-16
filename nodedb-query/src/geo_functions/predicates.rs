// SPDX-License-Identifier: Apache-2.0

//! Topological predicate evaluation. Every predicate is NULL-propagating: an
//! argument that is not a readable geometry yields NULL rather than `false`,
//! so "no geometry" stays distinguishable from "does not match". Two
//! operands in different SRIDs fail with `EvalError::MixedSrid`.

use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

use super::helpers::{GeoEval, geom_arg, num_arg, same_srid};
use crate::expr::EvalError;

pub(super) fn eval(canonical: &'static str, args: &[Value]) -> GeoEval {
    let result = match canonical {
        "st_contains" => binary(canonical, args, nodedb_spatial::st_contains),
        "st_intersects" => binary(canonical, args, nodedb_spatial::st_intersects),
        "st_within" => binary(canonical, args, nodedb_spatial::st_within),
        "st_disjoint" => binary(canonical, args, nodedb_spatial::st_disjoint),
        "st_dwithin" => dwithin(canonical, args),
        "st_isvalid" => Ok(match geom_arg(args, 0) {
            Some(g) => Value::Bool(nodedb_spatial::is_valid(&g.geometry)),
            None => Value::Null,
        }),
        _ => return None,
    };
    Some(result)
}

fn binary(
    function: &'static str,
    args: &[Value],
    f: fn(&Geometry, &Geometry) -> bool,
) -> Result<Value, EvalError> {
    let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
        return Ok(Value::Null);
    };
    same_srid(function, &a, &b)?;
    Ok(Value::Bool(f(&a.geometry, &b.geometry)))
}

/// `ST_DWithin(a, b, distance)`, distance in metres: geodesic-approximate for
/// 4326, planar for a projected SRID.
fn dwithin(function: &'static str, args: &[Value]) -> Result<Value, EvalError> {
    let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
        return Ok(Value::Null);
    };
    let Some(distance) = num_arg(args, 2) else {
        return Ok(Value::Null);
    };
    let srid = same_srid(function, &a, &b)?;
    let within = if srid.is_geographic() {
        nodedb_spatial::st_dwithin(&a.geometry, &b.geometry, distance)
    } else {
        nodedb_spatial::planar_dwithin(&a.geometry, &b.geometry, distance)
    };
    Ok(Value::Bool(within))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Srid, SridGeometry};

    fn geom(srid: Srid, g: Geometry) -> Value {
        Value::Geometry(SridGeometry::new(srid, g))
    }

    #[test]
    fn mixed_srid_intersects_fails() {
        let args = vec![
            geom(Srid::Wgs84, Geometry::point(1.0, 2.0)),
            geom(Srid::Utm48N, Geometry::point(1.0, 2.0)),
        ];
        assert_eq!(
            eval("st_intersects", &args),
            Some(Err(EvalError::MixedSrid {
                function: "st_intersects",
                left: 4326,
                right: 32648
            }))
        );
    }

    #[test]
    fn projected_dwithin_is_planar_metres() {
        let args = |d: f64| {
            vec![
                geom(Srid::Utm48N, Geometry::point(500_000.0, 0.0)),
                geom(Srid::Utm48N, Geometry::point(500_300.0, 400.0)),
                Value::Float(d),
            ]
        };
        assert_eq!(
            eval("st_dwithin", &args(500.0)),
            Some(Ok(Value::Bool(true)))
        );
        assert_eq!(
            eval("st_dwithin", &args(499.0)),
            Some(Ok(Value::Bool(false)))
        );
    }

    #[test]
    fn same_srid_predicate_evaluates() {
        let square = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        let args = vec![
            geom(Srid::PeninsularRso, square),
            geom(Srid::PeninsularRso, Geometry::point(5.0, 5.0)),
        ];
        assert_eq!(eval("st_contains", &args), Some(Ok(Value::Bool(true))));
    }
}
