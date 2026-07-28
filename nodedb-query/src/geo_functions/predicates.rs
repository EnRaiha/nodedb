// SPDX-License-Identifier: Apache-2.0

//! Topological predicate evaluation. Every predicate is NULL-propagating: an
//! argument that is not a readable geometry yields NULL rather than `false`,
//! so "no geometry" stays distinguishable from "does not match".

use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

use super::helpers::{geom_arg, num_arg};

pub(super) fn eval(canonical: &str, args: &[Value]) -> Option<Value> {
    let result = match canonical {
        "st_contains" => binary(args, nodedb_spatial::st_contains),
        "st_intersects" => binary(args, nodedb_spatial::st_intersects),
        "st_within" => binary(args, nodedb_spatial::st_within),
        "st_disjoint" => binary(args, nodedb_spatial::st_disjoint),
        "st_dwithin" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(Value::Null);
            };
            let Some(distance) = num_arg(args, 2) else {
                return Some(Value::Null);
            };
            Value::Bool(nodedb_spatial::st_dwithin(&a, &b, distance))
        }
        "st_isvalid" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            Value::Bool(nodedb_spatial::is_valid(&geom))
        }
        _ => return None,
    };
    Some(result)
}

fn binary(args: &[Value], f: fn(&Geometry, &Geometry) -> bool) -> Value {
    let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
        return Value::Null;
    };
    Value::Bool(f(&a, &b))
}
