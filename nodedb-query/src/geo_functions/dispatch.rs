// SPDX-License-Identifier: Apache-2.0

//! Entry point for geospatial function evaluation.

use nodedb_types::Value;

use super::catalog;
use super::{accessors, constructors, indexing, measures, predicates};

/// Evaluate a geospatial function.
///
/// `name` may be any spelling the catalog accepts — the standard `ST_*` name
/// or an internal `geo_*` one — and is resolved to a single canonical entry
/// before dispatch, so every spelling of a capability behaves identically.
///
/// Returns `None` only when `name` is not a geospatial function at all, so the
/// caller can fall through to its own function table.
pub fn eval_geo_function(name: &str, args: &[Value]) -> Option<Value> {
    let spec = catalog::lookup(name)?;
    eval_canonical(spec.canonical, args)
}

/// Route a canonical name to its family evaluator.
///
/// A `None` here means the catalog declares a function the evaluators do not
/// implement. That drift is caught by `every_catalog_entry_is_evaluable`
/// below rather than being allowed to surface as a silent NULL at runtime.
fn eval_canonical(canonical: &str, args: &[Value]) -> Option<Value> {
    predicates::eval(canonical, args)
        .or_else(|| measures::eval(canonical, args))
        .or_else(|| accessors::eval(canonical, args))
        .or_else(|| constructors::eval(canonical, args))
        .or_else(|| indexing::eval(canonical, args))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::Geometry;

    /// Arguments that satisfy any catalog shape: enough geometry-shaped and
    /// number-shaped values that no evaluator indexes past the end.
    fn probe_args() -> Vec<Value> {
        vec![
            Value::Geometry(Geometry::point(1.0, 2.0)),
            Value::Geometry(Geometry::point(3.0, 4.0)),
            Value::Float(5.0),
            Value::Float(6.0),
        ]
    }

    /// The catalog drives the SQL function registry, so a name it declares is
    /// accepted at plan time. If no evaluator implements that name, the query
    /// plans and then evaluates to NULL for every row — the exact silent
    /// failure this catalog exists to prevent. Every entry must dispatch.
    #[test]
    fn every_catalog_entry_is_evaluable() {
        for spec in catalog::GEO_FUNCTIONS {
            assert!(
                eval_canonical(spec.canonical, &probe_args()).is_some(),
                "catalog declares '{}' but no evaluator handles it",
                spec.canonical
            );
        }
    }

    /// Every accepted spelling must reach the same evaluator as its canonical
    /// name — the property that makes `ST_X` and `geo_x` interchangeable.
    #[test]
    fn every_alias_evaluates_identically_to_its_canonical_name() {
        for spec in catalog::GEO_FUNCTIONS {
            let canonical = eval_geo_function(spec.canonical, &probe_args());
            for alias in spec.aliases {
                assert_eq!(
                    eval_geo_function(alias, &probe_args()),
                    canonical,
                    "'{alias}' must behave exactly like '{}'",
                    spec.canonical
                );
            }
        }
    }

    /// Names are matched case-insensitively, as SQL identifiers are.
    #[test]
    fn dispatch_is_case_insensitive() {
        let args = probe_args();
        assert_eq!(
            eval_geo_function("ST_AsText", &args),
            eval_geo_function("st_astext", &args)
        );
    }

    #[test]
    fn non_geo_function_falls_through() {
        assert_eq!(eval_geo_function("lower", &probe_args()), None);
    }

    /// The reported bug in one line: the standard accessor spelling must read
    /// a geometry that the internal spelling already read.
    #[test]
    fn standard_accessor_names_read_stored_geometry() {
        use crate::value_ops::to_value_number;
        let point = vec![Value::Geometry(Geometry::point(1.0, 2.0))];
        assert_eq!(
            eval_geo_function("st_x", &point),
            Some(to_value_number(1.0))
        );
        assert_eq!(
            eval_geo_function("st_y", &point),
            Some(to_value_number(2.0))
        );
        assert!(matches!(
            eval_geo_function("st_astext", &point),
            Some(Value::String(_))
        ));
    }
}
