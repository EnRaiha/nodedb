// SPDX-License-Identifier: Apache-2.0

//! Scalar function evaluation for SqlExpr.
//!
//! All functions return `Value::Null` on invalid/missing
//! arguments (SQL NULL propagation semantics).

mod array;
mod conditional;
mod datetime;
pub(crate) mod fts;
mod id;
mod json;
mod math;
pub(crate) mod shared;
mod string;
mod system;
mod types;

use nodedb_types::Value;

use crate::expr::EvalError;

/// Evaluate a scalar function call.
///
/// Every function returns `Ok(Value::Null)` on invalid/missing arguments
/// (SQL NULL propagation semantics) — the sole exception is `mod`'s
/// zero-modulus arm (`math::try_eval`), which returns
/// `Err(EvalError::DivisionByZero)`. Every other
/// sibling module here stays `Option<Value>`-shaped internally; only the
/// `math` arm is threaded as `Option<Result<Value, EvalError>>` and the
/// rest are wrapped in `Ok` at this dispatch boundary, so a single
/// fallible arm doesn't force every scalar-function module to carry a
/// `Result` it can never actually produce.
pub fn eval_function(name: &str, args: &[Value]) -> Result<Value, EvalError> {
    if let Some(v) = string::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(r) = math::try_eval(name, args) {
        return r;
    }
    if let Some(v) = conditional::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = id::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = datetime::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = json::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = types::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = array::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = fts::try_eval_fts(name, args) {
        return Ok(v);
    }
    if let Some(v) = system::try_eval(name, args) {
        return Ok(v);
    }
    // Geo / Spatial functions — delegated to geo_functions module.
    Ok(crate::geo_functions::eval_geo_function(name, args).unwrap_or(Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::SqlExpr;

    fn eval_fn(name: &str, args: Vec<Value>) -> Value {
        eval_function(name, &args).unwrap()
    }

    #[test]
    fn mod_by_zero_errors() {
        let err = eval_function("mod", &[Value::Integer(5), Value::Integer(0)]).unwrap_err();
        assert_eq!(err, EvalError::DivisionByZero);
    }

    #[test]
    fn upper() {
        assert_eq!(
            eval_fn("upper", vec![Value::String("hello".into())]),
            Value::String("HELLO".into())
        );
    }

    #[test]
    fn upper_null_propagation() {
        assert_eq!(eval_fn("upper", vec![Value::Null]), Value::Null);
    }

    #[test]
    fn substring() {
        assert_eq!(
            eval_fn(
                "substr",
                vec![
                    Value::String("hello".into()),
                    Value::Integer(2),
                    Value::Integer(3)
                ]
            ),
            Value::String("ell".into())
        );
    }

    #[test]
    fn round_with_decimals() {
        assert_eq!(
            eval_fn("round", vec![Value::Float(3.15159), Value::Integer(2)]),
            Value::Float(3.15)
        );
    }

    #[test]
    fn typeof_int() {
        assert_eq!(
            eval_fn("typeof", vec![Value::Integer(42)]),
            Value::String("int".into())
        );
    }

    #[test]
    fn function_via_expr() {
        let expr = SqlExpr::Function {
            name: "upper".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        let doc = Value::Object(
            [("name".to_string(), Value::String("alice".into()))]
                .into_iter()
                .collect(),
        );
        assert_eq!(expr.eval(&doc).unwrap(), Value::String("ALICE".into()));
    }

    #[test]
    fn geo_geohash_encode() {
        let result = eval_fn(
            "geo_geohash",
            vec![
                Value::Float(-73.9857),
                Value::Float(40.758),
                Value::Integer(6),
            ],
        );
        let hash = result.as_str().unwrap();
        assert_eq!(hash.len(), 6);
        assert!(hash.starts_with("dr5ru"), "got {hash}");
    }

    #[test]
    fn geo_geohash_decode() {
        let hash = eval_fn(
            "geo_geohash",
            vec![Value::Float(0.0), Value::Float(0.0), Value::Integer(6)],
        );
        let result = eval_fn("geo_geohash_decode", vec![hash]);
        assert!(!result.is_null());
        assert!(result.get("min_lng").is_some());
    }

    #[test]
    fn geo_geohash_neighbors_returns_8() {
        let hash = eval_fn(
            "geo_geohash",
            vec![Value::Float(10.0), Value::Float(50.0), Value::Integer(6)],
        );
        let result = eval_fn("geo_geohash_neighbors", vec![hash]);
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 8);
    }

    fn point_value(lng: f64, lat: f64) -> Value {
        let geom = nodedb_types::geometry::Geometry::point(lng, lat);
        Value::Geometry(geom)
    }

    fn square_value() -> Value {
        let geom = nodedb_types::geometry::Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        Value::Geometry(geom)
    }

    #[test]
    fn st_contains_sql() {
        let result = eval_fn("st_contains", vec![square_value(), point_value(5.0, 5.0)]);
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn st_intersects_sql() {
        let result = eval_fn("st_intersects", vec![square_value(), point_value(5.0, 0.0)]);
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn st_distance_sql() {
        let result = eval_fn(
            "st_distance",
            vec![point_value(0.0, 0.0), point_value(0.0, 1.0)],
        );
        let d = result.as_f64().unwrap();
        assert!((d - 111_195.0).abs() < 500.0, "got {d}");
    }

    #[test]
    fn st_dwithin_sql() {
        let result = eval_fn(
            "st_dwithin",
            vec![
                point_value(0.0, 0.0),
                point_value(0.001, 0.0),
                Value::Float(200.0),
            ],
        );
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn st_buffer_sql() {
        let result = eval_fn(
            "st_buffer",
            vec![
                point_value(0.0, 0.0),
                Value::Float(1000.0),
                Value::Integer(8),
            ],
        );
        // Result should be a Geometry (Polygon)
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn st_envelope_sql() {
        let result = eval_fn("st_envelope", vec![square_value()]);
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn geo_length_sql() {
        let line = Value::Geometry(nodedb_types::geometry::Geometry::line_string(vec![
            [0.0, 0.0],
            [0.0, 1.0],
        ]));
        let result = eval_fn("geo_length", vec![line]);
        let d = result.as_f64().unwrap();
        assert!((d - 111_195.0).abs() < 500.0, "got {d}");
    }

    #[test]
    fn geo_x_y() {
        assert_eq!(
            eval_fn("geo_x", vec![point_value(5.0, 10.0)])
                .as_f64()
                .unwrap(),
            5.0
        );
        assert_eq!(
            eval_fn("geo_y", vec![point_value(5.0, 10.0)])
                .as_f64()
                .unwrap(),
            10.0
        );
    }

    #[test]
    fn geo_type_sql() {
        assert_eq!(
            eval_fn("geo_type", vec![point_value(0.0, 0.0)]),
            Value::String("Point".into())
        );
        assert_eq!(
            eval_fn("geo_type", vec![square_value()]),
            Value::String("Polygon".into())
        );
    }

    #[test]
    fn geo_num_points_sql() {
        assert_eq!(
            eval_fn("geo_num_points", vec![point_value(0.0, 0.0)]),
            Value::Integer(1)
        );
        assert_eq!(
            eval_fn("geo_num_points", vec![square_value()]),
            Value::Integer(5)
        );
    }

    #[test]
    fn geo_is_valid_sql() {
        assert_eq!(
            eval_fn("geo_is_valid", vec![square_value()]),
            Value::Bool(true)
        );
    }

    #[test]
    fn geo_as_wkt_sql() {
        let result = eval_fn("geo_as_wkt", vec![point_value(5.0, 10.0)]);
        assert_eq!(result, Value::String("POINT(5 10)".into()));
    }

    #[test]
    fn geo_from_wkt_sql() {
        let result = eval_fn("geo_from_wkt", vec![Value::String("POINT(5 10)".into())]);
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn geo_circle_sql() {
        let result = eval_fn(
            "geo_circle",
            vec![
                Value::Float(0.0),
                Value::Float(0.0),
                Value::Float(1000.0),
                Value::Integer(16),
            ],
        );
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn geo_bbox_sql() {
        let result = eval_fn(
            "geo_bbox",
            vec![
                Value::Float(0.0),
                Value::Float(0.0),
                Value::Float(10.0),
                Value::Float(10.0),
            ],
        );
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn version_returns_postgres_compatible_string() {
        match eval_fn("version", vec![]) {
            Value::String(s) => assert!(s.contains("PostgreSQL")),
            other => panic!("expected Value::String, got {other:?}"),
        }
    }
}
