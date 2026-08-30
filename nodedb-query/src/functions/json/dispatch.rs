// SPDX-License-Identifier: Apache-2.0

//! Name-to-implementation dispatch for the JSON function family.

use nodedb_types::Value;

pub(in crate::functions) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    // PostgreSQL JSON operator functions (lowered from AST BinaryOp).
    let pg_result = match name {
        "pg_json_get" => {
            let t = args.first().unwrap_or(&Value::Null);
            let k = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_get(t, k))
        }
        "pg_json_get_text" => {
            let t = args.first().unwrap_or(&Value::Null);
            let k = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_get_text(t, k))
        }
        "pg_json_path_get" => {
            let t = args.first().unwrap_or(&Value::Null);
            let p = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_path_get(t, p))
        }
        "pg_json_path_get_text" => {
            let t = args.first().unwrap_or(&Value::Null);
            let p = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_path_get_text(t, p))
        }
        "pg_json_contains" => {
            let a = args.first().unwrap_or(&Value::Null);
            let b = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_contains(a, b))
        }
        "pg_json_contained_by" => {
            let a = args.first().unwrap_or(&Value::Null);
            let b = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_contained_by(a, b))
        }
        "pg_json_has_key" => {
            let t = args.first().unwrap_or(&Value::Null);
            let k = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_has_key(t, k))
        }
        "pg_json_has_all_keys" => {
            let t = args.first().unwrap_or(&Value::Null);
            let k = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_has_all_keys(t, k))
        }
        "pg_json_has_any_key" => {
            let t = args.first().unwrap_or(&Value::Null);
            let k = args.get(1).unwrap_or(&Value::Null);
            Some(super::pg_ops::pg_json_has_any_key(t, k))
        }
        // SQL/JSON standard functions.
        "json_value" => {
            let t = args.first().unwrap_or(&Value::Null);
            let p = args.get(1).unwrap_or(&Value::Null);
            Some(super::standard::json_value(t, p))
        }
        "json_query" => {
            let t = args.first().unwrap_or(&Value::Null);
            let p = args.get(1).unwrap_or(&Value::Null);
            Some(super::standard::json_query(t, p))
        }
        "json_exists" => {
            let t = args.first().unwrap_or(&Value::Null);
            let p = args.get(1).unwrap_or(&Value::Null);
            Some(super::standard::json_exists(t, p))
        }
        _ => None,
    };
    if pg_result.is_some() {
        return pg_result;
    }

    // Legacy json_* functions.
    super::legacy::try_eval(name, args)
}
