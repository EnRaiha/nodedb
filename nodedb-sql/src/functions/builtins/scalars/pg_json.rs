// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL JSON operator and SQL/JSON standard scalar function registrations.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn pg_json_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "pg_json_get",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::PG_JSON_2_ARGS,
        ),
        m(
            "pg_json_get_text",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_JSON_2_ARGS,
        ),
        m(
            "pg_json_path_get",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::PG_JSON_2_ARGS,
        ),
        m(
            "pg_json_path_get_text",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_JSON_2_ARGS,
        ),
        m(
            "pg_json_contains",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::PG_JSON_BOOL_2_ARGS,
        ),
        m(
            "pg_json_contained_by",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::PG_JSON_BOOL_2_ARGS,
        ),
        m(
            "pg_json_has_key",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::PG_JSON_BOOL_2_ARGS,
        ),
        m(
            "pg_json_has_all_keys",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::PG_JSON_BOOL_2_ARGS,
        ),
        m(
            "pg_json_has_any_key",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::PG_JSON_BOOL_2_ARGS,
        ),
        m(
            "json_value",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::JSON_VALUE_ARGS,
        ),
        m(
            "json_query",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Json),
            arg_types::JSON_QUERY_ARGS,
        ),
        m(
            "json_exists",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::JSON_EXISTS_ARGS,
        ),
        // `to_jsonb(v)` is `v` as JSON. `to_jsonb(*)` is the whole row.
        m(
            "to_jsonb",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Json),
            arg_types::TO_JSONB_ARGS,
        ),
    ]
}
