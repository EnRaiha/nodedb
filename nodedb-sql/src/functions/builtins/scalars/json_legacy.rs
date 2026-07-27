// SPDX-License-Identifier: Apache-2.0

//! Legacy JSON manipulation scalar function registrations.
//!
//! Distinct from `pg_json.rs`'s PostgreSQL JSON operators
//! (`pg_json_get`/...) and SQL/JSON standard functions
//! (`json_value`/`json_query`/`json_exists`): these are the pre-existing
//! `json_*` document-helper functions evaluated by
//! `nodedb_query::functions::json::legacy::try_eval`. Historically absent
//! from this registry entirely — the plan-time existence
//! gate rejects any name missing here. The list is audited to match every
//! match arm in `nodedb_query::functions::json::legacy::try_eval` exactly:
//! `json_extract` / `json_get`, `json_set`, `json_remove`, `json_keys`,
//! `json_values`, `json_length` / `json_len`, `json_type`, `json_array`,
//! `json_object`, `json_contains`, `json_merge` / `json_patch`. Keep both
//! lists in sync.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn json_legacy_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "json_extract",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::JSON_EXTRACT_ARGS,
        ),
        // `json_get` is an alias for `json_extract` — same
        // `nodedb_query::functions::json::legacy::try_eval` match arm
        // (`"json_extract" | "json_get"`).
        m(
            "json_get",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::JSON_EXTRACT_ARGS,
        ),
        m(
            "json_set",
            Scalar,
            3,
            3,
            no_trigger(),
            Some(ColumnType::Json),
            arg_types::JSON_SET_ARGS,
        ),
        m(
            "json_remove",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Json),
            arg_types::JSON_REMOVE_ARGS,
        ),
        m(
            "json_keys",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::JSON_1_ARGS,
        ),
        m(
            "json_values",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::JSON_1_ARGS,
        ),
        m(
            "json_length",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::JSON_1_ARGS,
        ),
        // `json_len` is an alias for `json_length` — same match arm
        // (`"json_length" | "json_len"`).
        m(
            "json_len",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::JSON_1_ARGS,
        ),
        m(
            "json_type",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::JSON_1_ARGS,
        ),
        m(
            "json_array",
            Scalar,
            0,
            255,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::JSON_ARRAY_ARGS,
        ),
        m(
            "json_object",
            Scalar,
            0,
            255,
            no_trigger(),
            Some(ColumnType::Json),
            arg_types::JSON_OBJECT_ARGS,
        ),
        m(
            "json_contains",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::JSON_CONTAINS_ARGS,
        ),
        m(
            "json_merge",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Json),
            arg_types::JSON_MERGE_ARGS,
        ),
        // `json_patch` is an alias for `json_merge` — same match arm
        // (`"json_merge" | "json_patch"`).
        m(
            "json_patch",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Json),
            arg_types::JSON_MERGE_ARGS,
        ),
    ]
}
