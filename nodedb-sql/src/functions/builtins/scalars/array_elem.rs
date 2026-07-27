// SPDX-License-Identifier: Apache-2.0

//! Array element scalar function registrations.
//!
//! Distinct from `array_fn.rs`'s "Array engine" table-valued functions
//! (`array_slice`/`array_project`/`array_agg`/...), which operate on a
//! named on-disk array-engine collection: these operate on an in-row
//! `Array` value and are evaluated by
//! `nodedb_query::functions::array::try_eval`. Historically absent from
//! this registry entirely — the plan-time existence gate
//! rejects any name missing here, so every one of these would have been
//! rejected at plan time despite the runtime evaluator fully supporting
//! them. The list is audited to match every match arm in
//! `nodedb_query::functions::array::try_eval` exactly: `array_length` /
//! `cardinality`, `array_append`, `array_prepend`, `array_remove`,
//! `array_concat` / `array_cat`, `array_distinct`, `array_contains`,
//! `array_position`, `array_reverse`. Keep both lists in sync.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn array_elem_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "array_length",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::ARRAY_1_ARGS,
        ),
        // `cardinality` is an alias for `array_length` — same
        // `nodedb_query::functions::array::try_eval` match arm
        // (`"array_length" | "cardinality"`).
        m(
            "cardinality",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::ARRAY_1_ARGS,
        ),
        m(
            "array_append",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::ARRAY_ELEM_ARGS,
        ),
        m(
            "array_prepend",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::ARRAY_PREPEND_ARGS,
        ),
        m(
            "array_remove",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::ARRAY_ELEM_ARGS,
        ),
        m(
            "array_concat",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::ARRAY_CAT_ARGS,
        ),
        // `array_cat` is an alias for `array_concat` — same match arm
        // (`"array_concat" | "array_cat"`).
        m(
            "array_cat",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::ARRAY_CAT_ARGS,
        ),
        m(
            "array_distinct",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::ARRAY_1_ARGS,
        ),
        m(
            "array_contains",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::ARRAY_ELEM_ARGS,
        ),
        m(
            "array_position",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::ARRAY_ELEM_ARGS,
        ),
        m(
            "array_reverse",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::ARRAY_1_ARGS,
        ),
    ]
}
