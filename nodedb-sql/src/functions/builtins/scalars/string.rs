// SPDX-License-Identifier: Apache-2.0

//! String scalar function registrations.
//!
//! The plan-time existence gate rejects any name missing
//! here, so this list is audited to match every match arm in
//! `nodedb_query::functions::string::try_eval` exactly: `upper`, `lower`,
//! `trim`, `ltrim`, `rtrim`, `length` / `char_length` / `character_length`,
//! `substr` / `substring`, `concat`, `replace`, `reverse`. Before this
//! audit `ltrim`, `rtrim`, `char_length`, `character_length`, `substr`, and
//! `reverse` were runtime-only, unreachable from SQL via the gate. Keep both
//! lists in sync.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn string_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "lower",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        m(
            "upper",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        m(
            "length",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::LENGTH_ARGS,
        ),
        m(
            "trim",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        m(
            "ltrim",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        m(
            "rtrim",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
        // `char_length` / `character_length` are SQL-standard aliases for
        // `length` — same `nodedb_query::functions::string::try_eval` match
        // arm (`"length" | "char_length" | "character_length"`).
        m(
            "char_length",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::LENGTH_ARGS,
        ),
        m(
            "character_length",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::LENGTH_ARGS,
        ),
        m(
            "substring",
            Scalar,
            2,
            3,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::SUBSTRING_ARGS,
        ),
        // `substr` is an alias for `substring` — same
        // `nodedb_query::functions::string::try_eval` match arm
        // (`"substr" | "substring"`).
        m(
            "substr",
            Scalar,
            2,
            3,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::SUBSTRING_ARGS,
        ),
        m(
            "concat",
            Scalar,
            1,
            255,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::CONCAT_ARGS,
        ),
        m(
            "replace",
            Scalar,
            3,
            3,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::REPLACE_ARGS,
        ),
        m(
            "reverse",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::STRING_1_ARGS,
        ),
    ]
}
