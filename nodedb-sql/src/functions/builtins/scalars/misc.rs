// SPDX-License-Identifier: Apache-2.0

//! Miscellaneous scalar function registrations (coalesce, nullif, greatest,
//! least, make_array, typeof, utility).
//!
//! The plan-time existence gate rejects any name missing
//! here. `coalesce` / `nullif` mirror
//! `nodedb_query::functions::conditional::try_eval`'s full match arm list
//! together with `greatest` / `least` below (before this audit those two
//! were runtime-only); `typeof` / `type_of` mirror
//! `nodedb_query::functions::types::try_eval` in full (previously
//! unregistered entirely). Keep both lists in sync.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn misc_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "coalesce",
            Scalar,
            1,
            255,
            no_trigger(),
            None,
            arg_types::COALESCE_ARGS,
        ),
        m(
            "nullif",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::NULLIF_ARGS,
        ),
        m(
            "greatest",
            Scalar,
            1,
            255,
            no_trigger(),
            None,
            arg_types::GREATEST_LEAST_ARGS,
        ),
        m(
            "least",
            Scalar,
            1,
            255,
            no_trigger(),
            None,
            arg_types::GREATEST_LEAST_ARGS,
        ),
        // `type_of` is an alias for `typeof` — same
        // `nodedb_query::functions::types::try_eval` match arm
        // (`"typeof" | "type_of"`).
        m(
            "typeof",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::TYPEOF_ARGS,
        ),
        m(
            "type_of",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::TYPEOF_ARGS,
        ),
        m(
            "make_array",
            Scalar,
            0,
            255,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::MAKE_ARRAY_ARGS,
        ),
        m(
            "ndb_chunk_text",
            Scalar,
            2,
            3,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::NDB_CHUNK_TEXT_ARGS,
        ),
        m(
            "version",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::NO_ARGS,
        ),
        m(
            "format_type",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_CATALOG_2_ARGS,
        ),
        m(
            "pg_get_expr",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_CATALOG_2_ARGS,
        ),
        m(
            "col_description",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_CATALOG_2_ARGS,
        ),
    ]
}
