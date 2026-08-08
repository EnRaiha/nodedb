// SPDX-License-Identifier: BUSL-1.1

//! Value conversion utilities: SqlValue ↔ nodedb_types::Value, msgpack encoding,
//! and the re-export of the shared column-default evaluator.

pub(super) mod assignments;
pub(super) mod convert;
pub(super) mod msgpack_write;
pub(super) mod rows;

pub(super) use assignments::{
    assignments_to_update_values, assignments_to_update_values_qualified,
};
pub(super) use convert::{
    sql_value_to_bytes, sql_value_to_msgpack, sql_value_to_nodedb_value, sql_value_to_string,
};
// The evaluator lives in the SQL crate so every engine that materializes a
// DEFAULT produces the same value for the same expression; re-exported here so
// the document/columnar converters keep their existing import path.
pub(super) use msgpack_write::{
    row_to_msgpack, write_msgpack_array_header, write_msgpack_map_header, write_msgpack_str,
    write_msgpack_value,
};
pub(super) use nodedb_sql::planner::defaults::evaluate_default_expr;
pub(super) use rows::rows_to_msgpack_array;
