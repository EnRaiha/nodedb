// SPDX-License-Identifier: Apache-2.0

//! Array table-valued function resolution.
//!
//! An `ARRAY_*(...)` factor in a FROM clause names an array, not a
//! collection. This module synthesizes the relation the call's rows expose
//! so column references and equi-join keys against it resolve.

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_object_name_checked;
use crate::planner::array_fn::{ArrayTableFn, try_plan_array_table_fn};
use crate::resolver::columns::ResolvedTable;
use crate::temporal::TemporalScope;
use crate::types::{
    ArrayCatalogView, ArraySlicePlan, CollectionInfo, ColumnInfo, EngineType, SqlCatalog,
    SqlDataType,
};
use crate::types_array::{ArrayAttrType, ArrayDimType};

/// The column an audit-log slice (`AS OF SYSTEM TIME NULL`) stamps on each
/// row: the version's system time in milliseconds.
pub const ARRAY_SYSTEM_TIME_COLUMN: &str = "_ts_system";

/// If `factor` is `ARRAY_*(name, ...)`, look up the array via the
/// catalog and build a `ResolvedTable` whose columns are the ones the
/// call's rows carry. Returns `Ok(None)` for any non-array-TVF factor.
pub(super) fn resolve_array_tvf(
    catalog: &dyn SqlCatalog,
    factor: &sqlparser::ast::TableFactor,
    temporal: TemporalScope,
) -> Result<Option<ResolvedTable>> {
    let (fn_name, args, alias) = match factor {
        sqlparser::ast::TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } => (
            normalize_object_name_checked(name)?,
            args,
            alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?,
        ),
        _ => return Ok(None),
    };
    if !matches!(
        fn_name.as_str(),
        "array_slice" | "array_project" | "array_agg" | "array_elementwise"
    ) {
        return Ok(None);
    }

    // First positional arg is the array name as a string literal.
    let first = args.args.first().ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array-name argument"),
    })?;
    let array_name = extract_string_literal_arg(first).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: array-name argument must be a string literal"),
    })?;
    let view = catalog
        .lookup_array(&array_name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: array_name.clone(),
        })?;

    let twj = sqlparser::ast::TableWithJoins {
        relation: factor.clone(),
        joins: Vec::new(),
    };
    let Some(call) = try_plan_array_table_fn(std::slice::from_ref(&twj), catalog, temporal)? else {
        return Ok(None);
    };

    let info = CollectionInfo {
        name: view.name.clone(),
        engine: EngineType::Array,
        columns: array_table_fn_columns(catalog, &call)?,
        primary_key: None,
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema: false,
    };
    Ok(Some(ResolvedTable {
        name: view.name,
        alias,
        info,
    }))
}

/// The columns of the relation an array cell read exposes: every dim in
/// schema order, then each attr `attrs` names in that order, or every attr
/// in schema order when `attrs` is empty.
pub fn array_relation_columns(view: &ArrayCatalogView, attrs: &[String]) -> Vec<ColumnInfo> {
    let mut cols = Vec::with_capacity(view.dims.len() + view.attrs.len());
    for d in &view.dims {
        cols.push(column(&d.name, dim_type_to_sql(d.dtype), false));
    }
    if attrs.is_empty() {
        for a in &view.attrs {
            cols.push(column(&a.name, attr_type_to_sql(a.dtype), a.nullable));
        }
    } else {
        for name in attrs {
            if let Some(a) = view.attrs.iter().find(|a| &a.name == name) {
                cols.push(column(&a.name, attr_type_to_sql(a.dtype), a.nullable));
            }
        }
    }
    cols
}

/// The columns a slice's rows carry: its dims and attrs, plus
/// [`ARRAY_SYSTEM_TIME_COLUMN`] when it reads every system-time version.
pub fn array_slice_columns(view: &ArrayCatalogView, slice: &ArraySlicePlan) -> Vec<ColumnInfo> {
    let mut cols = array_relation_columns(view, &slice.attr_projection);
    if matches!(
        slice.temporal.system_time,
        nodedb_types::SystemTimeScope::AllVersions
    ) {
        cols.push(column(ARRAY_SYSTEM_TIME_COLUMN, SqlDataType::Int64, false));
    }
    cols
}

/// The columns the rows of one array table-function call carry.
///
/// - A slice or project row holds the array's dims and the requested attrs.
/// - An aggregate row holds `result`, after `group` when it groups by a dim.
/// - An elementwise row holds the `coords` and `attrs` arrays of one cell.
pub fn array_table_fn_columns(
    catalog: &dyn SqlCatalog,
    call: &ArrayTableFn,
) -> Result<Vec<ColumnInfo>> {
    match call {
        ArrayTableFn::Slice(slice) | ArrayTableFn::Project(slice) => {
            Ok(array_slice_columns(&view_of(catalog, &slice.name)?, slice))
        }
        ArrayTableFn::Agg(agg) => {
            let view = view_of(catalog, &agg.name)?;
            let mut cols = Vec::with_capacity(2);
            if let Some(dim) = &agg.group_by_dim
                && let Some(d) = view.dims.iter().find(|d| &d.name == dim)
            {
                cols.push(column("group", dim_type_to_sql(d.dtype), false));
            }
            cols.push(column("result", SqlDataType::Float64, true));
            Ok(cols)
        }
        ArrayTableFn::Elementwise(_) => Ok(vec![
            column("coords", SqlDataType::Json, false),
            column("attrs", SqlDataType::Json, false),
        ]),
    }
}

fn view_of(catalog: &dyn SqlCatalog, name: &str) -> Result<ArrayCatalogView> {
    catalog
        .lookup_array(name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: name.to_string(),
        })
}

fn column(name: &str, data_type: SqlDataType, nullable: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable,
        is_primary_key: false,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

fn dim_type_to_sql(t: ArrayDimType) -> SqlDataType {
    match t {
        ArrayDimType::Int64 => SqlDataType::Int64,
        ArrayDimType::Float64 => SqlDataType::Float64,
        ArrayDimType::TimestampMs => SqlDataType::Timestamp,
        ArrayDimType::String => SqlDataType::String,
    }
}

fn attr_type_to_sql(t: ArrayAttrType) -> SqlDataType {
    match t {
        ArrayAttrType::Int64 => SqlDataType::Int64,
        ArrayAttrType::Float64 => SqlDataType::Float64,
        ArrayAttrType::String => SqlDataType::String,
        ArrayAttrType::Bytes => SqlDataType::Bytes,
    }
}

fn extract_string_literal_arg(arg: &sqlparser::ast::FunctionArg) -> Option<String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value};
    let expr = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        _ => return None,
    };
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}
