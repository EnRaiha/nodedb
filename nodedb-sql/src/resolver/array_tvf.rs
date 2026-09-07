// SPDX-License-Identifier: Apache-2.0

//! Array table-valued function resolution.
//!
//! An `ARRAY_*(...)` factor in a FROM clause names an array, not a
//! collection. This module synthesizes the relation its dims and attrs
//! expose so column references and equi-join keys against it resolve.

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_object_name_checked;
use crate::resolver::columns::ResolvedTable;
use crate::types::{
    ArrayCatalogView, CollectionInfo, ColumnInfo, EngineType, SqlCatalog, SqlDataType,
};
use crate::types_array::{ArrayAttrType, ArrayDimType};

/// If `factor` is `ARRAY_*(name, ...)`, look up the array via the
/// catalog and build a `ResolvedTable` whose columns mirror the array's
/// dims + attrs. Returns `Ok(None)` for any non-array-TVF factor.
pub(super) fn resolve_array_tvf(
    catalog: &dyn SqlCatalog,
    factor: &sqlparser::ast::TableFactor,
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

    let info = CollectionInfo {
        name: view.name.clone(),
        engine: EngineType::Array,
        columns: array_columns(&view),
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

fn array_columns(view: &ArrayCatalogView) -> Vec<ColumnInfo> {
    let mut cols = Vec::with_capacity(view.dims.len() + view.attrs.len());
    for d in &view.dims {
        cols.push(ColumnInfo {
            name: d.name.clone(),
            data_type: dim_type_to_sql(d.dtype),
            nullable: false,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        });
    }
    for a in &view.attrs {
        cols.push(ColumnInfo {
            name: a.name.clone(),
            data_type: attr_type_to_sql(a.dtype),
            nullable: a.nullable,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        });
    }
    cols
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
