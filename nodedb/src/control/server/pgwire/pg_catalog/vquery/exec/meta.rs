// SPDX-License-Identifier: BUSL-1.1

//! Output schema, projection naming, and result-type inference.

use super::super::expr::types::{AggFn, BinOp, CastType, Expr, ScalarFn};
use super::super::table::VTable;
use super::super::value::{VType, VValue};

/// Output schema name + value type for one projected column.
#[derive(Debug, Clone)]
pub struct OutColumn {
    pub name: String,
    pub ty: VType,
}

#[derive(Debug)]
pub struct ResultSet {
    pub columns: Vec<OutColumn>,
    pub rows: Vec<Vec<VValue>>,
}

pub fn projection_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Aggregate(agg, _) => aggregate_name(*agg),
        Expr::ScalarFn(f, _) => scalar_fn_name(*f).to_string(),
        Expr::Cast(_, target) => cast_name(*target).to_string(),
        _ => "?column?".to_string(),
    }
}

pub fn aggregate_name(agg: AggFn) -> String {
    match agg {
        AggFn::Count => "count".into(),
        AggFn::Sum => "sum".into(),
        AggFn::Min => "min".into(),
        AggFn::Max => "max".into(),
        AggFn::Avg => "avg".into(),
    }
}

fn scalar_fn_name(f: ScalarFn) -> &'static str {
    match f {
        ScalarFn::CurrentSchemas => "current_schemas",
        ScalarFn::CurrentSchema => "current_schema",
        ScalarFn::CurrentDatabase => "current_database",
        ScalarFn::CurrentUser => "current_user",
        ScalarFn::CurrentRole => "current_role",
        ScalarFn::Version => "version",
    }
}

fn cast_name(target: CastType) -> &'static str {
    match target {
        CastType::Regclass => "regclass",
        CastType::Regtype => "regtype",
        CastType::Oid => "oid",
        CastType::Int8 => "int8",
        CastType::Int4 => "int4",
        CastType::Text => "text",
        CastType::Bool => "bool",
    }
}

pub fn infer_type(expr: &Expr, table: &VTable) -> VType {
    match expr {
        Expr::Literal(VValue::Bool(_)) => VType::Bool,
        Expr::Literal(VValue::Int4(_)) => VType::Int4,
        Expr::Literal(VValue::Int8(_)) => VType::Int8,
        Expr::Literal(VValue::Text(_)) => VType::Text,
        Expr::Literal(VValue::Null) => VType::Text,
        Expr::Literal(VValue::Array(_)) => VType::Text,
        Expr::Column { qualifier, name } => table
            .resolve_column(qualifier.as_deref(), name)
            .ok()
            .map(|i| table.columns[i].ty)
            .unwrap_or(VType::Text),
        Expr::BinaryOp(_, op, _) => match op {
            BinOp::Eq
            | BinOp::NotEq
            | BinOp::Lt
            | BinOp::LtEq
            | BinOp::Gt
            | BinOp::GtEq
            | BinOp::And
            | BinOp::Or => VType::Bool,
            _ => VType::Int8,
        },
        Expr::UnaryNot(_)
        | Expr::IsNull(_, _)
        | Expr::InList(_, _, _)
        | Expr::Between(_, _, _, _)
        | Expr::Like(_, _, _)
        | Expr::AnyAll { .. } => VType::Bool,
        Expr::UnaryNeg(_) => VType::Int8,
        Expr::Cast(_, target) => match target {
            CastType::Regclass | CastType::Regtype | CastType::Oid | CastType::Int8 => VType::Int8,
            CastType::Int4 => VType::Int4,
            CastType::Text => VType::Text,
            CastType::Bool => VType::Bool,
        },
        Expr::Array(_) => VType::Text,
        Expr::ScalarFn(_, _) => VType::Text,
        Expr::Aggregate(AggFn::Count, _) => VType::Int8,
        Expr::Aggregate(_, e) => infer_type(e, table),
        Expr::Star => VType::Text,
    }
}
