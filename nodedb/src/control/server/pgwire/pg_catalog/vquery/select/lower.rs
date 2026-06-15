// SPDX-License-Identifier: BUSL-1.1

//! Lowering from sqlparser expression AST to the internal [`Expr`].

use sqlparser::ast::{
    BinaryOperator, DataType, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments,
    UnaryOperator, Value,
};

use super::super::expr::types::{AggFn, BinOp, CastType, Expr, ScalarFn};
use super::super::value::VValue;
use super::error::ParseError;

pub fn lower_expr(e: SqlExpr) -> Result<Expr, ParseError> {
    match e {
        SqlExpr::Value(v) => Ok(Expr::Literal(lower_literal(v.value)?)),
        SqlExpr::Identifier(id) => Ok(bareword(&id.value)),
        SqlExpr::CompoundIdentifier(ids) => {
            let name = ids
                .last()
                .ok_or_else(|| ParseError::Unsupported("empty compound identifier".into()))?
                .value
                .clone();
            let qualifier = if ids.len() >= 2 {
                Some(ids[ids.len() - 2].value.clone())
            } else {
                None
            };
            Ok(Expr::Column { qualifier, name })
        }
        SqlExpr::Nested(inner) => lower_expr(*inner),
        SqlExpr::UnaryOp { op, expr } => {
            let inner = Box::new(lower_expr(*expr)?);
            match op {
                UnaryOperator::Not => Ok(Expr::UnaryNot(inner)),
                UnaryOperator::Minus => Ok(Expr::UnaryNeg(inner)),
                UnaryOperator::Plus => Ok(*inner),
                other => Err(ParseError::Unsupported(format!("unary operator {other}"))),
            }
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let bop = lower_binop(&op)?;
            Ok(Expr::BinaryOp(
                Box::new(lower_expr(*left)?),
                bop,
                Box::new(lower_expr(*right)?),
            ))
        }
        SqlExpr::IsNull(e) => Ok(Expr::IsNull(Box::new(lower_expr(*e)?), false)),
        SqlExpr::IsNotNull(e) => Ok(Expr::IsNull(Box::new(lower_expr(*e)?), true)),
        SqlExpr::IsTrue(e) => Ok(Expr::BinaryOp(
            Box::new(lower_expr(*e)?),
            BinOp::Eq,
            Box::new(Expr::Literal(VValue::Bool(true))),
        )),
        SqlExpr::IsFalse(e) => Ok(Expr::BinaryOp(
            Box::new(lower_expr(*e)?),
            BinOp::Eq,
            Box::new(Expr::Literal(VValue::Bool(false))),
        )),
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let items = list
                .into_iter()
                .map(lower_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::InList(Box::new(lower_expr(*expr)?), items, negated))
        }
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between(
            Box::new(lower_expr(*expr)?),
            Box::new(lower_expr(*low)?),
            Box::new(lower_expr(*high)?),
            negated,
        )),
        SqlExpr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let Expr::Literal(VValue::Text(s)) = lower_expr(*pattern)? else {
                return Err(ParseError::Unsupported(
                    "LIKE pattern must be a string literal".into(),
                ));
            };
            Ok(Expr::Like(Box::new(lower_expr(*expr)?), s, negated))
        }
        SqlExpr::Cast {
            expr, data_type, ..
        } => {
            let target = lower_cast_type(&data_type)?;
            Ok(Expr::Cast(Box::new(lower_expr(*expr)?), target))
        }
        SqlExpr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => Ok(Expr::AnyAll {
            left: Box::new(lower_expr(*left)?),
            op: lower_binop(&compare_op)?,
            array: Box::new(lower_expr(*right)?),
            any: true,
        }),
        SqlExpr::AllOp {
            left,
            compare_op,
            right,
        } => Ok(Expr::AnyAll {
            left: Box::new(lower_expr(*left)?),
            op: lower_binop(&compare_op)?,
            array: Box::new(lower_expr(*right)?),
            any: false,
        }),
        SqlExpr::Array(arr) => {
            let items = arr
                .elem
                .into_iter()
                .map(lower_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::Array(items))
        }
        SqlExpr::Function(func) => lower_function(func),
        other => Err(ParseError::Unsupported(format!(
            "expression `{other}` not supported on virtual catalog tables"
        ))),
    }
}

/// A bareword identifier may be a niladic catalog keyword (`current_user`,
/// `current_schema`, …); otherwise it is a column reference.
fn bareword(name: &str) -> Expr {
    match name.to_ascii_lowercase().as_str() {
        "current_user" => Expr::ScalarFn(ScalarFn::CurrentUser, Vec::new()),
        "current_role" => Expr::ScalarFn(ScalarFn::CurrentRole, Vec::new()),
        "current_schema" => Expr::ScalarFn(ScalarFn::CurrentSchema, Vec::new()),
        "current_database" => Expr::ScalarFn(ScalarFn::CurrentDatabase, Vec::new()),
        _ => Expr::Column {
            qualifier: None,
            name: name.to_string(),
        },
    }
}

fn lower_binop(op: &BinaryOperator) -> Result<BinOp, ParseError> {
    Ok(match op {
        BinaryOperator::Eq => BinOp::Eq,
        BinaryOperator::NotEq => BinOp::NotEq,
        BinaryOperator::Lt => BinOp::Lt,
        BinaryOperator::LtEq => BinOp::LtEq,
        BinaryOperator::Gt => BinOp::Gt,
        BinaryOperator::GtEq => BinOp::GtEq,
        BinaryOperator::And => BinOp::And,
        BinaryOperator::Or => BinOp::Or,
        BinaryOperator::Plus => BinOp::Add,
        BinaryOperator::Minus => BinOp::Sub,
        BinaryOperator::Multiply => BinOp::Mul,
        BinaryOperator::Divide => BinOp::Div,
        other => return Err(ParseError::Unsupported(format!("binary operator {other}"))),
    })
}

fn lower_cast_type(dt: &DataType) -> Result<CastType, ParseError> {
    Ok(match dt {
        DataType::Regclass => CastType::Regclass,
        DataType::Text => CastType::Text,
        DataType::Varchar(_) | DataType::Char(_) => CastType::Text,
        DataType::Boolean | DataType::Bool => CastType::Bool,
        DataType::Int(_) | DataType::Integer(_) => CastType::Int4,
        DataType::SmallInt(_) => CastType::Int4,
        DataType::BigInt(_) => CastType::Int8,
        DataType::Custom(name, _) => {
            let last = name
                .0
                .last()
                .map(|p| p.to_string().to_ascii_lowercase())
                .unwrap_or_default();
            match last.as_str() {
                "regclass" => CastType::Regclass,
                "regtype" => CastType::Regtype,
                "oid" => CastType::Oid,
                "name" | "text" | "varchar" => CastType::Text,
                "int8" | "bigint" => CastType::Int8,
                "int4" | "int" | "integer" => CastType::Int4,
                "bool" | "boolean" => CastType::Bool,
                other => {
                    return Err(ParseError::Unsupported(format!(
                        "cast to type `{other}` not supported on virtual catalog tables"
                    )));
                }
            }
        }
        other => {
            return Err(ParseError::Unsupported(format!(
                "cast to type `{other}` not supported on virtual catalog tables"
            )));
        }
    })
}

fn lower_function(func: sqlparser::ast::Function) -> Result<Expr, ParseError> {
    let name = func
        .name
        .0
        .last()
        .map(|p| match p {
            sqlparser::ast::ObjectNamePart::Identifier(id) => id.value.to_ascii_lowercase(),
            sqlparser::ast::ObjectNamePart::Function(_) => String::new(),
        })
        .unwrap_or_default();

    let args = match func.args {
        FunctionArguments::List(list) => list.args,
        FunctionArguments::None => Vec::new(),
        FunctionArguments::Subquery(_) => {
            return Err(ParseError::Unsupported(
                "subquery as function argument not supported".into(),
            ));
        }
    };

    if let Some(scalar) = scalar_fn(&name) {
        let mut lowered = Vec::with_capacity(args.len());
        for arg in args {
            lowered.push(lower_function_arg(arg)?);
        }
        return Ok(Expr::ScalarFn(scalar, lowered));
    }

    let agg = match name.as_str() {
        "count" => AggFn::Count,
        "sum" => AggFn::Sum,
        "min" => AggFn::Min,
        "max" => AggFn::Max,
        "avg" => AggFn::Avg,
        _ => {
            return Err(ParseError::Unsupported(format!(
                "function `{name}` not supported on virtual catalog tables"
            )));
        }
    };

    if args.len() != 1 {
        return Err(ParseError::Unsupported(format!(
            "aggregate `{name}` expects exactly one argument"
        )));
    }
    let arg_expr = match args.into_iter().next().unwrap() {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Expr::Star,
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => lower_expr(e)?,
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => Expr::Star,
        FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. } => {
            return Err(ParseError::Unsupported(
                "named function arguments not supported on virtual catalog tables".into(),
            ));
        }
    };
    Ok(Expr::Aggregate(agg, Box::new(arg_expr)))
}

fn scalar_fn(name: &str) -> Option<ScalarFn> {
    Some(match name {
        "current_schemas" => ScalarFn::CurrentSchemas,
        "current_schema" => ScalarFn::CurrentSchema,
        "current_database" => ScalarFn::CurrentDatabase,
        "current_user" => ScalarFn::CurrentUser,
        "current_role" => ScalarFn::CurrentRole,
        "version" => ScalarFn::Version,
        _ => return None,
    })
}

fn lower_function_arg(arg: FunctionArg) -> Result<Expr, ParseError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => lower_expr(e),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
        | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => Err(
            ParseError::Unsupported("wildcard argument not supported here".into()),
        ),
        FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. } => Err(ParseError::Unsupported(
            "named function arguments not supported".into(),
        )),
    }
}

pub fn lower_literal(v: Value) -> Result<VValue, ParseError> {
    match v {
        Value::Null => Ok(VValue::Null),
        Value::Boolean(b) => Ok(VValue::Bool(b)),
        Value::Number(s, _) => s.parse::<i64>().map(VValue::Int8).map_err(|_| {
            ParseError::Unsupported(format!(
                "non-integer numeric literal `{s}` not supported on virtual tables"
            ))
        }),
        // Unbound `$N` placeholders are only reachable on the Parse/Describe
        // path before parameters are bound. Treat as NULL so schema inference
        // succeeds; Execute always re-parses with parameters bound.
        Value::Placeholder(_) => Ok(VValue::Null),
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::EscapedStringLiteral(s)
        | Value::NationalStringLiteral(s)
        | Value::DollarQuotedString(sqlparser::ast::DollarQuotedString { value: s, .. }) => {
            Ok(VValue::Text(s))
        }
        other => Err(ParseError::Unsupported(format!(
            "literal value `{other}` not supported on virtual catalog tables"
        ))),
    }
}
