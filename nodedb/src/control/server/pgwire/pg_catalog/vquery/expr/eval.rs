// SPDX-License-Identifier: BUSL-1.1

//! Row-level evaluation of expressions against a (possibly joined) table.

use super::super::table::{ResolveError, VTable};
use super::super::value::VValue;
use super::cast::{EvalCtx, eval_cast, eval_scalar_fn, like_match};
use super::types::{BinOp, EvalError, Expr};

/// Evaluate an expression in the context of a single row.
pub fn eval(
    expr: &Expr,
    row: &[VValue],
    table: &VTable,
    ctx: &EvalCtx,
) -> Result<VValue, EvalError> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Star => Ok(VValue::Null),
        Expr::Column { qualifier, name } => {
            let idx = table
                .resolve_column(qualifier.as_deref(), name)
                .map_err(|e| match e {
                    ResolveError::Unknown(s) => EvalError::UnknownColumn(s),
                    ResolveError::Ambiguous(s) => EvalError::AmbiguousColumn(s),
                })?;
            Ok(row[idx].clone())
        }
        Expr::UnaryNot(e) => match eval(e, row, table, ctx)? {
            VValue::Null => Ok(VValue::Null),
            VValue::Bool(b) => Ok(VValue::Bool(!b)),
            _ => Err(EvalError::TypeMismatch("NOT requires boolean".into())),
        },
        Expr::UnaryNeg(e) => match eval(e, row, table, ctx)? {
            VValue::Null => Ok(VValue::Null),
            VValue::Int4(i) => Ok(VValue::Int4(-i)),
            VValue::Int8(i) => Ok(VValue::Int8(-i)),
            _ => Err(EvalError::TypeMismatch("unary - on non-integer".into())),
        },
        Expr::IsNull(e, negated) => {
            let is_null = eval(e, row, table, ctx)?.is_null();
            Ok(VValue::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::BinaryOp(l, op, r) => {
            let lv = eval(l, row, table, ctx)?;
            let rv = eval(r, row, table, ctx)?;
            apply_binary(op, &lv, &rv)
        }
        Expr::InList(e, items, negated) => {
            let v = eval(e, row, table, ctx)?;
            if v.is_null() {
                return Ok(VValue::Null);
            }
            let mut found = false;
            let mut any_null = false;
            for item in items {
                let iv = eval(item, row, table, ctx)?;
                if iv.is_null() {
                    any_null = true;
                    continue;
                }
                if let Some(std::cmp::Ordering::Equal) = v.sql_cmp(&iv) {
                    found = true;
                    break;
                }
            }
            let result = if found {
                true
            } else if any_null {
                return Ok(VValue::Null);
            } else {
                false
            };
            Ok(VValue::Bool(if *negated { !result } else { result }))
        }
        Expr::Between(e, lo, hi, negated) => {
            let v = eval(e, row, table, ctx)?;
            let lov = eval(lo, row, table, ctx)?;
            let hiv = eval(hi, row, table, ctx)?;
            if v.is_null() || lov.is_null() || hiv.is_null() {
                return Ok(VValue::Null);
            }
            let in_range = matches!(
                v.sql_cmp(&lov),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ) && matches!(
                v.sql_cmp(&hiv),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            );
            Ok(VValue::Bool(if *negated { !in_range } else { in_range }))
        }
        Expr::Like(e, pattern, negated) => {
            let v = eval(e, row, table, ctx)?;
            let Some(s) = v.as_text() else {
                if v.is_null() {
                    return Ok(VValue::Null);
                }
                return Err(EvalError::TypeMismatch("LIKE requires text".into()));
            };
            let m = like_match(s, pattern);
            Ok(VValue::Bool(if *negated { !m } else { m }))
        }
        Expr::Cast(inner, target) => {
            let v = eval(inner, row, table, ctx)?;
            eval_cast(v, *target, ctx)
        }
        Expr::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(eval(item, row, table, ctx)?);
            }
            Ok(VValue::Array(out))
        }
        Expr::ScalarFn(func, args) => {
            let mut argv = Vec::with_capacity(args.len());
            for a in args {
                argv.push(eval(a, row, table, ctx)?);
            }
            eval_scalar_fn(*func, &argv, ctx)
        }
        Expr::AnyAll {
            left,
            op,
            array,
            any,
        } => {
            let lv = eval(left, row, table, ctx)?;
            let arr = eval(array, row, table, ctx)?;
            eval_any_all(&lv, *op, &arr, *any)
        }
        Expr::Aggregate(_, _) => Err(EvalError::AggregateInPredicate),
    }
}

/// `left <op> ANY(array)` / `left <op> ALL(array)` with SQL NULL semantics.
fn eval_any_all(left: &VValue, op: BinOp, array: &VValue, any: bool) -> Result<VValue, EvalError> {
    if left.is_null() || array.is_null() {
        return Ok(VValue::Null);
    }
    let Some(items) = array.as_array() else {
        return Err(EvalError::TypeMismatch(
            "ANY/ALL requires an array right-hand side".into(),
        ));
    };
    let mut saw_null = false;
    for item in items {
        match apply_binary(&op, left, item)? {
            VValue::Bool(true) => {
                if any {
                    return Ok(VValue::Bool(true));
                }
            }
            VValue::Bool(false) => {
                if !any {
                    return Ok(VValue::Bool(false));
                }
            }
            VValue::Null => saw_null = true,
            other => {
                return Err(EvalError::TypeMismatch(format!(
                    "non-boolean comparison in ANY/ALL: {other:?}"
                )));
            }
        }
    }
    // ANY: no match found — NULL if any comparison was unknown, else false.
    // ALL: no false found — NULL if any comparison was unknown, else true.
    if saw_null {
        Ok(VValue::Null)
    } else {
        Ok(VValue::Bool(!any))
    }
}

pub fn apply_binary(op: &BinOp, l: &VValue, r: &VValue) -> Result<VValue, EvalError> {
    match op {
        BinOp::And => {
            return Ok(match (l.as_bool(), r.as_bool()) {
                (Some(true), Some(true)) => VValue::Bool(true),
                (Some(false), _) | (_, Some(false)) => VValue::Bool(false),
                _ => VValue::Null,
            });
        }
        BinOp::Or => {
            return Ok(match (l.as_bool(), r.as_bool()) {
                (Some(true), _) | (_, Some(true)) => VValue::Bool(true),
                (Some(false), Some(false)) => VValue::Bool(false),
                _ => VValue::Null,
            });
        }
        _ => {}
    }
    if l.is_null() || r.is_null() {
        return Ok(VValue::Null);
    }
    match op {
        BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
            let Some(ord) = l.sql_cmp(r) else {
                return Err(EvalError::TypeMismatch(format!(
                    "incompatible comparison: {l:?} vs {r:?}"
                )));
            };
            let result = match op {
                BinOp::Eq => ord == std::cmp::Ordering::Equal,
                BinOp::NotEq => ord != std::cmp::Ordering::Equal,
                BinOp::Lt => ord == std::cmp::Ordering::Less,
                BinOp::LtEq => ord != std::cmp::Ordering::Greater,
                BinOp::Gt => ord == std::cmp::Ordering::Greater,
                BinOp::GtEq => ord != std::cmp::Ordering::Less,
                _ => unreachable!(),
            };
            Ok(VValue::Bool(result))
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => {
            let (Some(x), Some(y)) = (l.as_i64(), r.as_i64()) else {
                return Err(EvalError::TypeMismatch(
                    "arithmetic requires integer operands".into(),
                ));
            };
            let result = match op {
                BinOp::Add => x.wrapping_add(y),
                BinOp::Sub => x.wrapping_sub(y),
                BinOp::Mul => x.wrapping_mul(y),
                BinOp::Div => {
                    if y == 0 {
                        return Err(EvalError::TypeMismatch("division by zero".into()));
                    }
                    x / y
                }
                _ => unreachable!(),
            };
            Ok(VValue::Int8(result))
        }
        BinOp::And | BinOp::Or => unreachable!(),
    }
}

/// SQL truth value of a predicate result: only `true` is truthy.
pub fn truthy(v: &VValue) -> bool {
    matches!(v, VValue::Bool(true))
}
