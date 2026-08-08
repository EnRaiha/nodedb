// SPDX-License-Identifier: Apache-2.0

//! `SELECT * FROM ARRAY_*(...)` table-valued function planning:
//! slice / project / aggregate / elementwise.

use sqlparser::ast;

use nodedb_types::Value;

use super::helpers::{
    collect_args, expect_string_array, expect_string_literal, expect_u32, is_null_literal,
    require_array_name, value_to_coord_literal,
};
use crate::error::{Result, SqlError};
use crate::parser::object_literal::parse_object_literal_complete;
use crate::temporal::TemporalScope;
use crate::types::{SqlCatalog, SqlPlan};
use crate::types_array::{ArrayBinaryOpAst, ArrayReducerAst, ArraySliceAst, NamedDimRange};

/// Try to intercept a `SELECT * FROM array_xxx(...)` table-valued
/// function call. Returns `Ok(Some(plan))` on a match, `Ok(None)` if
/// the FROM is not an array function (caller falls through to normal
/// catalog resolution).
///
/// `temporal` carries any `AS OF SYSTEM TIME` / `AS OF VALID TIME` qualifiers
/// extracted by the pre-processor. It is propagated verbatim into
/// `SqlPlan::ArraySlice` and `SqlPlan::ArrayAgg` so the planner-to-physical
/// conversion can populate the corresponding `ArrayOp` fields. When neither
/// clause was present, `temporal` is `TemporalScope::default()`, which maps to
/// the live-state fast path in the Data Plane handler.
pub fn try_plan_array_table_fn(
    from: &[ast::TableWithJoins],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<Option<SqlPlan>> {
    if from.len() != 1 {
        return Ok(None);
    }
    let twj = &from[0];
    if !twj.joins.is_empty() {
        return Ok(None);
    }
    let (name, args) = match &twj.relation {
        ast::TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } => {
            if let Some(alias) = alias {
                crate::reserved::check_ast_identifier(&alias.name)?;
            }
            (name, args)
        }
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name_checked(name)?;
    let arg_exprs = collect_args(&args.args);
    match fn_name.as_str() {
        "array_slice" => Ok(Some(plan_slice(&arg_exprs, catalog, temporal)?)),
        "array_project" => Ok(Some(plan_project(&arg_exprs, catalog)?)),
        "array_agg" => Ok(Some(plan_agg(&arg_exprs, catalog, temporal)?)),
        "array_elementwise" => Ok(Some(plan_elementwise(&arg_exprs, catalog)?)),
        _ => Ok(None),
    }
}

fn plan_slice(
    args: &[ast::Expr],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    if args.len() < 2 || args.len() > 4 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_SLICE expects 2..=4 args (name, slice_obj, [attrs], [limit]); got {}",
                args.len()
            ),
        });
    }
    let name = require_array_name(args, 0, "ARRAY_SLICE", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_SLICE: array '{name}' not found"),
        })?;

    // Slice-predicate literal: encoded as a quoted string carrying the
    // brace-form object literal. The PostgreSQL dialect does not accept
    // bare `{...}` in expression position, so we decode the string
    // contents here.
    let slice_str = expect_string_literal(&args[1], "ARRAY_SLICE slice predicate")?;
    let parsed =
        parse_object_literal_complete(&slice_str).ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_SLICE: slice predicate must be an object literal: {slice_str}"),
        })?;
    let map = parsed.map_err(|detail| SqlError::Unsupported {
        detail: format!("ARRAY_SLICE: slice parse: {detail}"),
    })?;
    let mut dim_ranges: Vec<NamedDimRange> = Vec::with_capacity(map.len());
    for (dim, val) in map {
        // Verify the dim exists on the array.
        if !view.dims.iter().any(|d| d.name == dim) {
            return Err(SqlError::Unsupported {
                detail: format!("ARRAY_SLICE: array '{name}' has no dim '{dim}'"),
            });
        }
        let arr = match val {
            Value::Array(a) if a.len() == 2 => a,
            _ => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "ARRAY_SLICE: dim '{dim}' range must be a 2-element array [lo, hi]"
                    ),
                });
            }
        };
        let lo = value_to_coord_literal(&arr[0], &dim)?;
        let hi = value_to_coord_literal(&arr[1], &dim)?;
        dim_ranges.push(NamedDimRange { dim, lo, hi });
    }

    let attr_projection = if args.len() >= 3 {
        match &args[2] {
            ast::Expr::Value(v) if matches!(v.value, ast::Value::SingleQuotedString(ref s) if s == "*") => {
                Vec::new()
            }
            _ => expect_string_array(&args[2], "ARRAY_SLICE attr projection")?,
        }
    } else {
        Vec::new()
    };
    // Validate attr names against the catalog.
    for attr in &attr_projection {
        if !view.attrs.iter().any(|a| &a.name == attr) {
            return Err(SqlError::Unsupported {
                detail: format!("ARRAY_SLICE: array '{name}' has no attr '{attr}'"),
            });
        }
    }

    let limit = if args.len() >= 4 {
        expect_u32(&args[3], "ARRAY_SLICE limit")?
    } else {
        0
    };

    Ok(SqlPlan::ArraySlice {
        name,
        slice: ArraySliceAst { dim_ranges },
        attr_projection,
        limit,
        temporal,
    })
}

fn plan_project(args: &[ast::Expr], catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if args.len() != 2 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_PROJECT expects 2 args (name, [attrs]); got {}",
                args.len()
            ),
        });
    }
    let name = require_array_name(args, 0, "ARRAY_PROJECT", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_PROJECT: array '{name}' not found"),
        })?;
    let attr_projection = expect_string_array(&args[1], "ARRAY_PROJECT attrs")?;
    if attr_projection.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "ARRAY_PROJECT: attr list must not be empty".into(),
        });
    }
    for attr in &attr_projection {
        if !view.attrs.iter().any(|a| &a.name == attr) {
            return Err(SqlError::Unsupported {
                detail: format!("ARRAY_PROJECT: array '{name}' has no attr '{attr}'"),
            });
        }
    }
    Ok(SqlPlan::ArrayProject {
        name,
        attr_projection,
    })
}

fn plan_agg(
    args: &[ast::Expr],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    if args.len() < 3 || args.len() > 4 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_AGG expects 3..=4 args (name, attr, reducer, [group_by_dim]); got {}",
                args.len()
            ),
        });
    }
    let name = require_array_name(args, 0, "ARRAY_AGG", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_AGG: array '{name}' not found"),
        })?;

    let attr = expect_string_literal(&args[1], "ARRAY_AGG attr")?;
    if !view.attrs.iter().any(|a| a.name == attr) {
        return Err(SqlError::Unsupported {
            detail: format!("ARRAY_AGG: array '{name}' has no attr '{attr}'"),
        });
    }

    let reducer_str = expect_string_literal(&args[2], "ARRAY_AGG reducer")?;
    let reducer = ArrayReducerAst::parse(&reducer_str).ok_or_else(|| SqlError::Unsupported {
        detail: format!("ARRAY_AGG: unknown reducer '{reducer_str}' (want sum/count/min/max/mean)"),
    })?;

    let group_by_dim = if args.len() == 4 && !is_null_literal(&args[3]) {
        let dim = expect_string_literal(&args[3], "ARRAY_AGG group_by_dim")?;
        if !view.dims.iter().any(|d| d.name == dim) {
            return Err(SqlError::Unsupported {
                detail: format!("ARRAY_AGG: array '{name}' has no dim '{dim}'"),
            });
        }
        Some(dim)
    } else {
        None
    };

    Ok(SqlPlan::ArrayAgg {
        name,
        attr,
        reducer,
        group_by_dim,
        temporal,
    })
}

fn plan_elementwise(args: &[ast::Expr], catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if args.len() != 4 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_ELEMENTWISE expects 4 args (left, right, op, attr); got {}",
                args.len()
            ),
        });
    }
    let left = require_array_name(args, 0, "ARRAY_ELEMENTWISE", catalog)?;
    let right = require_array_name(args, 1, "ARRAY_ELEMENTWISE", catalog)?;
    let lview = catalog
        .lookup_array(&left)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_ELEMENTWISE: array '{left}' not found"),
        })?;
    let rview = catalog
        .lookup_array(&right)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_ELEMENTWISE: array '{right}' not found"),
        })?;
    if lview.dims.len() != rview.dims.len() || lview.attrs.len() != rview.attrs.len() {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_ELEMENTWISE: arrays '{left}' and '{right}' must share schema shape"
            ),
        });
    }
    let op_str = expect_string_literal(&args[2], "ARRAY_ELEMENTWISE op")?;
    let op = ArrayBinaryOpAst::parse(&op_str).ok_or_else(|| SqlError::Unsupported {
        detail: format!("ARRAY_ELEMENTWISE: unknown op '{op_str}' (want add/sub/mul/div)"),
    })?;
    let attr = expect_string_literal(&args[3], "ARRAY_ELEMENTWISE attr")?;
    if !lview.attrs.iter().any(|a| a.name == attr) {
        return Err(SqlError::Unsupported {
            detail: format!("ARRAY_ELEMENTWISE: array '{left}' has no attr '{attr}'"),
        });
    }
    Ok(SqlPlan::ArrayElementwise {
        left,
        right,
        op,
        attr,
    })
}
