// SPDX-License-Identifier: Apache-2.0

//! ARRAY_* table-valued function as a JOIN source.
//!
//! When a JOIN's left or right relation is `ARRAY_SLICE(...)` (or
//! sibling read TVFs), the planner lowers it to the corresponding
//! `SqlPlan::Array*` variant directly — bypassing catalog lookup.
//! The TVF's output rows participate in the join key resolution like
//! any other relation. A slice or project row holds one field per dim
//! and per attr, keyed by name, so equi-join keys read them directly.

use sqlparser::ast;

use crate::error::Result;
use crate::temporal::TemporalScope;
use crate::types::{SqlCatalog, SqlPlan};

/// If `rel` is a `ARRAY_*(...)` TVF call, return the corresponding
/// `SqlPlan::Array*` node. Otherwise return `Ok(None)` so the caller
/// falls through to the named-table scan path.
pub(super) fn try_plan_relation(
    rel: &ast::TableFactor,
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<Option<SqlPlan>> {
    let name = match rel {
        ast::TableFactor::Table {
            name,
            args: Some(_),
            ..
        } => name,
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name_checked(name)?;
    if !is_array_tvf(&fn_name) {
        return Ok(None);
    }

    // Reuse the SELECT * FROM ARRAY_*(...) planner by wrapping the
    // single relation into a one-element TableWithJoins. Both call
    // sites need exactly the same validation (arg arity, schema
    // resolution, dim/attr name checks) — re-deriving it here would
    // duplicate that logic and let the two paths drift.
    let twj = ast::TableWithJoins {
        relation: rel.clone(),
        joins: Vec::new(),
    };
    Ok(super::super::array_fn::try_plan_array_table_fn(
        std::slice::from_ref(&twj),
        catalog,
        temporal,
    )?
    .map(super::super::array_fn::ArrayTableFn::into_plan))
}

fn is_array_tvf(name: &str) -> bool {
    matches!(
        name,
        "array_slice" | "array_project" | "array_agg" | "array_elementwise"
    )
}
