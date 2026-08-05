// SPDX-License-Identifier: BUSL-1.1

//! Refusal of aggregates computed over a redacted column.
//!
//! An aggregate is evaluated in the Data Plane over the STORED values, which
//! are never redacted — redaction is a Control-Plane rewrite of the result
//! rows. Masking the scalar an aggregate produces therefore protects nothing:
//! `MIN(ssn)` has already disclosed a stored `ssn`, and `SUM(salary)` a fact
//! about every stored `salary`. There is no correct masking for these, so the
//! query is refused instead.

use nodedb_physical::physical_plan::AggregateSpec;
use nodedb_query::expr::SqlExpr;

use super::lookup::RefusalCtx;

/// One side of a join, as the plan names it.
pub(super) struct JoinSide<'a> {
    /// The side's alias, or `None` when the query did not alias it.
    pub(super) alias: Option<&'a str>,
    /// The collection the side reads.
    pub(super) collection: &'a str,
}

impl JoinSide<'_> {
    /// The qualifier this side's columns carry in the join's output row: the
    /// alias when there is one, the collection name otherwise.
    fn qualifier(&self) -> &str {
        self.alias.unwrap_or(self.collection)
    }
}

/// Refuse when any spec in `specs` aggregates a redacted column of `collection`.
pub(super) fn refuse_aggregates(
    ctx: &RefusalCtx<'_>,
    collection: &str,
    specs: &[AggregateSpec],
) -> crate::Result<()> {
    if collection.is_empty() {
        return Ok(());
    }
    for spec in specs {
        for field in read_fields(spec) {
            if ctx.field_is_redacted(collection, field) {
                return Err(refusal(collection, field));
            }
        }
    }
    Ok(())
}

/// Refuse when a post-join aggregate reads a redacted column of either side.
///
/// Post-join aggregates are `(function, field)` pairs whose `field` may be
/// qualified with a side's alias. A qualified field is checked against the side
/// it names; an unqualified one — and one whose qualifier matches neither side
/// — is checked against both, so an ambiguous name cannot slip past.
pub(super) fn refuse_join_aggregates(
    ctx: &RefusalCtx<'_>,
    left: &JoinSide<'_>,
    right: &JoinSide<'_>,
    aggregates: &[(String, String)],
) -> crate::Result<()> {
    for (_function, field) in aggregates {
        if is_count_star(field) {
            continue;
        }
        let (qualifier, column) = match field.split_once('.') {
            Some((qualifier, column)) => (Some(qualifier), column),
            None => (None, field.as_str()),
        };
        let names_a_side = qualifier.is_some_and(|qualifier| {
            qualifier == left.qualifier() || qualifier == right.qualifier()
        });
        for side in [left, right] {
            if names_a_side && qualifier != Some(side.qualifier()) {
                continue;
            }
            if !side.collection.is_empty() && ctx.field_is_redacted(side.collection, column) {
                return Err(refusal(side.collection, column));
            }
        }
    }
    Ok(())
}

/// Refuse when a facet request counts a redacted column.
///
/// Facet counts return `[{value, count}]` per field — the stored values
/// themselves, in a nested array the row-level masking hook does not reach.
pub(super) fn refuse_facet_fields(
    ctx: &RefusalCtx<'_>,
    collection: &str,
    fields: &[String],
) -> crate::Result<()> {
    if collection.is_empty() {
        return Ok(());
    }
    for field in fields {
        if ctx.field_is_redacted(collection, field) {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "column '{field}' on '{collection}' is redacted for this role: faceting a \
                     redacted column is not permitted — facet counts enumerate the stored values \
                     the policy protects"
                ),
            });
        }
    }
    Ok(())
}

fn refusal(collection: &str, field: &str) -> crate::Error {
    crate::Error::PlanError {
        detail: format!(
            "column '{field}' on '{collection}' is redacted for this role: aggregating a redacted \
             column is not permitted — the aggregate is computed over the stored values, so \
             masking its result would not protect them"
        ),
    }
}

/// Every stored column one aggregate spec reads.
///
/// `field` is the simple field-based argument; `expr` is the optional
/// per-document expression evaluated before aggregating (`MIN(LENGTH(ssn))`),
/// whose column references are read exactly the same way.
fn read_fields(spec: &AggregateSpec) -> Vec<&str> {
    let mut fields = Vec::new();
    if !is_count_star(&spec.field) {
        fields.push(spec.field.as_str());
    }
    if let Some(expr) = &spec.expr {
        collect_columns(expr, &mut fields);
    }
    fields
}

/// True for an aggregate argument that reads no column value.
///
/// `COUNT(*)` reaches the plan as the sentinel field `"*"` (see
/// `AggregateSpec::field`), and an argument-less aggregate as the empty
/// string. Neither discloses anything about a redacted column, so neither is
/// refused.
fn is_count_star(field: &str) -> bool {
    field == "*" || field.is_empty()
}

/// Collect every column reference in `expr`.
fn collect_columns<'e>(expr: &'e SqlExpr, out: &mut Vec<&'e str>) {
    match expr {
        SqlExpr::Column(name) | SqlExpr::OldColumn(name) | SqlExpr::ExcludedColumn(name) => {
            out.push(name.as_str());
        }
        SqlExpr::Literal(_) => {}
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        SqlExpr::Negate(inner) => collect_columns(inner, out),
        SqlExpr::Function { args, .. } => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        SqlExpr::Cast { expr, .. } => collect_columns(expr, out),
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_columns(operand, out);
            }
            for (when, then) in when_thens {
                collect_columns(when, out);
                collect_columns(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_columns(else_expr, out);
            }
        }
        SqlExpr::Coalesce(args) => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        SqlExpr::NullIf(left, right) => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        SqlExpr::IsNull { expr, .. } => collect_columns(expr, out),
    }
}
