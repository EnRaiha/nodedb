// SPDX-License-Identifier: Apache-2.0

//! MERGE statement planning.
//!
//! Translates `sqlparser::ast::Statement::Merge` into `SqlPlan::Merge`.
//! Supported engines: `document_schemaless`, `document_strict`.
//! All other engines return `SqlError::Unsupported`.

use nodedb_types::DatabaseId;
use sqlparser::ast::{self, MergeAction, MergeClauseKind as AstMergeClauseKind, MergeInsertKind};

use super::ast_helpers::{qualified_ident_pair, strip_and_convert_filters};
use crate::engine_rules::{self, MergeParams, ScanParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::resolver::expr::convert_expr;
use crate::temporal::TemporalScope;
use crate::types::*;
use crate::types::{MergeClauseKind, MergePlanAction, MergePlanClause, SqlPlan};

/// Plan a `MERGE INTO target USING source ON ... WHEN ... THEN ...` statement.
pub fn plan_merge(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Merge(merge) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected MERGE statement".into(),
        });
    };

    if merge.clauses.is_empty() {
        return Err(SqlError::Parse {
            detail: "MERGE statement requires at least one WHEN arm".into(),
        });
    }

    // ── Resolve target ──
    let (target_name, target_alias) = extract_table_factor_name_alias(&merge.table)?;
    // The ON clause uses the alias (or table name if no alias) as the qualifier.
    let target_ref = target_alias.as_deref().unwrap_or(target_name.as_str());
    let target_info = catalog
        .get_collection(DatabaseId::DEFAULT, &target_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: target_name.clone(),
        })?;

    // ── Resolve source ──
    let source_plan = plan_merge_source(&merge.source, catalog)?;
    let source_alias = merge_source_alias(&merge.source, &source_plan)?;

    // ── Parse ON clause into equi-join columns ──
    let (target_join_col, source_join_col) =
        extract_merge_equijoin(&merge.on, target_ref, &source_alias)?;

    // ── Convert WHEN clauses ──
    let clauses = convert_merge_clauses(&merge.clauses, target_ref, &source_alias)?;

    // ── Dispatch to engine rules ──
    let rules = engine_rules::resolve_engine_rules(target_info.engine);
    rules.plan_merge(MergeParams {
        collection: target_name,
        source: Box::new(source_plan),
        target_join_col,
        source_join_col,
        source_alias,
        clauses,
        // sqlparser folds MSSQL's `OUTPUT ... [INTO tbl]` and Postgres'
        // `RETURNING ...` into one `output` field. Only the RETURNING form
        // sends rows back to the client; `OUTPUT ... INTO` redirects them into
        // another table and must not be reported as a row-returning statement.
        returning: matches!(&merge.output, Some(ast::OutputClause::Returning { .. })),
    })
}

// ── Source planning ────────────────────────────────────────────────────────

/// Plan the USING <source> clause.
///
/// Supports:
/// - Table name: `USING src_table ON ...`
/// - Derived subquery: `USING (SELECT ...) AS alias ON ...`
/// - VALUES constructor: treated as a subquery alias.
fn plan_merge_source(factor: &ast::TableFactor, catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let source_name = normalize_object_name_checked(name)?;
            let source_info = catalog
                .get_collection(DatabaseId::DEFAULT, &source_name)?
                .ok_or_else(|| SqlError::UnknownTable {
                    name: source_name.clone(),
                })?;
            let alias_str = alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?;
            let source_rules = engine_rules::resolve_engine_rules(source_info.engine);
            source_rules.plan_scan(ScanParams {
                collection: source_name,
                alias: alias_str,
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
                window_functions: Vec::new(),
                indexes: Vec::new(),
                temporal: TemporalScope::default(),
                bitemporal: source_info.bitemporal,
            })
        }
        ast::TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
            sample: _,
        } => {
            use crate::functions::registry::FunctionRegistry;
            let alias_name = alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?
                .unwrap_or_else(|| "source".to_string());
            let functions = FunctionRegistry::new();
            let plan = crate::planner::select::plan_query(
                subquery,
                catalog,
                &functions,
                TemporalScope::default(),
            )?;
            // Wrap in an alias scan-like node; for Merge we pass the sub-plan
            // directly. The alias is tracked separately via `source_alias`.
            let _ = alias_name;
            Ok(plan)
        }
        other => Err(SqlError::Unsupported {
            detail: format!(
                "MERGE USING source type not supported: {other}; \
                 use a table name or a subquery"
            ),
        }),
    }
}

/// Determine the alias used to qualify source-column references in WHEN arms.
fn merge_source_alias(factor: &ast::TableFactor, source_plan: &SqlPlan) -> Result<String> {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            if let Some(alias) = alias {
                crate::reserved::check_ast_identifier(&alias.name)
            } else {
                normalize_object_name_checked(name)
            }
        }
        ast::TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
            .transpose()
            .map(|alias| alias.unwrap_or_else(|| "source".to_string())),
        _ => Ok(match source_plan {
            SqlPlan::Scan {
                collection: _,
                alias: Some(alias),
                ..
            } => alias.clone(),
            SqlPlan::Scan { collection, .. } => collection.clone(),
            _ => "source".to_string(),
        }),
    }
}

// ── ON clause parsing ──────────────────────────────────────────────────────

/// Extract a single equi-join predicate of the form `target.col = source.col`
/// from the MERGE ON expression.  Returns `(target_col, source_col)`.
fn extract_merge_equijoin(
    on: &ast::Expr,
    target_ref: &str,
    source_ref: &str,
) -> Result<(String, String)> {
    if let ast::Expr::BinaryOp {
        left,
        op: ast::BinaryOperator::Eq,
        right,
    } = on
    {
        let lhs = qualified_ident_pair(left);
        let rhs = qualified_ident_pair(right);
        match (lhs, rhs) {
            (Some((lt, lc)), Some((rt, rc))) => {
                if lt == target_ref && rt == source_ref {
                    return Ok((lc, rc));
                }
                if lt == source_ref && rt == target_ref {
                    return Ok((rc, lc));
                }
            }
            // Unqualified bare-column references: assume target.col = source.col
            // pattern when one side is unqualified.
            (Some((t, c)), None) if t == source_ref => {
                if let ast::Expr::Identifier(ident) = right.as_ref() {
                    return Ok((normalize_ident(ident), c));
                }
            }
            (None, Some((t, c))) if t == source_ref => {
                if let ast::Expr::Identifier(ident) = left.as_ref() {
                    return Ok((normalize_ident(ident), c));
                }
            }
            _ => {}
        }
    }
    Err(SqlError::Unsupported {
        detail: format!(
            "MERGE ON clause must be a single equi-join predicate of the form \
             `{target_ref}.col = {source_ref}.col`; complex ON expressions are not \
             yet supported"
        ),
    })
}

// ── WHEN clause conversion ─────────────────────────────────────────────────

fn convert_merge_clauses(
    clauses: &[ast::MergeClause],
    target_ref: &str,
    source_ref: &str,
) -> Result<Vec<MergePlanClause>> {
    clauses
        .iter()
        .map(|c| convert_one_clause(c, target_ref, source_ref))
        .collect()
}

fn convert_one_clause(
    clause: &ast::MergeClause,
    target_ref: &str,
    source_ref: &str,
) -> Result<MergePlanClause> {
    let kind = match clause.clause_kind {
        AstMergeClauseKind::Matched => MergeClauseKind::Matched,
        AstMergeClauseKind::NotMatched | AstMergeClauseKind::NotMatchedByTarget => {
            MergeClauseKind::NotMatched
        }
        AstMergeClauseKind::NotMatchedBySource => MergeClauseKind::NotMatchedBySource,
    };

    let extra_predicate = match &clause.predicate {
        Some(expr) => strip_and_convert_filters(vec![expr.clone()], target_ref)?,
        None => Vec::new(),
    };

    let action = convert_merge_action(&clause.action, source_ref)?;

    Ok(MergePlanClause {
        kind,
        extra_predicate,
        action,
    })
}

fn convert_merge_action(action: &MergeAction, source_ref: &str) -> Result<MergePlanAction> {
    match action {
        MergeAction::Update(update_expr) => {
            let assignments = update_expr
                .assignments
                .iter()
                .map(|a| {
                    let col = match &a.target {
                        ast::AssignmentTarget::ColumnName(name) => {
                            normalize_object_name_checked(name)
                        }
                        ast::AssignmentTarget::Tuple(_) => Err(SqlError::Unsupported {
                            detail: "tuple assignment target in MERGE UPDATE is not supported"
                                .into(),
                        }),
                    }?;
                    let val = convert_expr(&a.value)?;
                    Ok((col, val))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(MergePlanAction::Update { assignments })
        }
        MergeAction::Delete { .. } => Ok(MergePlanAction::Delete),
        MergeAction::Insert(insert_expr) => {
            let columns: Vec<String> = insert_expr
                .columns
                .iter()
                .map(normalize_object_name_checked)
                .collect::<Result<Vec<_>>>()?;

            let values: Vec<crate::types_expr::SqlExpr> = match &insert_expr.kind {
                MergeInsertKind::Values(vals) => {
                    if vals.rows.len() != 1 {
                        return Err(SqlError::Unsupported {
                            detail: format!(
                                "MERGE INSERT VALUES must have exactly one row; got {}",
                                vals.rows.len()
                            ),
                        });
                    }
                    vals.rows[0]
                        .iter()
                        .map(convert_expr)
                        .collect::<Result<Vec<_>>>()?
                }
                MergeInsertKind::Row => {
                    return Err(SqlError::Unsupported {
                        detail: "MERGE INSERT ROW is not supported; use explicit VALUES".into(),
                    });
                }
            };

            if !columns.is_empty() && columns.len() != values.len() {
                return Err(SqlError::Parse {
                    detail: format!(
                        "MERGE INSERT column list ({}) and VALUES ({}) lengths do not match",
                        columns.len(),
                        values.len()
                    ),
                });
            }

            let _ = source_ref; // for future multi-row insert support
            Ok(MergePlanAction::Insert { columns, values })
        }
    }
}

// ── Helpers ────────────────────────────────────────────────────────────────

pub(super) fn extract_table_factor_name_alias(
    factor: &ast::TableFactor,
) -> Result<(String, Option<String>)> {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let table_name = normalize_object_name_checked(name)?;
            let alias_str = alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?;
            Ok((table_name, alias_str))
        }
        other => Err(SqlError::Unsupported {
            detail: format!("MERGE target must be a plain table name, not: {other}"),
        }),
    }
}
