// SPDX-License-Identifier: BUSL-1.1

//! DML-target analysis for a procedure body.
//!
//! Each `StoredProcedure` carries a [`ProcedureRoutability`]
//! classification that the dispatcher uses to bias a CALL onto the
//! vShard that owns the single collection a procedure touches (when
//! it touches just one) — eliminating cross-shard round-trips for
//! the common "stored procedure scoped to one table" pattern. A
//! procedure that touches multiple collections, or no DML at all,
//! falls back to the standard cross-shard scatter.
//!
//! The walker is intentionally narrow: it only resolves the
//! statically-visible target of `INSERT INTO`, `UPDATE`, and
//! `DELETE FROM` (recursing through `IF` / loop bodies). DML against
//! a dynamic name, computed via concatenation, or buried inside a
//! WASM call falls through to `MultiCollection` — the safe default.

use crate::control::security::catalog::procedure_types::ProcedureRoutability;

/// Classify a procedure body's vShard affinity.
///
/// Returns:
/// - `SingleCollection(name)` if every DML statement targets the
///   same collection.
/// - `MultiCollection` otherwise — including bodies that parse
///   correctly but contain no static DML target (no affinity to bias
///   toward), and bodies that fail the procedural-SQL parse (fall
///   back conservatively rather than panic).
pub fn extract_routability(body_sql: &str) -> ProcedureRoutability {
    let block = match crate::control::planner::procedural::parse_block(body_sql) {
        Ok(b) => b,
        Err(_) => return ProcedureRoutability::MultiCollection,
    };

    let mut collections = std::collections::HashSet::new();
    collect_dml_targets(&block.statements, &mut collections);

    match collections.len() {
        0 => ProcedureRoutability::MultiCollection, // No DML — no affinity
        1 => {
            if let Some(name) = collections.into_iter().next() {
                ProcedureRoutability::SingleCollection(name)
            } else {
                ProcedureRoutability::MultiCollection
            }
        }
        _ => ProcedureRoutability::MultiCollection,
    }
}

/// Recursively walk statements to find DML target collection names.
fn collect_dml_targets(
    stmts: &[crate::control::planner::procedural::ast::Statement],
    collections: &mut std::collections::HashSet<String>,
) {
    use crate::control::planner::procedural::ast::Statement;

    for stmt in stmts {
        match stmt {
            Statement::Sql { sql } => {
                if let Some(name) = extract_dml_target_collection(sql) {
                    collections.insert(name);
                }
            }
            Statement::If {
                then_block,
                elsif_branches,
                else_block,
                ..
            } => {
                collect_dml_targets(then_block, collections);
                for branch in elsif_branches {
                    collect_dml_targets(&branch.body, collections);
                }
                if let Some(else_stmts) = else_block {
                    collect_dml_targets(else_stmts, collections);
                }
            }
            Statement::Loop { body }
            | Statement::While { body, .. }
            | Statement::For { body, .. } => {
                collect_dml_targets(body, collections);
            }
            _ => {}
        }
    }
}

/// Extract the target collection name from a DML SQL string.
///
/// Handles:
/// - `INSERT INTO <collection> ...`
/// - `UPDATE <collection> SET ...`
/// - `DELETE FROM <collection> ...`
fn extract_dml_target_collection(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();

    if upper.starts_with("INSERT INTO") && tokens.len() >= 3 {
        Some(tokens[2].to_lowercase().trim_matches('(').to_string())
    } else if upper.starts_with("UPDATE") && tokens.len() >= 2 {
        Some(tokens[1].to_lowercase())
    } else if upper.starts_with("DELETE FROM") && tokens.len() >= 3 {
        Some(tokens[2].to_lowercase())
    } else {
        None
    }
}
