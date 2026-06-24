// SPDX-License-Identifier: BUSL-1.1

//! WHERE predicate application and RETURN column projection.

use super::super::ast::*;
use super::core::execute_clause;
use super::expansion::VarLenCaps;
use super::types::{BindingRow, ExecutionState};
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;

/// Apply a WHERE predicate to filter rows.
///
/// `varlen_caps` carries the same per-expansion caps as the outer query so a
/// variable-length sub-pattern (e.g. inside `NOT EXISTS`) truncates at the
/// configured ceiling rather than a hardcoded one.
pub(super) fn apply_predicate(
    rows: &[BindingRow],
    predicate: &WherePredicate,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    _frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    varlen_caps: VarLenCaps,
) -> Result<Vec<BindingRow>, crate::Error> {
    match predicate {
        WherePredicate::Equals {
            binding,
            field,
            value,
        } => {
            if field.is_empty() {
                Ok(rows
                    .iter()
                    .filter(|row| row.get(binding).is_some_and(|v| v == value))
                    .cloned()
                    .collect())
            } else {
                Ok(rows
                    .iter()
                    .filter(|row| {
                        if let Some(node_id) = row.get(binding) {
                            check_property(edge_store, node_id, field, value)
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect())
            }
        }

        WherePredicate::Comparison {
            binding,
            field,
            op,
            value,
        } => {
            if field.is_empty() {
                // Node-identity comparison: `WHERE a <> b` or `WHERE a <> 'literal'`.
                //
                // The parser stores `value` as the raw RHS string — either a binding
                // name (no quotes, e.g. `p3` from `WHERE p1 <> p3`) or a stripped
                // literal (e.g. `alice` from `WHERE p1 <> 'alice'`). We distinguish by
                // attempting to resolve `value` as a binding in the current row. If it
                // resolves, we compare two bound node identities (binding-vs-binding).
                // If it does not resolve, we compare the bound node identity against
                // the literal string (binding-vs-literal).
                //
                // This covers the LSQB `WHERE p1 <> p3` anti-self-join filter as well
                // as identity equality/inequality against a fixed value.
                Ok(rows
                    .iter()
                    .filter(|row| {
                        let lhs = match row.get(binding.as_str()) {
                            Some(v) => v.as_str(),
                            // Binding not yet resolved in this row → keep row
                            // (predicate is unevaluable; don't silently drop).
                            None => return true,
                        };
                        // Resolve RHS: prefer binding lookup, fall back to literal.
                        let rhs: &str = match row.get(value.as_str()) {
                            Some(v) => v.as_str(),
                            None => value.as_str(),
                        };
                        apply_op(op, lhs, rhs)
                    })
                    .cloned()
                    .collect())
            } else {
                // Property comparison: `WHERE a.age > 25`.
                //
                // `check_property` is a stub (sparse engine not yet wired); it
                // always returns `true`. Property predicates will be pushed down
                // when the document store is wired. We preserve the existing
                // behavior rather than silently claiming the predicate is evaluated.
                Ok(rows
                    .iter()
                    .filter(|row| {
                        if let Some(node_id) = row.get(binding.as_str()) {
                            check_property(edge_store, node_id, field, value)
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect())
            }
        }

        WherePredicate::NotExists { sub_pattern } => {
            let mut result = Vec::new();
            // NOT EXISTS sub-patterns run in their own local state: any
            // truncation inside the sub-query would make the anti-join
            // unsound (a truncated "empty" isn't really empty), so we
            // instead propagate truncation to the outer query via the
            // top-level `ExecutionState` that the caller of
            // `apply_predicate` already tracks. Here we keep a throwaway
            // local state and inspect it.
            for row in rows {
                let mut sub_state = ExecutionState::new(None, varlen_caps);
                // NOT EXISTS sub-patterns check structural connectivity
                // against already-bound variables — no anchor enumeration
                // occurs, so the frontier bitmap does not apply here.
                let sub_rows = execute_clause(
                    sub_pattern,
                    csr,
                    std::slice::from_ref(row),
                    &mut sub_state,
                    None,
                )?;
                if sub_state.truncated() {
                    // Sub-pattern hit a cap — treat the outer match as
                    // truncated too. The outer caller of apply_predicate
                    // is responsible for surfacing this, but we have no
                    // handle to it from inside predicate evaluation, so
                    // the safest contract is to conservatively drop the
                    // row: a truncated "did not match" might actually
                    // have matched. Emitting it would be a false-positive.
                    continue;
                }
                if sub_rows.is_empty() {
                    result.push(row.clone());
                }
            }
            Ok(result)
        }
    }
}

/// Apply a `ComparisonOp` to two string-typed node identities.
///
/// For node-identity comparisons (empty `field`), identities are strings so
/// only `Eq` and `Neq` have defined semantics. Ordering operators (`Lt`, `Lte`,
/// `Gt`, `Gte`) are not meaningful for opaque node names; we conservatively
/// keep every row rather than silently drop on an unevaluable predicate.
fn apply_op(op: &ComparisonOp, lhs: &str, rhs: &str) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::Neq => lhs != rhs,
        // Ordering on node identities is undefined — preserve the row.
        ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Gt | ComparisonOp::Gte => true,
    }
}

/// Check if a node has a property with the expected value.
fn check_property(_edge_store: &EdgeStore, _node_id: &str, _field: &str, _expected: &str) -> bool {
    // Property lookups require the sparse engine (document store).
    // For MATCH patterns, primary filtering is structural (edge traversal).
    // Property predicates will be pushed down when sparse engine is wired.
    true
}

/// Project RETURN columns from rows.
pub(super) fn project_columns(rows: &[BindingRow], columns: &[ReturnColumn]) -> Vec<BindingRow> {
    rows.iter()
        .map(|row| {
            let mut projected = BindingRow::new();
            for col in columns {
                let key = col.alias.as_deref().unwrap_or(&col.expr);

                let value = if let Some(dot) = col.expr.find('.') {
                    let binding = &col.expr[..dot];
                    row.get(binding)
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string())
                } else {
                    row.get(&col.expr)
                        .cloned()
                        .unwrap_or_else(|| "NULL".to_string())
                };

                projected.insert(key.to_string(), value);
            }
            projected
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // `apply_op` is the pure filtering kernel for node-identity comparisons.
    // These tests prove that `WHERE a <> b` (Neq) and `WHERE a = b` (Eq) actually
    // filter rather than the old no-op behaviour, without needing real CsrIndex /
    // EdgeStore instances.

    #[test]
    fn neq_filters_equal_values() {
        // lhs == rhs → Neq must return false (row dropped).
        assert!(!apply_op(&ComparisonOp::Neq, "alice", "alice"));
        // lhs != rhs → Neq must return true (row kept).
        assert!(apply_op(&ComparisonOp::Neq, "alice", "bob"));
    }

    #[test]
    fn eq_keeps_only_matching_values() {
        assert!(apply_op(&ComparisonOp::Eq, "alice", "alice"));
        assert!(!apply_op(&ComparisonOp::Eq, "alice", "bob"));
    }

    #[test]
    fn self_comparison_neq_is_always_false() {
        // WHERE p1 <> p1: the same binding resolves to the same value → always false.
        assert!(!apply_op(&ComparisonOp::Neq, "x", "x"));
        assert!(!apply_op(&ComparisonOp::Neq, "carol", "carol"));
    }

    #[test]
    fn ordering_ops_on_node_identities_preserve_row() {
        // Lt/Lte/Gt/Gte on opaque node names are undefined; we conservatively
        // keep the row rather than silently drop.
        for op in &[
            ComparisonOp::Lt,
            ComparisonOp::Lte,
            ComparisonOp::Gt,
            ComparisonOp::Gte,
        ] {
            assert!(apply_op(op, "alice", "bob"), "{op:?} should preserve row");
            assert!(apply_op(op, "alice", "alice"), "{op:?} should preserve row");
        }
    }

    // End-to-end simulation of the binding-row filtering logic that
    // `apply_predicate` runs for empty-field Comparison predicates.
    // We call the inner closure logic directly (no EdgeStore needed) by
    // hand-rolling what `apply_predicate` does for the Comparison + empty-field branch.
    //
    // This proves the RHS resolution strategy: `value` is first looked up as a
    // binding name in the row; if absent, treated as a literal.
    #[test]
    fn rhs_resolved_as_binding_when_present_in_row() {
        use std::collections::HashMap;

        // Simulate WHERE p1 <> p2 over a binding row set.
        let rows: Vec<HashMap<String, String>> = vec![
            [("p1", "alice"), ("p2", "alice")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // same → drop
            [("p1", "alice"), ("p2", "bob")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // differ → keep
            [("p1", "carol"), ("p2", "carol")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // same → drop
        ];

        let binding = "p1";
        let value = "p2"; // RHS is a binding name, not a literal
        let op = ComparisonOp::Neq;

        let result: Vec<_> = rows
            .iter()
            .filter(|row| {
                let lhs = match row.get(binding) {
                    Some(v) => v.as_str(),
                    None => return true,
                };
                let rhs: &str = match row.get(value) {
                    Some(v) => v.as_str(),
                    None => value,
                };
                apply_op(&op, lhs, rhs)
            })
            .collect();

        assert_eq!(
            result.len(),
            1,
            "only the alice→bob row survives WHERE p1 <> p2"
        );
        assert_eq!(result[0]["p1"], "alice");
        assert_eq!(result[0]["p2"], "bob");
    }

    #[test]
    fn rhs_used_as_literal_when_not_a_binding_in_row() {
        use std::collections::HashMap;

        // Simulate WHERE p1 <> 'alice' (literal not present as a binding key).
        let rows: Vec<HashMap<String, String>> = vec![
            [("p1", "alice")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // equals literal → drop
            [("p1", "bob")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // differs → keep
        ];

        let binding = "p1";
        let value = "alice"; // literal (no binding named "alice" in any row)
        let op = ComparisonOp::Neq;

        let result: Vec<_> = rows
            .iter()
            .filter(|row| {
                let lhs = match row.get(binding) {
                    Some(v) => v.as_str(),
                    None => return true,
                };
                let rhs: &str = match row.get(value) {
                    Some(v) => v.as_str(),
                    None => value,
                };
                apply_op(&op, lhs, rhs)
            })
            .collect();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["p1"], "bob");
    }
}
