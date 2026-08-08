// SPDX-License-Identifier: BUSL-1.1

//! The two value-merge rules the upsert branches share with the staged
//! in-transaction path, so an `UPSERT INTO` merges identically whether it runs
//! through the autocommit handler or the transaction overlay.

/// Apply `ON CONFLICT DO UPDATE SET` assignments against the existing row.
///
/// Each assignment's RHS is evaluated via `SqlExpr::eval` — identical to
/// the UPDATE handler's path — so arithmetic (`n = n + 1`), functions
/// (`name = UPPER(name)`), `CASE`, and concatenation all work. Literal
/// assignments bypass the evaluator and decode their msgpack directly.
pub(in crate::data::executor) fn apply_on_conflict_updates(
    existing: nodedb_types::Value,
    excluded: &nodedb_types::Value,
    updates: &[(String, nodedb_physical::physical_plan::UpdateValue)],
) -> crate::Result<nodedb_types::Value> {
    let mut obj = match existing {
        nodedb_types::Value::Object(map) => map,
        // If the existing row isn't an object (shouldn't happen for
        // document engines) fall back to the assignments as a blank slate.
        _ => std::collections::HashMap::new(),
    };
    // Snapshot the row before any assignment applies, so all assignments
    // see the pre-update state — matches PostgreSQL semantics. `excluded`
    // is the row proposed for INSERT that triggered the conflict — it
    // resolves `EXCLUDED.col` references inside the RHS expressions.
    let snapshot = nodedb_types::Value::Object(obj.clone());
    for (field, update_val) in updates {
        let new_val: nodedb_types::Value = match update_val {
            nodedb_physical::physical_plan::UpdateValue::Literal(bytes) => {
                match nodedb_types::value_from_msgpack(bytes) {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            }
            // `ON CONFLICT DO UPDATE SET` is write-path-shaped: a
            // division/modulo-by-zero fails the statement instead of
            // silently writing NULL.
            nodedb_physical::physical_plan::UpdateValue::Expr(expr) => {
                expr.eval_with_excluded(&snapshot, excluded)?
            }
        };
        obj.insert(field.clone(), new_val);
    }
    Ok(nodedb_types::Value::Object(obj))
}

/// Merge two `nodedb_types::Value` objects: overlay `new` fields onto `existing`.
///
/// Shared with the in-transaction staging path (`stage_write/stage_upsert.rs`)
/// so a staged `UPSERT INTO` with no `ON CONFLICT DO UPDATE` clause merges
/// identically to the autocommit handler above.
pub(in crate::data::executor) fn merge_values(
    existing: nodedb_types::Value,
    new: nodedb_types::Value,
) -> nodedb_types::Value {
    match (existing, new) {
        (nodedb_types::Value::Object(mut existing_map), nodedb_types::Value::Object(new_map)) => {
            for (k, v) in new_map {
                existing_map.insert(k, v);
            }
            nodedb_types::Value::Object(existing_map)
        }
        // If shapes don't match, new value wins entirely.
        (_, new) => new,
    }
}
