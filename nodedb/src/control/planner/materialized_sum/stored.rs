// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane resolution of the materialized-sum targets a write addresses
//! through its STORED row rather than through anything the plan carries.
//!
//! # The gap this closes
//!
//! A `PointDelete` names a row by key and carries no body. A `PointUpdate`
//! carries field assignments, not a whole row. Neither can say which target the
//! row it names contributes to, so both used to resolve nothing at all — and a
//! plain `UPDATE ... WHERE id = '…'` or `DELETE FROM … WHERE id = '…'` on a
//! collection driving a binding reached the Data-Plane fold with an empty
//! resolution and failed the statement outright.
//!
//! `PointPut` and `Upsert` onto an EXISTING row have the same gap from the other
//! side. They fold as an update of the stored row by the submitted body, so a
//! write that rewrites the join column moves value between TWO targets — and
//! only the target the body names was ever resolved. The one the row is leaving
//! is readable from the stored image and nowhere else.
//!
//! # Where the row comes from
//!
//! [`recon_point_row`] — the same routed plan-time read the predicate-driven
//! shapes use for their reconnaissance scan, narrowed to one row. There is
//! deliberately no second way to read a source row at plan time: two readers are
//! free to disagree about where a collection lives, and a disagreement here is a
//! resolution that silently misses a target.
//!
//! The Data Plane resolves nothing, here or anywhere: the primary-key →
//! surrogate map is catalog state, and a Data-Plane copy of it would be exactly
//! the cross-plane shared state the plane rules forbid.
//!
//! # Both sides of a join-key change
//!
//! [`binding_join_keys`](crate::query::binding_join_keys) is what turns the one
//! stored row into join values, so the pre-image's value and the value the
//! statement's assignment produces are BOTH resolved. Resolving one side only
//! leaves the other target's total wrong by the row's whole value — and it is
//! the same function the Data-Plane leader re-derives its coverage check from,
//! so the two cannot drift.

use std::sync::Arc;

use nodedb_physical::physical_plan::{DocumentOp, MaterializedSumBinding, UpdateValue};
use nodedb_types::Surrogate;

use super::recon::recon_point_row;
use super::resolve::lookup_join_value;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// The one stored row a point-shaped write rewrites or removes.
pub(super) struct StoredRowScope<'a> {
    /// Source collection as it appears on the plan (db-qualified).
    pub collection: &'a str,
    /// The row's user-facing primary key. Carried for the read plan; identity
    /// for storage addressing is `surrogate` and only `surrogate`.
    pub document_id: &'a str,
    /// Catalog-bound identity of the row, which is what actually addresses it.
    pub surrogate: Surrogate,
    /// The statement's assignments, so a write that rewrites the join column
    /// resolves the target it moves the row ONTO as well as the one it moves it
    /// off. Empty for the shapes that carry no assignments — a delete, and a put
    /// whose post-image is the submitted body.
    pub updates: &'a [(String, UpdateValue)],
}

/// The stored row an op rewrites or removes, or `None` for every op that
/// rewrites none. The match is exhaustive so a new `DocumentOp` variant must
/// state which side it is on.
///
/// The four point shapes are all here, including the two that DO carry a body.
/// A `PointPut` or an `Upsert` onto an existing row folds as an update of the
/// stored row by the submitted body, so a write that rewrites the join column
/// debits the target the row is leaving — and that target is named only by the
/// stored image.
///
/// `PointInsert` and `BatchInsert` are deliberately absent: their rows are new
/// by construction (a duplicate primary key fails the statement, and an
/// `if_absent` conflict writes nothing at all), so there is no pre-image to read
/// and the routed read would cost one round trip per insert to learn that.
pub(super) fn stored_row_scope(op: &DocumentOp) -> Option<StoredRowScope<'_>> {
    match op {
        DocumentOp::PointUpdate {
            collection,
            document_id,
            surrogate,
            updates,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: updates.as_slice(),
        }),
        // A delete assigns nothing: the row's pre-image join value is the whole
        // of what it owes.
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: &[],
        }),
        // A put replaces the row wholesale, so its post-image is the submitted
        // body — already resolved from the body — and the stored row is needed
        // only for the value it held before.
        DocumentOp::PointPut {
            collection,
            document_id,
            surrogate,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: &[],
        }),
        // On the conflict branch an upsert's post-image is the stored row with
        // `on_conflict_updates` applied, so those assignments decide the target
        // the row moves ONTO exactly as an update's `SET` does.
        DocumentOp::Upsert {
            collection,
            document_id,
            surrogate,
            on_conflict_updates,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: on_conflict_updates.as_slice(),
        }),
        DocumentOp::PointInsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Add every target the stored row addresses to `resolved`, leaving entries the
/// caller already resolved from a carried body untouched.
///
/// A row that is not there resolves nothing: an upsert that inserts, and an
/// update or delete whose key matches no row, rewrite no stored row and so owe
/// no target anything. That is the ordinary answer, not a failure — the write
/// path reaches the same conclusion about the same absent row.
pub(super) async fn extend_with_stored_row(
    state: &SharedState,
    bindings: &Arc<Vec<MaterializedSumBinding>>,
    scope: &StoredRowScope<'_>,
    resolved: &mut Vec<(String, Surrogate)>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<()> {
    let Some(row) = recon_point_row(
        state,
        tenant_id,
        database_id,
        scope.collection,
        scope.document_id,
        scope.surrogate,
    )
    .await?
    else {
        return Ok(());
    };

    let rows = [row];
    for binding in bindings.iter() {
        for join_value in crate::query::binding_join_keys(binding, scope.updates, &rows)? {
            // One entry per DISTINCT join value across every binding and every
            // source of join values, mirroring the body-driven resolution: a
            // write whose old and new join keys are the same resolves that
            // target once.
            if resolved.iter().any(|(value, _)| *value == join_value) {
                continue;
            }
            let surrogate = lookup_join_value(
                state,
                binding,
                &join_value,
                tenant_id,
                database_id,
                trace_id,
            )
            .await?;
            resolved.push((join_value, surrogate));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROW: Surrogate = Surrogate(77);

    fn literal(value: serde_json::Value) -> UpdateValue {
        UpdateValue::Literal(nodedb_types::json_to_msgpack(&value).expect("encode literal"))
    }

    fn assignments() -> Vec<(String, UpdateValue)> {
        vec![(
            "account_id".to_string(),
            literal(serde_json::json!("acc-2")),
        )]
    }

    fn point_delete() -> DocumentOp {
        DocumentOp::PointDelete {
            collection: "entries".to_string(),
            document_id: "e1".to_string(),
            surrogate: ROW,
            pk_bytes: b"e1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn point_update() -> DocumentOp {
        DocumentOp::PointUpdate {
            collection: "entries".to_string(),
            document_id: "e1".to_string(),
            surrogate: ROW,
            pk_bytes: b"e1".to_vec(),
            updates: assignments(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn point_put() -> DocumentOp {
        DocumentOp::PointPut {
            collection: "entries".to_string(),
            document_id: "e1".to_string(),
            value: Vec::new(),
            surrogate: ROW,
            pk_bytes: b"e1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn upsert() -> DocumentOp {
        DocumentOp::Upsert {
            collection: "entries".to_string(),
            document_id: "e1".to_string(),
            value: Vec::new(),
            on_conflict_updates: assignments(),
            surrogate: ROW,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn point_insert() -> DocumentOp {
        DocumentOp::PointInsert {
            collection: "entries".to_string(),
            document_id: "e1".to_string(),
            value: Vec::new(),
            if_absent: false,
            surrogate: ROW,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        }
    }

    /// Every point-shaped write that can land on an EXISTING row names that row
    /// for the resolution — including the two that carry a body, whose body
    /// names only the target the row moves ONTO.
    #[test]
    fn every_point_shape_names_the_stored_row_it_rewrites() {
        for op in [point_delete(), point_update(), point_put(), upsert()] {
            let scope = stored_row_scope(&op)
                .unwrap_or_else(|| panic!("a point write must name its stored row: {op:?}"));
            assert_eq!(scope.collection, "entries");
            assert_eq!(scope.document_id, "e1");
            assert_eq!(
                scope.surrogate, ROW,
                "the row is addressed by its surrogate, never by the primary-key string"
            );
        }
    }

    /// An insert's row is new by construction, so there is no stored image to
    /// read and no round trip to spend learning that.
    #[test]
    fn an_insert_names_no_stored_row() {
        assert!(stored_row_scope(&point_insert()).is_none());
    }

    /// The assignments travel on the scope, so a write that rewrites the join
    /// column resolves the target it moves the row ONTO as well as the one it
    /// moves it off. An upsert's conflict-branch assignments count for exactly
    /// the same reason.
    #[test]
    fn assignments_that_rewrite_the_join_column_travel_on_the_scope() {
        for op in [point_update(), upsert()] {
            let scope = stored_row_scope(&op).expect("a point write names its stored row");
            assert_eq!(
                scope.updates.len(),
                1,
                "the statement's assignments must reach the resolution: {op:?}"
            );
            assert_eq!(scope.updates[0].0, "account_id");
        }
    }

    /// A delete and a whole-row put assign nothing: the stored row's own join
    /// value is the whole of what each owes.
    #[test]
    fn the_shapes_without_assignments_carry_none() {
        for op in [point_delete(), point_put()] {
            let scope = stored_row_scope(&op).expect("a point write names its stored row");
            assert!(scope.updates.is_empty(), "{op:?}");
        }
    }
}
