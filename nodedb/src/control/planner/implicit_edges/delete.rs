// SPDX-License-Identifier: BUSL-1.1

//! Implicit-edge DELETE lifecycle: append `GraphOp::EdgeDelete` tasks for the
//! implicit edges surfaced by the OLLP pre-execution scan of a predicate
//! `DELETE`.

use nodedb_physical::physical_task::PhysicalTask;

use super::extract::resolve_edge_label;
use super::routed::{EdgeRouteCtx, push_edge_delete};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// Append a `GraphOp::EdgeDelete` task per implicit edge surfaced by the OLLP
/// pre-execution reconnaissance scan of a predicate `DELETE`.
///
/// This is the symmetric counterpart to `append_implicit_edge_tasks`: when a
/// schemaless edge document (`_from`/`_to`) is deleted via a predicate
/// `BulkDelete`, the implicit graph edge auto-created for it on INSERT must be
/// deleted in the SAME Calvin transaction, cross-shard-correctly. Each appended
/// task is built exactly like an explicit `GRAPH DELETE EDGE`: homed on
/// `from_key(_from)` with both endpoints' canonical surrogates resolved, so the
/// downstream classify/Calvin logic dual-homes cross-shard deletes and
/// single-homes same-shard deletes identically to the matching insert.
///
/// # Label default
///
/// The label default is applied HERE via `resolve_edge_label` so the emitted
/// `EdgeDelete` label matches the `EdgePut` label the matching INSERT created
/// (which also defaults `_type`-absent edges to `"edge"`).
pub async fn append_implicit_edge_delete_tasks(
    state: &SharedState,
    out: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    collection: &str,
    edges: &[crate::control::planner::calvin::preexec::ScannedEdge],
) -> crate::Result<()> {
    for edge in edges {
        let label = resolve_edge_label(edge.label.as_deref());
        push_edge_delete(
            EdgeRouteCtx {
                state,
                tenant_id,
                database_id,
                trace_id,
                collection,
                src: &edge.from,
                dst: &edge.to,
            },
            out,
            label,
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_label_default_matches_insert_default() {
        // The delete-side helper substitutes the SAME default label the INSERT
        // side uses when a `ScannedEdge` carries no `_type`, via the shared
        // `resolve_edge_label`. The surrogate-resolution path needs a live
        // `state` and is covered by the cross-node cluster test.
        assert_eq!(resolve_edge_label(None), "edge");
        assert_eq!(resolve_edge_label(Some("ROAD")), "ROAD");
    }
}
