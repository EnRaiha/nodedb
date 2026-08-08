// SPDX-License-Identifier: BUSL-1.1

//! Implicit-edge INSERT lifecycle: scan document-write tasks for `_from`/`_to`
//! documents and append the mirrored `GraphOp::EdgePut` tasks.

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::catalog::mark_collection_edge_bearing;
use super::extract::{extract_edge, weight_properties};
use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Scan the current document-write tasks for `_from` / `_to` documents and
/// append a `GraphOp::EdgePut` task per implicit edge.
///
/// Each appended task is built exactly like an explicit `GRAPH INSERT EDGE`:
/// the edge is homed on `from_key(_from)` with both endpoints' canonical
/// surrogates resolved via the routed surrogate exchange, so the downstream
/// classify/Calvin/single-shard logic dual-homes cross-shard edges and
/// single-homes same-shard edges identically to explicit edges.
///
/// A mirrored edge carries NO row-level-security write gate, and must not: it
/// is derived from a `DocumentOp` write on the same collection, that write was
/// already decided against the collection's write policy before this runs, and
/// a denial there fails the statement before any mirror is derived. Deciding
/// the mirror as well would refuse every governed document insert on the
/// strength of its own edge, whose property object holds a weight and none of
/// the columns a policy names.
pub async fn append_implicit_edge_tasks(
    state: &SharedState,
    tasks: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<()> {
    // Collect a SNAPSHOT of candidate edges first so the immutable scan of
    // `tasks` does not borrow-conflict with the `&mut Vec` we push into below.
    let mut edges = Vec::new();
    for task in tasks.iter() {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection, value, ..
            })
            | PhysicalPlan::Document(DocumentOp::Upsert {
                collection, value, ..
            }) => {
                if let Some(edge) = extract_edge(collection, value) {
                    edges.push(edge);
                }
            }
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection,
                documents,
                ..
            }) => {
                for (_doc_id, value) in documents {
                    if let Some(edge) = extract_edge(collection, value) {
                        edges.push(edge);
                    }
                }
            }
            // Every other plan (other DocumentOp variants, and non-Document
            // plans) carries no implicit edge — intentionally skipped.
            _ => {}
        }
    }

    // Flag each DISTINCT edge-bearing collection exactly once. Only runs when at
    // least one implicit edge was found, so non-edge inserts do zero catalog
    // work. The mark is idempotent and skips the Raft write when already set.
    let mut marked: Vec<&str> = Vec::new();
    for edge in &edges {
        if !marked.contains(&edge.collection.as_str()) {
            marked.push(edge.collection.as_str());
            mark_collection_edge_bearing(state, database_id, tenant_id, &edge.collection).await?;
        }
    }

    for edge in edges {
        let vsrc = VShardId::from_key(edge.src.as_bytes());
        let vdst = VShardId::from_key(edge.dst.as_bytes());

        let src_surrogate = assign_surrogate_routed(
            state,
            vsrc,
            database_id,
            tenant_id,
            &edge.collection,
            edge.src.as_bytes(),
            trace_id,
        )
        .await?;
        let dst_surrogate = assign_surrogate_routed(
            state,
            vdst,
            database_id,
            tenant_id,
            &edge.collection,
            edge.dst.as_bytes(),
            trace_id,
        )
        .await?;

        let properties = match edge.weight {
            Some(w) => weight_properties(w),
            None => Vec::new(),
        };

        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vsrc,
            database_id,
            plan: PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: edge.collection,
                src_id: edge.src,
                label: edge.label,
                dst_id: edge.dst,
                properties,
                src_surrogate,
                dst_surrogate,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(())
}
