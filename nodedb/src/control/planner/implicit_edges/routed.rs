// SPDX-License-Identifier: BUSL-1.1

//! Routed edge-task builders shared by the DELETE and UPDATE lifecycle paths.
//!
//! Both endpoints' canonical surrogates are resolved via the routed surrogate
//! exchange and the task is homed on `from_key(src)`, so the downstream
//! classify/Calvin logic dual-homes cross-shard edges and single-homes
//! same-shard edges identically to explicit `GRAPH ... EDGE` statements.

use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::extract::weight_properties;
use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Append a `GraphOp::EdgeDelete` task for `(src, dst, label)`.
///
/// # Surrogate resolution never allocates
///
/// A delete must never *allocate* a surrogate. We reuse `assign_surrogate_routed`
/// (the same call the INSERT side uses) because the implicit-edge invariant
/// guarantees both endpoints are already bound — the matching INSERT assigned
/// them — so the get-or-create path always hits the existing binding and never
/// allocates.
#[allow(clippy::too_many_arguments)]
pub(super) async fn push_edge_delete(
    state: &SharedState,
    out: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    collection: &str,
    src: &str,
    dst: &str,
    label: String,
) -> crate::Result<()> {
    let vsrc = VShardId::from_key(src.as_bytes());
    let vdst = VShardId::from_key(dst.as_bytes());

    let src_surrogate = assign_surrogate_routed(
        state,
        vsrc,
        database_id,
        tenant_id,
        collection,
        src.as_bytes(),
        trace_id,
    )
    .await?;
    let dst_surrogate = assign_surrogate_routed(
        state,
        vdst,
        database_id,
        tenant_id,
        collection,
        dst.as_bytes(),
        trace_id,
    )
    .await?;

    out.push(PhysicalTask {
        tenant_id,
        vshard_id: vsrc,
        database_id,
        plan: PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: collection.to_string(),
            src_id: src.to_string(),
            label,
            dst_id: dst.to_string(),
            src_surrogate,
            dst_surrogate,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    });
    Ok(())
}

/// Append a `GraphOp::EdgePut` task for `(src, dst, label)` carrying `weight`.
///
/// Mirrors the INSERT-side EdgePut construction. The edge `properties` are built
/// from `weight` via the SAME [`weight_properties`] helper the INSERT path uses,
/// so INSERT and UPDATE produce byte-identical properties for equal weight
/// (`None` → empty properties → CSR unit weight). Endpoint surrogates are
/// resolved get-or-create (a new endpoint may not exist yet), homed on
/// `from_key(src)`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn push_edge_put(
    state: &SharedState,
    out: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    collection: &str,
    src: &str,
    dst: &str,
    label: String,
    weight: Option<f64>,
) -> crate::Result<()> {
    let properties = match weight {
        Some(w) => weight_properties(w),
        None => Vec::new(),
    };

    let vsrc = VShardId::from_key(src.as_bytes());
    let vdst = VShardId::from_key(dst.as_bytes());

    let src_surrogate = assign_surrogate_routed(
        state,
        vsrc,
        database_id,
        tenant_id,
        collection,
        src.as_bytes(),
        trace_id,
    )
    .await?;
    let dst_surrogate = assign_surrogate_routed(
        state,
        vdst,
        database_id,
        tenant_id,
        collection,
        dst.as_bytes(),
        trace_id,
    )
    .await?;

    out.push(PhysicalTask {
        tenant_id,
        vshard_id: vsrc,
        database_id,
        plan: PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: collection.to_string(),
            src_id: src.to_string(),
            label,
            dst_id: dst.to_string(),
            properties,
            src_surrogate,
            dst_surrogate,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    });
    Ok(())
}
