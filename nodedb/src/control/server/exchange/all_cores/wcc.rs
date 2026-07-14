// SPDX-License-Identifier: BUSL-1.1

//! WCC contraction-round fan: dispatch across all local cores and merge the
//! per-core `WccSuperstepResult` parts by field concatenation.

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_physical::physical_plan::{PhysicalPlan, WccSuperstepResult};

use super::dispatch::NodeLevelResult;
use super::fanout::gather_graph_op_all_cores;

/// WCC contraction-round fan: dispatch to all local cores, decode each core's
/// [`WccSuperstepResult`], merge by field concatenation, and re-encode.
///
/// Owned-node sets are disjoint across cores (each graph node is homed on
/// exactly one core via `VShardId::from_key`), so concatenation requires no
/// dedup. Cross-core edges become ordinary boundary edges (the destination is
/// owned by a sibling core) and are stitched globally by the coordinator.
pub(super) async fn fan_wcc_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    let responses =
        gather_graph_op_all_cores(state, tenant_id, database_id, plan, trace_id, None, "wcc")
            .await?;

    let mut parts: Vec<WccSuperstepResult> = Vec::with_capacity(responses.len());
    for resp in responses {
        // An empty payload decodes to WccSuperstepResult::default() (a
        // zero-vertex shard — contributes no labels or boundary edges).
        let part = if resp.payload.is_empty() {
            WccSuperstepResult::default()
        } else {
            zerompk::from_msgpack::<WccSuperstepResult>(resp.payload.as_ref()).map_err(|e| {
                crate::Error::Codec {
                    detail: format!("wcc gather: result decode: {e}"),
                }
            })?
        };
        parts.push(part);
    }

    let merged = merge_wcc_results(parts);
    let payload = zerompk::to_msgpack_vec(&merged).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wcc gather: merged result encode: {e}"),
    })?;

    Ok(NodeLevelResult {
        payload,
        watermark_lsn: Lsn::ZERO,
        read_version_lsn: Lsn::ZERO,
    })
}

/// Merge per-core [`WccSuperstepResult`] parts by field concatenation.
///
/// Owned-node sets are DISJOINT across cores because `gather_graph_op_all_cores`
/// scopes each core's `owned_vshards` to the vShards homed on that core, so each
/// graph node is owned by exactly one core. Concatenation therefore requires no
/// dedup; cross-core edges already appear as boundary edges (their destination
/// is owned by a sibling core) and are stitched globally by the coordinator.
fn merge_wcc_results(parts: Vec<WccSuperstepResult>) -> WccSuperstepResult {
    let mut out = WccSuperstepResult::default();
    for p in parts {
        out.vertex_count += p.vertex_count;
        out.node_labels.extend(p.node_labels);
        out.boundary_edges.extend(p.boundary_edges);
    }
    out
}
