// SPDX-License-Identifier: BUSL-1.1

//! Raise the surrogate high-water mark above every surrogate a re-issue binds.
//!
//! A RESTORE binds the surrogates the source cluster issued, and a MOVE TENANT
//! binds the ones its capture carries. The allocator here must never issue one
//! of them again. Before any re-issue binds a carried surrogate, the re-issue
//! proposes `MetadataEntry::SurrogateAlloc` at the highest carried surrogate.
//! Every node applies it in log order: it raises the global watermark, so every
//! later reservation carves above it, and it retires the unissued part of the
//! node's reserved batch at or below it. The re-issue then waits until every
//! node applied the entry. The metadata log replays in full on a restart, so
//! the raise survives it.

use futures::future::join_all;
use nodedb_physical::physical_plan::ClusterEventOp;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::node_snapshot::snapshot_remote;

/// Make sure no node issues a surrogate at or below `highest` from now on.
/// `tenant_id` frames the requests to the other nodes.
pub(super) async fn raise_surrogate_floor(
    state: &SharedState,
    tenant_id: u64,
    highest: u32,
) -> crate::Result<()> {
    if highest == 0 {
        return Ok(());
    }
    let index = crate::control::metadata_proposer::propose_surrogate_hwm(state, highest).await?;
    await_applied_on_every_node(state, tenant_id, index).await
}

/// Wait until every active node other than this one applied the metadata log
/// through `index`. This node applied it before the propose returned.
async fn await_applied_on_every_node(
    state: &SharedState,
    tenant_id: u64,
    index: u64,
) -> crate::Result<()> {
    let peers: Vec<u64> = match state.cluster_topology.as_ref() {
        Some(topology) => topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .active_nodes()
            .iter()
            .map(|node| node.node_id)
            .filter(|node_id| *node_id != state.node_id)
            .collect(),
        None => Vec::new(),
    };
    let plan = PhysicalPlan::ClusterEvent(ClusterEventOp::MetadataApplied { index });
    let answers = join_all(peers.iter().map(|node_id| {
        let plan = &plan;
        async move {
            snapshot_remote(state, *node_id, tenant_id, DatabaseId::DEFAULT, plan)
                .await
                .map_err(|e| Error::Internal {
                    detail: format!(
                        "restore: node {node_id} did not confirm the surrogate floor at \
                         metadata index {index}: {e}"
                    ),
                })
        }
    }))
    .await;
    for answer in answers {
        answer?;
    }
    Ok(())
}
