// SPDX-License-Identifier: BUSL-1.1

//! Shared coordinator-side peer/partition helpers for the distributed shuffle
//! resolvers (`shuffle` = shuffle-join, `shuffle_aggregate` = shuffle GROUP BY).
//!
//! Both resolvers fan producer/consumer RPCs across the cluster and need the
//! same primitives: resolve a collection's owner nodes, register peer addresses
//! from the live topology before dispatch, and count the cluster's data nodes
//! for the default partition count. They live here (rather than duplicated in
//! each resolver) so the two paths share one implementation.

use std::collections::BTreeSet;

use nodedb_cluster::{
    METADATA_GROUP_ID, RaftRpc, RoutingTable, ShuffleProduceRequest, ShuffleProduceResponse,
};

use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};

/// Producer nodes that own `collection`'s data: resolve its vShard → owning
/// group → leader. A user collection is single-vShard-homed, so this is one
/// node; returned as a deduped sorted vec for generality.
pub(super) fn producer_nodes(
    routing: &RoutingTable,
    database_id: DatabaseId,
    collection: &str,
) -> crate::Result<Vec<u64>> {
    let vshard = VShardId::from_collection_in_database(database_id, collection).as_u32();
    let group = routing
        .group_for_vshard(vshard)
        .map_err(|e| crate::Error::Internal {
            detail: format!("shuffle: no group for vshard {vshard} ({collection}): {e}"),
        })?;
    let leader = routing
        .group_info(group)
        .map(|g| g.leader)
        .filter(|&l| l != 0)
        .ok_or_else(|| crate::Error::Internal {
            detail: format!("shuffle: no leader for group {group} ({collection})"),
        })?;
    Ok(vec![leader])
}

/// Count distinct data-group leaders (the cluster's data-node count), excluding
/// the metadata group, which owns no vShards.
pub(super) fn distinct_data_node_count(routing: &RoutingTable) -> usize {
    let mut nodes: BTreeSet<u64> = BTreeSet::new();
    for group_id in routing.group_ids() {
        if group_id == METADATA_GROUP_ID {
            continue;
        }
        if let Some(info) = routing.group_info(group_id)
            && info.leader != 0
        {
            nodes.insert(info.leader);
        }
    }
    nodes.len()
}

/// Register each target node's address with the transport from the live cluster
/// topology (idempotent). Makes the shuffle fan-out robust to a peer the
/// transport has not warmed yet — without it `send_rpc` to an unregistered (but
/// topology-known) node fails with `NodeUnreachable`. Self IS registered too:
/// when this coordinator also owns one of the sides it dispatches that
/// producer/consumer to itself via `send_rpc`, which loops back through the local
/// QUIC endpoint and runs the same handler (an extra local hop, functionally
/// correct). Missing topology / address for a node is left alone so the
/// subsequent `send_rpc` surfaces the typed `NodeUnreachable` rather than this
/// silently masking it.
pub(crate) fn register_peers_from_topology(
    state: &SharedState,
    transport: &nodedb_cluster::NexarTransport,
    nodes: &BTreeSet<u64>,
) {
    let Some(topology) = state.cluster_topology.as_ref() else {
        return;
    };
    let topo = topology.read().unwrap_or_else(|p| p.into_inner());
    for &node in nodes {
        if let Some(info) = topo.get_node(node)
            && let Some(addr) = info.socket_addr()
        {
            transport.register_peer(node, addr);
        }
    }
}

/// Send one `ShuffleProduceRequest` and map the reply / RPC error to a typed
/// coordinator error, returning the producer's observed per-collection
/// read-version LSN on a clean produce. Fail-fast: a producer-reported terminal
/// error aborts. Shared by both the shuffle-JOIN and shuffle-AGGREGATE
/// resolvers, which each max-fold the returned LSN over their producers.
pub(super) async fn send_produce(
    transport: &nodedb_cluster::NexarTransport,
    node: u64,
    req: ShuffleProduceRequest,
) -> crate::Result<u64> {
    match transport
        .send_rpc(node, RaftRpc::ShuffleProduceRequest(req))
        .await
    {
        Ok(RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse {
            error: None,
            read_version_lsn,
        })) => Ok(read_version_lsn),
        Ok(RaftRpc::ShuffleProduceResponse(ShuffleProduceResponse { error: Some(e), .. })) => {
            Err(crate::Error::Internal {
                detail: format!("shuffle produce failed on node {node}: {e:?}"),
            })
        }
        Ok(other) => Err(crate::Error::Internal {
            detail: format!("shuffle produce: unexpected reply from node {node}: {other:?}"),
        }),
        Err(e) => Err(crate::Error::Internal {
            detail: format!("shuffle produce RPC to node {node} failed: {e}"),
        }),
    }
}
