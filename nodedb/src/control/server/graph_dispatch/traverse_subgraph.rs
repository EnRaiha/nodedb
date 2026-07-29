// SPDX-License-Identifier: BUSL-1.1

//! `cross_core_traverse_subgraph` — BFS that emits a wire-shape subgraph
//! matching what `nodedb-client`'s remote `graph_traverse` parses.
//!
//! Distinct from [`super::bfs::cross_core_bfs_with_options`]: that returns
//! only the visited node-id set (used by tree DDL aggregates that need a
//! flat reachable set). The remote client's `graph_traverse` trait method
//! parses a `{nodes:[{id,depth}], edges:[{from,to,label}]}` JSON object —
//! anything else (a bare array, or a `{visited: [...]}`-shaped object)
//! decodes to an empty `SubGraph`. This dispatcher emits exactly the
//! shape the client decoder expects so a same-session insert is visible
//! to a same-session traverse.
//!
//! The shared per-hop scatter/decode/merge logic lives in
//! [`super::hop::execute_neighbor_hop`]; this dispatcher layers depth
//! tagging and edge recording on top.

use std::collections::{HashMap, HashSet};

use sonic_rs;

use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId};

use super::helpers::ok_response;
use super::hop::{NeighborHopParams, execute_neighbor_hop};

/// Wire-shape JSON node entry. Field names mirror the client decoder in
/// `nodedb-client/src/remote/parse.rs::parse_graph_traverse_json`.
#[derive(serde::Serialize)]
struct WireNode<'a> {
    id: &'a str,
    depth: u8,
}

/// Wire-shape JSON edge entry. Field names mirror the client decoder.
#[derive(serde::Serialize)]
struct WireEdge<'a> {
    from: &'a str,
    to: &'a str,
    label: &'a str,
}

/// Wire-shape JSON envelope. The client decoder calls
/// `parsed.get("nodes")` and `parsed.get("edges")`; a flat array or any
/// other key set decodes to an empty `SubGraph`, which is the visible
/// failure mode the regression test in
/// `nodedb-client-tests/tests/graph_traverse_remote_round_trip.rs`
/// guards against.
#[derive(serde::Serialize)]
struct WireSubGraph<'a> {
    nodes: Vec<WireNode<'a>>,
    edges: Vec<WireEdge<'a>>,
}

/// Parameters for [`cross_core_traverse_subgraph`].
pub struct CrossCoreTraverseSubgraphParams<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// Collection scope, or `None` for a label-only traversal.
    pub collection: Option<String>,
    pub start: String,
    pub edge_label: Option<String>,
    pub direction: Direction,
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
}

/// BFS that returns a `{nodes,edges}` JSON subgraph for `GRAPH TRAVERSE`.
///
/// Each hop expands every frontier node at the node that owns
/// `from_key(node)` via the shared [`execute_neighbor_hop`] helper and
/// records:
///   * each newly-visited node (with its discovery depth), and
///   * each `(src, label, dst)` edge the hop crossed where `src` is in the
///     current frontier — fully attributed for BOTH the local-shard and
///     remote-shard portions of the frontier.
pub async fn cross_core_traverse_subgraph(
    shared: &SharedState,
    params: CrossCoreTraverseSubgraphParams<'_>,
) -> crate::Result<Response> {
    let CrossCoreTraverseSubgraphParams {
        tenant_id,
        collection,
        database_id,
        start,
        edge_label,
        direction,
        max_depth,
        options,
    } = params;
    // Per-node depth: the start node is at depth 0; subsequent nodes
    // are tagged with the hop index that first surfaced them.
    let mut depth_of: HashMap<String, u8> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut node_order: Vec<String> = Vec::new();
    let mut edges: Vec<(String, String, String)> = Vec::new();
    let mut frontier: Vec<String> = vec![start.clone()];

    visited.insert(start.clone());
    depth_of.insert(start.clone(), 0);
    node_order.push(start);

    for hop_idx in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        let hop = execute_neighbor_hop(
            shared,
            tenant_id,
            database_id,
            NeighborHopParams {
                collection: collection.as_deref(),
                frontier: &frontier,
                edge_label: edge_label.as_deref(),
                direction,
                options,
                discovered_so_far: node_order.len(),
            },
        )
        .await?;

        // Edges are fully attributed for BOTH local-shard and remote-shard
        // expansion (each frontier node was expanded at its owner). Always
        // record them — even when the destination is already visited (an
        // A→B→C graph with a back-edge B→A should surface that edge once).
        edges.extend(hop.local_triples);

        // Tag newly-discovered nodes with the current hop's depth and
        // build the next frontier. `hop_idx=0` expands the depth-0
        // start node into depth-1 neighbors.
        let next_depth_tag = (hop_idx + 1).min(u8::MAX as usize) as u8;
        let mut next_frontier: Vec<String> = Vec::new();
        for node in hop.merged_destinations {
            if visited.insert(node.clone()) {
                depth_of.insert(node.clone(), next_depth_tag);
                node_order.push(node.clone());
                next_frontier.push(node);
                if node_order.len() >= options.max_visited {
                    break;
                }
            }
        }

        frontier = next_frontier;

        if node_order.len() >= options.max_visited {
            break;
        }
    }

    let wire_nodes: Vec<WireNode<'_>> = node_order
        .iter()
        .map(|id| WireNode {
            id: id.as_str(),
            depth: *depth_of.get(id).unwrap_or(&0),
        })
        .collect();
    let wire_edges: Vec<WireEdge<'_>> = edges
        .iter()
        .map(|(src, label, dst)| WireEdge {
            from: src.as_str(),
            to: dst.as_str(),
            label: label.as_str(),
        })
        .collect();
    let envelope = WireSubGraph {
        nodes: wire_nodes,
        edges: wire_edges,
    };

    let payload = sonic_rs::to_vec(&envelope).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: e.to_string(),
    })?;

    Ok(ok_response(payload))
}
