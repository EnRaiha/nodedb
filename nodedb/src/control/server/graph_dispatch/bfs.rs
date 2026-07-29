// SPDX-License-Identifier: BUSL-1.1

//! `cross_core_bfs` — multi-hop BFS that drives the tree DDL aggregates
//! (`TREE_SUM`, `TREE_CHILDREN`) and any other breadth-first walk that
//! needs a flat reachable-node set across the full cross-core /
//! cross-shard neighborhood of each frontier node.
//!
//! The shared per-hop scatter/decode/merge logic lives in
//! [`super::hop::execute_neighbor_hop`]; this dispatcher only retains
//! the merged destination set. `GRAPH TRAVERSE`, which needs the
//! `{nodes,edges}` subgraph shape the remote client decodes, lives in
//! [`super::traverse_subgraph::cross_core_traverse_subgraph`].

use std::collections::HashSet;

use crate::bridge::envelope::Response;
use crate::control::state::SharedState;
use crate::engine::graph::traversal_options::GraphTraversalOptions;
use crate::types::{DatabaseId, TenantId};

use super::helpers::{encode_path, ok_response};
use super::hop::{NeighborHopParams, execute_neighbor_hop};

/// Parameters for [`cross_core_bfs_with_options`].
pub struct CrossCoreBfsParams<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// Collection scope, or `None` for a label-only traversal.
    pub collection: Option<&'a str>,
    pub start_nodes: Vec<String>,
    pub edge_label: Option<String>,
    pub direction: crate::engine::graph::edge_store::Direction,
    pub max_depth: usize,
    pub options: &'a GraphTraversalOptions,
}

/// Cross-core BFS with explicit traversal options (fan-out limits, partial mode).
///
/// This is the cluster-aware entry point. Callers pass
/// `&GraphTraversalOptions::default()` for standard traversal.
pub async fn cross_core_bfs_with_options(
    shared: &SharedState,
    params: CrossCoreBfsParams<'_>,
) -> crate::Result<Response> {
    let CrossCoreBfsParams {
        tenant_id,
        database_id,
        collection,
        start_nodes,
        edge_label,
        direction,
        max_depth,
        options,
    } = params;
    let mut visited: HashSet<String> = HashSet::new();
    let mut all_discovered: Vec<String> = Vec::new();
    let mut frontier: Vec<String> = start_nodes;

    for node in &frontier {
        visited.insert(node.clone());
        all_discovered.push(node.clone());
    }

    for _depth in 0..max_depth {
        if frontier.is_empty() {
            break;
        }

        let hop = execute_neighbor_hop(
            shared,
            tenant_id,
            database_id,
            NeighborHopParams {
                collection,
                frontier: &frontier,
                edge_label: edge_label.as_deref(),
                direction,
                options,
                discovered_so_far: all_discovered.len(),
            },
        )
        .await?;

        // Extend global visited set and compute next frontier.
        let mut next_frontier: Vec<String> = Vec::new();
        for node in hop.merged_destinations {
            if visited.insert(node.clone()) {
                next_frontier.push(node.clone());
                all_discovered.push(node);
                if all_discovered.len() >= options.max_visited {
                    break;
                }
            }
        }

        frontier = next_frontier;

        if all_discovered.len() >= options.max_visited {
            break;
        }
    }

    Ok(ok_response(encode_path(&all_discovered)?))
}
