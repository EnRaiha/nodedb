// SPDX-License-Identifier: BUSL-1.1

//! Shard enumeration for distributed BSP PageRank.
//!
//! A "shard" here is one **owner NODE** (the local node + each distinct
//! non-local data-group leader), NOT one vShard. A remote `ExecuteRequest` runs
//! on a single core whose per-core `EdgeStore` holds that node's full graph
//! slice (one core / node — the single-core-coverage property the MATCH scatter
//! relies on; see the caveat below), and the receiver always dispatches with
//! `vshard_id = 0` rather than routing to a per-vShard core. Enumerating one
//! dispatch per *vShard* therefore lands hundreds of dispatches on the SAME core
//! of the SAME node, each rebuilding that node's full CSR — a massive waste where
//! most per-vShard dispatches own zero nodes. Enumerating one dispatch per
//! *node*, carrying that node's FULL set of owned vShards, rebuilds each node's
//! CSR exactly once per superstep and ranks every node it owns in one pass.
//!
//! This mirrors `match_scatter::round_zero::distinct_remote_owners`'s
//! one-dispatch-per-distinct-owner-node enumeration (live Raft leadership via the
//! `raft_status_fn` snapshot, falling back to the routing-table hint). The
//! metadata group (0) owns no vShards and is skipped. A data group with no
//! resolvable leader is a hard `NotLeader` error — never a silently-dropped
//! shard (same contract as the MATCH scatter).
//!
//! **Multi-core caveat (pre-existing, shared with MATCH — out of scope here):** a
//! remote `ExecuteRequest` executes on core 0 only, and the per-core `EdgeStore`
//! means core 0 holds a node's FULL graph slice only when that node runs a single
//! Data-Plane core (as the cluster tests do). On a multi-core node, cross-node
//! reads (BOTH this per-node BSP enumeration AND the existing cross-shard MATCH
//! scatter) would observe only core-0's partition. This per-node enumeration
//! assumes the same single-core-coverage property the MATCH scatter already
//! relies on; a multi-core cross-node fan-out is a separate concern.

use std::collections::HashMap;

use crate::control::state::SharedState;
use crate::types::VShardId;

/// One BSP shard: a distinct owner node, the FULL set of vShards it owns (the
/// union of every data group it leads), and whether it is the coordinating node
/// (dispatch local vs. remote).
pub(in crate::control::server::graph_dispatch) struct ShardTarget {
    /// Owning node (resolved via live Raft leadership). Used as the stable
    /// per-shard key for the coordinator's rank-state map and as the remote
    /// dispatch target.
    pub(in crate::control::server::graph_dispatch) node_id: u64,
    /// `true` if this node is the coordinating node (dispatch local).
    pub(in crate::control::server::graph_dispatch) is_local: bool,
    /// The FULL set of vShards this node owns this superstep (union of
    /// `vshards_for_group` over every data group whose resolved leader is this
    /// node). Passed verbatim as the plan's `owned_vshards` so the handler ranks
    /// EVERY node homed on this owner in one CSR pass.
    pub(in crate::control::server::graph_dispatch) owned_vshards: Vec<u32>,
}

impl ShardTarget {
    /// One vShard from this node's owned set, used as the `vshard_id` field of a
    /// remote `RouteDecision::Remote` route (any one of the node's vShards picks
    /// the same node). Owned sets are never empty (a node is only a shard target
    /// if it leads at least one data group with at least one vShard).
    pub(in crate::control::server::graph_dispatch) fn route_vshard(&self) -> u32 {
        self.owned_vshards.first().copied().unwrap_or(0)
    }
}

/// Result of per-node enumeration: the shard targets plus a `vShard → owner
/// node` map used to redistribute cross-shard contributions to the owning node.
pub(in crate::control::server::graph_dispatch) struct Enumeration {
    pub(in crate::control::server::graph_dispatch) targets: Vec<ShardTarget>,
    /// Every owned vShard → its owner node id. Used by the coordinator to route
    /// each outbound `(target_vshard, …)` contribution to the node-shard that
    /// owns `target_vshard`.
    pub(in crate::control::server::graph_dispatch) vshard_owner: HashMap<u32, u64>,
}

/// Enumerate all BSP shards: one per distinct owner node (local + each distinct
/// non-local data-group leader), each carrying the FULL set of vShards that node
/// owns. Also returns the `vShard → owner node` map for contribution routing.
///
/// Returns an empty enumeration in single-node mode (`cluster_routing.is_none()`).
pub(in crate::control::server::graph_dispatch) fn enumerate_shards(
    state: &SharedState,
) -> crate::Result<Enumeration> {
    let Some(routing_lock) = state.cluster_routing.as_ref() else {
        return Ok(Enumeration {
            targets: Vec::new(),
            vshard_owner: HashMap::new(),
        });
    };
    let routing = routing_lock.read().unwrap_or_else(|p| p.into_inner());

    let raft_snapshot: Vec<nodedb_cluster::GroupStatus> =
        state.raft_status_fn.get().map(|f| f()).unwrap_or_default();
    let live_leader = |group_id: u64| -> u64 {
        raft_snapshot
            .iter()
            .find(|gs| gs.group_id == group_id)
            .map(|gs| gs.leader_id)
            .unwrap_or(0)
    };

    // Accumulate each owner node's full vShard set (union over the data groups it
    // leads), preserving first-seen node order, and build the vShard → owner map.
    let mut owned_by_node: HashMap<u64, Vec<u32>> = HashMap::new();
    let mut node_order: Vec<u64> = Vec::new();
    let mut vshard_owner: HashMap<u32, u64> = HashMap::new();

    for group_id in routing.group_ids() {
        // Skip the metadata group — it owns no vShards.
        if group_id == 0 {
            continue;
        }
        let vshards = routing.vshards_for_group(group_id);
        if vshards.is_empty() {
            continue;
        }
        // Prefer live Raft leadership; fall back to the routing-table hint.
        let mut leader = live_leader(group_id);
        if leader == 0 {
            leader = routing.group_info(group_id).map(|g| g.leader).unwrap_or(0);
        }
        if leader == 0 {
            // No known leader for this group: fail hard so a leader election
            // surfaces as an explicit error rather than silently omitting every
            // vShard in this group from the global PageRank computation.
            let first = vshards.first().copied().unwrap_or(0);
            return Err(crate::Error::NotLeader {
                vshard_id: VShardId::new(first),
                leader_node: 0,
                leader_addr: String::new(),
            });
        }
        if !owned_by_node.contains_key(&leader) {
            node_order.push(leader);
        }
        let entry = owned_by_node.entry(leader).or_default();
        for vs in vshards {
            vshard_owner.insert(vs, leader);
            entry.push(vs);
        }
    }

    let targets = node_order
        .into_iter()
        .map(|node_id| {
            let mut owned_vshards = owned_by_node.remove(&node_id).unwrap_or_default();
            owned_vshards.sort_unstable();
            ShardTarget {
                node_id,
                is_local: node_id == state.node_id,
                owned_vshards,
            }
        })
        .collect();

    Ok(Enumeration {
        targets,
        vshard_owner,
    })
}
