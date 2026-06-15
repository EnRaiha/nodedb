// SPDX-License-Identifier: BUSL-1.1

//! Physical plan → `Vec<TaskRoute>` routing.
//!
//! The router consults the local [`RoutingTable`] to decide whether each
//! task runs locally or must be forwarded to a remote node.
//!
//! # Routing rules
//!
//! 1. Compute the vShard for the plan's primary collection via
//!    [`vshard_for_collection`].
//! 2. Look up the Raft group leader for that vShard in the routing table.
//! 3. If the leader is this node (`local_node_id`) → `RouteDecision::Local`.
//! 4. If the leader is another node → `RouteDecision::Remote`.
//! 5. For plans wrapped in `QueryOp::Exchange{Gather{..}}` →
//!    `RouteDecision::Broadcast` listing every vShard in the routing table.
//!    The Exchange node itself is resolved by the coordinator; the child plan
//!    is routed to all vShards.
//!
//! In single-node mode (routing table = `None`), all plans route locally.

use nodedb_cluster::routing::{RoutingTable, vshard_for_collection};
use nodedb_types::id::DatabaseId;

use nodedb_physical::physical_plan::PhysicalPlan;

use super::route::{RouteDecision, TaskRoute};
use super::version_set::touched_collections;

/// Compute routing decisions for a single `PhysicalPlan`.
///
/// Returns a `Vec<TaskRoute>` — usually one element; multiple elements only
/// for broadcast scans (one route per vShard).
///
/// `database_id` scopes the routing hash so that the same collection name in
/// two different databases resolves to independent vShards.
pub fn route_plan(
    plan: PhysicalPlan,
    local_node_id: u64,
    routing: Option<&RoutingTable>,
    database_id: DatabaseId,
) -> Vec<TaskRoute> {
    // In single-node mode every plan runs locally.
    let Some(routing) = routing else {
        let vshard_id = primary_vshard(&plan, database_id);
        return vec![TaskRoute {
            plan,
            decision: RouteDecision::Local,
            vshard_id,
        }];
    };

    // A sharded read/aggregate reaches the router wrapped in `Exchange{Gather}`.
    // The coordinator strips the Exchange here: its child is the plan that runs
    // on each vShard, and the per-vShard payloads are fused on return (see
    // `fuse_payloads` in the gateway core). Shipping the Exchange wrapper itself
    // would let it reach a Data-Plane core, which rejects unresolved Exchange
    // nodes ("Exchange must be resolved by the coordinator before dispatch").
    use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, QueryOp};
    match plan {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { .. },
        })) => route_broadcast(*child, local_node_id, routing),
        other => {
            let vshard_id = primary_vshard(&other, database_id);
            let decision = resolve_decision(vshard_id, local_node_id, Some(routing), None);
            vec![TaskRoute {
                plan: other,
                decision,
                vshard_id,
            }]
        }
    }
}

/// Resolve the `RouteDecision` for a single vShard.
///
/// The routing table is a *cached hint*. The authoritative source of
/// truth is the live Raft group status. When `live_leader_for_group` is
/// provided, it overrides the routing table's leader hint for the
/// vShard's group — the routing table can be stale (especially with
/// "leader is me" pointing at a former leader), while live Raft state
/// always reflects the current term's actual leader on this node's view.
///
/// Decision rules (cluster mode):
/// 1. If live Raft says this node is leader for the group → `Local`.
/// 2. If live Raft names a *different* leader → `Remote { that node }`.
/// 3. If neither live Raft nor the routing table know a leader →
///    `LeaderUnknown` (surfaced as `Error::NotLeader` by dispatch so the
///    gateway retry loop sleeps and re-resolves).
///
/// Single-node mode (`routing == None`) always routes locally.
pub fn resolve_decision(
    vshard_id: u32,
    local_node_id: u64,
    routing: Option<&RoutingTable>,
    live_leader_for_group: Option<&dyn Fn(u64) -> u64>,
) -> RouteDecision {
    let Some(routing) = routing else {
        return RouteDecision::Local;
    };
    let unknown = RouteDecision::LeaderUnknown {
        vshard_id: vshard_id as u64,
    };

    // Prefer live Raft state over the routing-table hint when available.
    if let Some(live) = live_leader_for_group
        && let Ok(group_id) = routing.group_for_vshard(vshard_id)
    {
        let live_leader = live(group_id);
        if live_leader == local_node_id {
            return RouteDecision::Local;
        }
        if live_leader != 0 {
            return RouteDecision::Remote {
                node_id: live_leader,
                vshard_id: vshard_id as u64,
            };
        }
        // Live state has no leader for this group yet — fall through to
        // routing-table hint (it may have a stale-but-usable forwarding
        // target from the last term).
    }

    match routing.leader_for_vshard(vshard_id) {
        Ok(0) => unknown,
        Ok(leader) if leader == local_node_id => RouteDecision::Local,
        Ok(leader) => RouteDecision::Remote {
            node_id: leader,
            vshard_id: vshard_id as u64,
        },
        Err(_) => unknown,
    }
}

/// Build one route per vShard for broadcast-scan plans.
///
/// Returns a mix of `Local` (this node's vShards) and `Remote` routes.
fn route_broadcast(
    plan: PhysicalPlan,
    local_node_id: u64,
    routing: &RoutingTable,
) -> Vec<TaskRoute> {
    use nodedb_cluster::routing::VSHARD_COUNT;

    let mut routes = Vec::with_capacity(VSHARD_COUNT as usize);
    for vshard_id in 0u32..VSHARD_COUNT {
        let decision = resolve_decision(vshard_id, local_node_id, Some(routing), None);
        routes.push(TaskRoute {
            plan: plan.clone(),
            decision,
            vshard_id,
        });
    }
    routes
}

/// Determine the primary vShard for a plan by hashing the first collection name.
///
/// Falls back to vShard 0 for plans that have no named collection (Meta ops).
fn primary_vshard(plan: &PhysicalPlan, database_id: DatabaseId) -> u32 {
    touched_collections(plan)
        .into_iter()
        .next()
        .map(|name| vshard_for_collection(database_id, &name))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

    fn single_node_table() -> RoutingTable {
        RoutingTable::uniform(1, &[1], 1)
    }

    fn two_node_table() -> RoutingTable {
        // Group 0 → leader=1, Group 1 → leader=2.
        // vShards distributed 50/50 across groups.
        RoutingTable::uniform(2, &[1, 2], 1)
    }

    #[test]
    fn single_node_routes_locally() {
        let table = single_node_table();
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "users".into(),
            key: vec![],
            rls_filters: vec![],
            surrogate_ceiling: None,
        });
        let routes = route_plan(plan, 1, Some(&table), DatabaseId::DEFAULT);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].decision, RouteDecision::Local);
    }

    #[test]
    fn no_routing_table_routes_locally() {
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: "x".into(),
            key: vec![],
            value: vec![],
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        });
        let routes = route_plan(plan, 99, None, DatabaseId::DEFAULT);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].decision, RouteDecision::Local);
    }

    #[test]
    fn remote_route_when_different_leader() {
        let mut table = two_node_table();
        // Force vShard 0 leader to node 2; we are node 1.
        let group = table.group_for_vshard(0).unwrap();
        table.set_leader(group, 2);

        // Use a collection that hashes to vShard 0.
        // Find one by brute force.
        let collection = find_collection_for_vshard(0);
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection,
            key: vec![],
            rls_filters: vec![],
            surrogate_ceiling: None,
        });
        let routes = route_plan(plan, 1, Some(&table), DatabaseId::DEFAULT);
        assert_eq!(routes.len(), 1);
        match &routes[0].decision {
            RouteDecision::Remote { node_id, .. } => assert_eq!(*node_id, 2),
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[test]
    fn broadcast_scan_produces_multiple_routes() {
        let table = two_node_table();
        let scan = PhysicalPlan::Document(DocumentOp::Scan {
            collection: "events".into(),
            limit: 100,
            offset: 0,
            sort_keys: vec![],
            filters: vec![],
            distinct: false,
            projection: vec![],
            computed_columns: vec![],
            window_functions: vec![],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        });
        // A sharded read reaches the router wrapped in Exchange{Gather} (the
        // shape `convert()` produces); routing fans it to every vShard.
        let plan = PhysicalPlan::Query(nodedb_physical::physical_plan::QueryOp::Exchange(
            nodedb_physical::physical_plan::ExchangeOp {
                child: Box::new(scan),
                mode: nodedb_physical::physical_plan::ExchangeMode::Gather {
                    as_aggregate: false,
                },
            },
        ));
        let routes = route_plan(plan, 1, Some(&table), DatabaseId::DEFAULT);
        // Broadcast should produce VSHARD_COUNT routes.
        assert_eq!(routes.len(), nodedb_cluster::routing::VSHARD_COUNT as usize);
        // Each route must carry the UNWRAPPED child plan, not the Exchange
        // wrapper. A wrapper shipped to a vShard reaches a Data-Plane core,
        // which rejects unresolved Exchange nodes.
        for route in &routes {
            assert!(
                matches!(route.plan, PhysicalPlan::Document(DocumentOp::Scan { .. })),
                "broadcast route must carry the unwrapped scan child, got {:?}",
                route.plan
            );
        }
    }

    /// Find a collection name that hashes to the given vShard.
    fn find_collection_for_vshard(target: u32) -> String {
        for i in 0u64.. {
            let name = format!("col_{i}");
            if vshard_for_collection(DatabaseId::DEFAULT, &name) == target {
                return name;
            }
        }
        unreachable!()
    }
}
