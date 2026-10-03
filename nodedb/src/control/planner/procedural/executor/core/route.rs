// SPDX-License-Identifier: BUSL-1.1

//! Placement of one planned trigger statement: this node, or one remote node.

use nodedb_physical::physical_task::PhysicalTask;

use super::StatementExecutor;
use crate::control::gateway::RouteDecision;
use crate::control::gateway::router::resolve_decision;

/// Where one statement's tasks execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StatementRoute {
    /// Every task is led by this node. It joins the body's local transaction.
    Local,
    /// Every task writes `vshard_id`, led by `node_id`. The statement is
    /// re-planned there, with every write it derives.
    Remote { node_id: u64, vshard_id: u32 },
}

impl StatementRoute {
    /// A local statement can span local vShards. A remote one names exactly
    /// one vShard: a request addresses one vShard, and its dedup key rides
    /// that vShard's redo record.
    fn same_target(self, other: Self) -> bool {
        match (self, other) {
            (Self::Local, Self::Local) => true,
            (Self::Remote { .. }, Self::Remote { .. }) => self == other,
            _ => false,
        }
    }
}

impl StatementExecutor<'_> {
    /// Resolve where `tasks` execute. Only a body carrying a cross-shard
    /// origin routes remotely; every other body runs all of its tasks here.
    ///
    /// A statement split across nodes, or across remote vShards, is refused.
    /// It is shipped as SQL, so sending it whole to one node will write the
    /// other node's rows there, and a request addresses one vShard.
    pub(super) fn statement_route(&self, tasks: &[PhysicalTask]) -> crate::Result<StatementRoute> {
        if self.cross_shard_origin.is_none() {
            return Ok(StatementRoute::Local);
        }
        let Some(routing) = self.state.cluster_routing.as_ref() else {
            return Ok(StatementRoute::Local);
        };
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());

        let mut route: Option<StatementRoute> = None;
        for task in tasks {
            let vshard_id = task.vshard_id.as_u32();
            let next = match resolve_decision(vshard_id, self.state.node_id, Some(&*routing), None)
            {
                RouteDecision::Local => StatementRoute::Local,
                RouteDecision::Remote { node_id, .. } => {
                    StatementRoute::Remote { node_id, vshard_id }
                }
                RouteDecision::LeaderUnknown { .. } => {
                    return Err(crate::Error::NotLeader {
                        vshard_id: task.vshard_id,
                        leader_node: 0,
                        leader_addr: String::new(),
                        leader_term: 0,
                    });
                }
                RouteDecision::Broadcast { .. } => {
                    return Err(crate::Error::Internal {
                        detail: "cross-shard trigger: resolve_decision returned Broadcast \
                                 for a single vShard"
                            .into(),
                    });
                }
            };
            route = match route {
                None => Some(next),
                Some(prev) if prev.same_target(next) => Some(prev),
                Some(_) => {
                    return Err(crate::Error::BadRequest {
                        detail: "a trigger statement writes vShards led by different nodes, \
                                 or several vShards on another node; write each collection \
                                 in its own statement"
                            .into(),
                    });
                }
            };
        }
        Ok(route.unwrap_or(StatementRoute::Local))
    }
}

#[cfg(test)]
mod tests {
    use super::StatementRoute;

    #[test]
    fn a_remote_placement_names_one_vshard() {
        let a = StatementRoute::Remote {
            node_id: 2,
            vshard_id: 1,
        };
        let b = StatementRoute::Remote {
            node_id: 2,
            vshard_id: 5,
        };
        let c = StatementRoute::Remote {
            node_id: 3,
            vshard_id: 1,
        };
        assert!(a.same_target(a));
        assert!(
            !a.same_target(b),
            "two vShards on one node are two requests"
        );
        assert!(!a.same_target(c));
        assert!(!a.same_target(StatementRoute::Local));
        assert!(StatementRoute::Local.same_target(StatementRoute::Local));
    }
}
