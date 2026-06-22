// SPDX-License-Identifier: BUSL-1.1

//! Calvin / OLLP and graph-traverse inspector methods on [`TestClusterNode`].
//!
//! These helpers are used by cluster tests that exercise the Calvin sequencer
//! and implicit-edge graph traversal. Separated from `inspect.rs` to keep both
//! files under the 500-line limit.

use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use super::lifecycle::TestClusterNode;

impl TestClusterNode {
    /// Observed sequencer-group leader id from this node's local Raft status,
    /// or `0` if no leader is known yet.
    ///
    /// Used by Calvin / OLLP cluster tests to gate execution on a stable
    /// sequencer-group leader and to pick a non-leader coordinator.
    pub fn sequencer_leader(&self) -> u64 {
        let Some(status_fn) = self.shared.raft_status_fn.get() else {
            return 0;
        };
        status_fn()
            .into_iter()
            .find(|g| g.group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID)
            .map(|g| g.leader_id)
            .unwrap_or(0)
    }

    /// Run a `GRAPH TRAVERSE` SQL query on this node and return the distinct
    /// node ids from the `result` JSON column (`nodes[].id`).
    ///
    /// Blocks the current thread via `block_in_place` so it can be called from
    /// inside a `wait_for` closure (which is not `async`).
    pub fn traversed_node_ids(&self, sql: &str) -> Vec<String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let msgs = self.client.simple_query(sql).await.expect("graph traverse");
                let row = msgs
                    .iter()
                    .find_map(|m| match m {
                        tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
                        _ => None,
                    })
                    .expect("graph traverse returned no result row");
                let raw = row.get("result").expect("result column present");
                let v: sonic_rs::Value =
                    sonic_rs::from_str(raw).expect("result column is valid JSON");
                v.get("nodes")
                    .and_then(|n| n.as_array())
                    .expect("traverse result has a nodes array")
                    .iter()
                    .map(|n| {
                        n.get("id")
                            .and_then(|id| id.as_str())
                            .expect("node has string id")
                            .to_string()
                    })
                    .collect()
            })
        })
    }
}
