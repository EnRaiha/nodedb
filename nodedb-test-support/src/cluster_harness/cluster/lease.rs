// SPDX-License-Identifier: BUSL-1.1

//! The authorization-lease barrier.
//!
//! A cluster node plans a permission-checked statement only while it holds
//! an authorization lease. A node can lose its lease with no fault:
//!
//! - Each new metadata term loads its floors before it grants any lease.
//!   The load needs a confirmed read index of the sequencer group, which is
//!   still electing while the cluster forms.
//! - An authorization-bearing DDL is acknowledged once every holder covered
//!   it or its lease expired. A node whose lease expired renews only on a
//!   later round.
//!
//! The barrier waits until every node holds a lease again, so the first
//! statement a test issues next is not refused with a retryable `55P03`.

use std::time::Duration;

use super::TestCluster;

/// How long each node may take to hold an authorization lease.
const LEASE_TIMEOUT: Duration = Duration::from_secs(15);

impl TestCluster {
    /// Block until every node can plan permission-checked statements.
    ///
    /// # Panics
    ///
    /// When a node holds no lease after 15 s, naming its index, its node id
    /// and how its last renewal round ended.
    pub async fn wait_for_authorization_leases(&self) {
        let outcomes = futures::future::join_all(self.nodes.iter().map(|node| {
            nodedb::control::security::auth_lease::await_planning_admitted(
                &node.shared,
                LEASE_TIMEOUT,
            )
        }))
        .await;
        let refused: Vec<String> = outcomes
            .into_iter()
            .enumerate()
            .filter_map(|(index, outcome)| {
                outcome.err().map(|error| {
                    format!(
                        "node index {index} (node id {}): {error}",
                        self.nodes[index].node_id
                    )
                })
            })
            .collect();
        assert!(
            refused.is_empty(),
            "a node held no authorization lease within {LEASE_TIMEOUT:?}: {}",
            refused.join("; ")
        );
    }
}
