// SPDX-License-Identifier: BUSL-1.1

//! Stop every node of a [`TestCluster`] and start each again in place, or
//! stop one member and bring it back later.

use super::TestCluster;
use super::types::ClusterSpawnConfig;
use crate::cluster_harness::node::TestClusterNode;
use crate::cluster_harness::node::lifecycle::StoppedNode;

/// A member stopped by [`TestCluster::stop_member`]. It keeps the member's
/// data directory until [`TestCluster::restart_member`] brings it back.
pub struct StoppedMember {
    index: usize,
    node: StoppedNode,
}

/// Every node of a [`TestCluster`], stopped by [`TestCluster::stop_all`].
/// Each keeps its data directory until [`Self::start_all`].
pub struct StoppedCluster {
    nodes: Vec<StoppedNode>,
    spawn_config: ClusterSpawnConfig,
}

/// One stopped node, as a caller that rewrites its data directory sees it.
#[derive(Debug, Clone)]
pub struct StoppedNodeInfo {
    pub node_id: u64,
    pub listen_addr: std::net::SocketAddr,
    pub data_dir: std::path::PathBuf,
}

impl StoppedCluster {
    /// Every stopped node, in cluster order.
    pub fn nodes(&self) -> Vec<StoppedNodeInfo> {
        self.nodes
            .iter()
            .map(|node| StoppedNodeInfo {
                node_id: node.node_id(),
                listen_addr: node.listen_addr(),
                data_dir: node.data_dir().to_path_buf(),
            })
            .collect()
    }

    /// Bring every node back on its node id, listen address and data
    /// directory, together, since each Raft group needs a quorum to elect,
    /// and wait until the cluster is ready.
    pub async fn start_all(self) -> Result<TestCluster, Box<dyn std::error::Error + Send + Sync>> {
        let StoppedCluster {
            nodes,
            spawn_config,
        } = self;
        let seeds: Vec<std::net::SocketAddr> =
            nodes.iter().map(|node| node.listen_addr()).collect();
        let nodes = futures::future::try_join_all(
            nodes
                .into_iter()
                .map(|node| TestClusterNode::restart(node, seeds.clone(), &spawn_config)),
        )
        .await?;
        let cluster = TestCluster {
            nodes,
            spawn_config,
        };
        cluster.await_ready(super::ready::LeaderBar::Elected).await;
        Ok(cluster)
    }
}

impl TestCluster {
    /// Stop every node, keeping each data directory. Every node stops before
    /// [`StoppedCluster::start_all`] restarts any, so nothing survives in
    /// memory: what each node serves afterwards comes from its own disk.
    pub async fn stop_all(
        self,
    ) -> Result<StoppedCluster, Box<dyn std::error::Error + Send + Sync>> {
        let TestCluster {
            nodes,
            spawn_config,
        } = self;
        let mut stopped = Vec::with_capacity(nodes.len());
        for node in nodes {
            stopped.push(node.stop_for_restart().await?);
        }
        Ok(StoppedCluster {
            nodes: stopped,
            spawn_config,
        })
    }

    /// Stop every node, then bring every one back on its node id, listen
    /// address and data directory, and wait until the cluster is ready.
    pub async fn restart_all(self) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        self.stop_all().await?.start_all().await
    }

    /// Stop the member at `index` and take it out of [`Self::nodes`]. The
    /// rest of the cluster keeps running and commits without it.
    pub async fn stop_member(
        &mut self,
        index: usize,
    ) -> Result<StoppedMember, Box<dyn std::error::Error + Send + Sync>> {
        let node = self.nodes.remove(index);
        Ok(StoppedMember {
            index,
            node: node.stop_for_restart().await?,
        })
    }

    /// Bring `stopped` back on its node id, listen address and data
    /// directory, at its former index, and wait until the cluster is ready.
    /// It catches up with what the cluster committed while it was down.
    pub async fn restart_member(
        &mut self,
        stopped: StoppedMember,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let StoppedMember { index, node } = stopped;
        let mut seeds: Vec<std::net::SocketAddr> =
            self.nodes.iter().map(|member| member.listen_addr).collect();
        seeds.push(node.listen_addr());
        let restarted = TestClusterNode::restart(node, seeds, &self.spawn_config).await?;
        self.nodes.insert(index, restarted);
        self.await_ready(super::ready::LeaderBar::Elected).await;
        Ok(())
    }
}
