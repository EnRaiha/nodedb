// SPDX-License-Identifier: BUSL-1.1

//! [`TestCluster`] + [`ClusterSpawnConfig`] type definitions, and the
//! fast-election tuning shared by every `spawn_three*` entry point.

use nodedb_types::config::tuning::ClusterTransportTuning;

use super::super::node::TestClusterNode;

/// The spawn configuration used to bring up every node in a cluster.
///
/// Captured at spawn so that a later [`TestCluster::add_learner_node`]
/// brings the new node up with the *same* tuning — most importantly the
/// same Raft `log_compaction_threshold`, so the learner behaves
/// identically to the original members.
#[derive(Clone)]
pub(crate) struct ClusterSpawnConfig {
    pub(crate) tuning: ClusterTransportTuning,
    pub(crate) graph_tuning: nodedb_types::config::tuning::GraphTuning,
    pub(crate) query_tuning: nodedb_types::config::tuning::QueryTuning,
    pub(crate) num_cores: usize,
    pub(crate) log_compaction_threshold: Option<u64>,
    /// Raft replication factor used for every original member AND any
    /// later `add_learner_node()` call (HRW placement takes
    /// `min(replication_factor, node_count)`). Defaults to 3 for every
    /// spawn entry point except [`TestCluster::spawn_three_with_compaction_threshold_and_rf`].
    pub(crate) replication_factor: usize,
    /// When `true`, the node acquires its cluster handle from
    /// `init_single_node_calvin` (the flag-gated standalone Calvin
    /// synthesis) instead of building explicit `ClusterSettings` and
    /// calling `init_cluster_with_transport`. Used only by
    /// [`TestClusterNode::spawn_single_node_calvin`]. Defaults to `false`
    /// for every multi-node spawn path.
    pub(crate) single_node_calvin: bool,
}

/// An in-process cluster of `TestClusterNode`s.
pub struct TestCluster {
    pub nodes: Vec<TestClusterNode>,
    /// Config used for the original members; reused by `add_learner_node`.
    pub(super) spawn_config: ClusterSpawnConfig,
}

/// Fast election tuning used by [`TestCluster::spawn_three`] and
/// [`TestCluster::spawn_three_with_cores`].
pub(super) fn fast_cluster_tuning() -> ClusterTransportTuning {
    ClusterTransportTuning {
        // Fast health pings so the HealthMonitor re-broadcasts
        // topology within ~1s if the initial join broadcast was missed.
        health_ping_interval_secs: 1,
        // Sub-second election windows. Bootstrap defaults are 150/300ms;
        // we allow significantly more headroom (500/1000ms) because
        // integration tests share the host CPU pool with hundreds of
        // unit tests running in parallel — under that load the Raft
        // tick loop can be starved long enough that aggressive
        // 200/500ms windows trigger spurious re-elections mid-test.
        // 500/1000ms is still ~3× faster than the seconds-floor of
        // 1s/2s but stable under contention.
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        ..ClusterTransportTuning::default()
    }
}
