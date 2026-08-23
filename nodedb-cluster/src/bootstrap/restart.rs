// SPDX-License-Identifier: BUSL-1.1

//! Restart path: reload topology/routing from catalog after a clean shutdown or crash.

use std::sync::{Arc, Mutex, RwLock};

use tracing::info;

use crate::catalog::ClusterCatalog;
use crate::error::{ClusterError, Result};
use crate::multi_raft::MultiRaft;
use crate::transport::NexarTransport;

use super::config::{ClusterConfig, ClusterState};

/// Re-stamp self `NodeInfo.wire_version` to the current build's
/// `CLUSTER_WIRE_FORMAT_VERSION` when the persisted record is older
/// (records written by an older build carry the serde default `1`).
///
/// Only raises — never lowers — the recorded stamp. The updated stamp is
/// persisted back so subsequent restarts keep it. Returns `true` when a
/// re-stamp was applied. Pure helper so the upgrade path is testable
/// without a live transport.
pub(crate) fn restamp_self_wire_version(
    config: &ClusterConfig,
    topology: &mut crate::topology::ClusterTopology,
    catalog: &ClusterCatalog,
) -> Result<bool> {
    let build_version = crate::topology::CLUSTER_WIRE_FORMAT_VERSION;
    let needs_restamp = topology
        .get_node(config.node_id)
        .is_some_and(|n| n.wire_version < build_version);
    if needs_restamp {
        if let Some(self_info) = topology.get_node_mut(config.node_id) {
            self_info.wire_version = build_version;
        }
        catalog.save_topology(topology)?;
        info!(
            node_id = config.node_id,
            wire_version = build_version,
            "re-stamped self wire_version on restart after upgrade"
        );
    }
    Ok(needs_restamp)
}

/// Restart from persisted state — load topology and routing from catalog.
pub(super) fn restart(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    let mut topology = catalog
        .load_topology()?
        .ok_or_else(|| ClusterError::Transport {
            detail: "catalog is bootstrapped but topology is missing".into(),
        })?;

    // Re-stamp self on upgrade. Records persisted by an older build carry
    // the serde default (`1`) for `wire_version`; once this build starts,
    // its own `NodeInfo` must report the current version so the
    // cluster-wide view's `min_version` converges and version-gated
    // features can flip. Only raise — never lower — the recorded stamp.
    // The stamp is persisted back so subsequent restarts keep it.
    restamp_self_wire_version(config, &mut topology, catalog)?;

    // ONE shared routing handle: MultiRaft and ClusterState read/write the
    // same table so committed Raft conf-changes converge the data-plane view.
    let routing = Arc::new(RwLock::new(catalog.load_routing()?.ok_or_else(|| {
        ClusterError::Transport {
            detail: "catalog is bootstrapped but routing table is missing".into(),
        }
    })?));

    // Reconstruct MultiRaft from routing table. A restarting node
    // may be a voter (`info.members`) OR a learner (`info.learners`)
    // — the latter is the window between an `AddLearner` commit
    // and the follow-up `PromoteLearner` commit during a join. A
    // node that crashes inside that window must still come back
    // as a learner on restart; dropping the group entirely would
    // leave the node permanently without any copy of it and
    // silently broken.
    let mut multi_raft = MultiRaft::new_with_shared_routing(
        config.node_id,
        routing.clone(),
        config.data_dir.clone(),
    )
    .with_election_timeout(config.election_timeout_min, config.election_timeout_max)
    .with_log_compaction_threshold(config.log_compaction_threshold);
    // Snapshot the group membership out from under one read guard so the
    // guard is released before `add_group` (which proposes into Raft).
    let group_membership: Vec<(u64, Vec<u64>, Vec<u64>)> = {
        let rt = routing.read().unwrap_or_else(|p| p.into_inner());
        rt.group_members()
            .iter()
            .map(|(group_id, info)| (*group_id, info.members.clone(), info.learners.clone()))
            .collect()
    };
    for (group_id, members, learners) in group_membership {
        let is_voter = members.contains(&config.node_id);
        let is_learner = learners.contains(&config.node_id);

        if is_voter {
            let peers: Vec<u64> = members
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group(group_id, peers)?;
        } else if is_learner {
            // Voters are the full member set (none of them is
            // self). Other learners catching up alongside us are
            // tracked for replication too.
            let voters = members.clone();
            let other_learners: Vec<u64> = learners
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group_as_learner(group_id, voters, other_learners)?;
        }
    }

    // Register all known peers in the transport.
    for node in topology.all_nodes() {
        if node.node_id != config.node_id
            && let Some(addr) = node.socket_addr()
        {
            transport.register_peer(node.node_id, addr);
        }
    }

    info!(
        node_id = config.node_id,
        nodes = topology.node_count(),
        groups = multi_raft.group_count(),
        "restarted from catalog"
    );

    Ok(ClusterState {
        topology: Arc::new(RwLock::new(topology)),
        routing,
        multi_raft: Arc::new(Mutex::new(multi_raft)),
    })
}

#[cfg(test)]
mod tests {
    use super::super::bootstrap_fn::bootstrap;
    use super::*;
    use crate::catalog::ClusterCatalog;
    use std::time::Duration;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[tokio::test]
    async fn restart_from_catalog() {
        let (_dir, catalog) = temp_catalog();
        let config = ClusterConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:9400".parse().unwrap(),
            seed_nodes: vec![],
            num_groups: 4,
            replication_factor: 1,
            data_dir: _dir.path().to_path_buf(),
            force_bootstrap: false,
            join_retry: Default::default(),
            swim_udp_addr: None,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            install_snapshot_chunk_bytes: 4 * 1024 * 1024,
            orphan_partial_max_age_secs: 300,
            log_compaction_threshold: None,
        };

        // Bootstrap first.
        let _ = bootstrap(&config, &catalog, None).unwrap();

        // Create transport for restart.
        use crate::transport::credentials::TransportCredentials;
        let transport = NexarTransport::new(
            1,
            "127.0.0.1:0".parse().unwrap(),
            TransportCredentials::Insecure,
        )
        .unwrap();

        // Restart — should load from catalog.
        let state = restart(&config, &catalog, &transport).unwrap();

        assert_eq!(state.topology.read().unwrap().node_count(), 1);
        // num_groups() counts data groups + metadata group: 4 data + 1 = 5.
        assert_eq!(state.routing.read().unwrap().num_groups(), 5);
        assert_eq!(state.multi_raft.lock().unwrap().group_count(), 5);
    }

    /// A catalog whose self `NodeInfo` carries a stale wire_version (as
    /// persisted by an older build) must be re-stamped to the current
    /// `CLUSTER_WIRE_FORMAT_VERSION` on restart, in memory and on disk,
    /// so the cluster-wide view's `min_version` converges after upgrade.
    #[test]
    fn restart_restamps_stale_self_wire_version() {
        let (_dir, catalog) = temp_catalog();
        let config = ClusterConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:9400".parse().unwrap(),
            seed_nodes: vec![],
            num_groups: 1,
            replication_factor: 1,
            data_dir: _dir.path().to_path_buf(),
            force_bootstrap: false,
            join_retry: Default::default(),
            swim_udp_addr: None,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            install_snapshot_chunk_bytes: 4 * 1024 * 1024,
            orphan_partial_max_age_secs: 300,
            log_compaction_threshold: None,
        };

        // Simulate a catalog written by an older build: bootstrap a fresh
        // topology, then rewrite self's wire_version to the stale default.
        let _ = bootstrap(&config, &catalog, None).unwrap();
        let mut stale_topo = catalog.load_topology().unwrap().unwrap();
        stale_topo.get_node_mut(1).unwrap().wire_version = 1;
        catalog.save_topology(&stale_topo).unwrap();

        // Restart path re-stamps in memory AND on disk.
        let mut topo = catalog.load_topology().unwrap().unwrap();
        let restamped = restamp_self_wire_version(&config, &mut topo, &catalog).unwrap();
        assert!(restamped, "stale self stamp must be re-stamped");
        assert_eq!(
            topo.get_node(1).unwrap().wire_version,
            crate::topology::CLUSTER_WIRE_FORMAT_VERSION
        );

        // The re-stamp is persisted so later restarts keep it.
        let persisted = catalog.load_topology().unwrap().unwrap();
        assert_eq!(
            persisted.get_node(1).unwrap().wire_version,
            crate::topology::CLUSTER_WIRE_FORMAT_VERSION
        );

        // Idempotent: a second pass does nothing.
        let mut topo2 = catalog.load_topology().unwrap().unwrap();
        let again = restamp_self_wire_version(&config, &mut topo2, &catalog).unwrap();
        assert!(!again, "re-stamp must be idempotent (never lowers)");
    }
}
