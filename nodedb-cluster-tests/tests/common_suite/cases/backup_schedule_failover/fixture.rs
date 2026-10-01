// SPDX-License-Identifier: BUSL-1.1

//! The cluster, schedule and per-node tickers the backup schedule failover
//! tests drive.

use crate::common;
use common::cluster_harness::shared_steps::holds_vshard0_lease;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::path::PathBuf;
use std::time::{Duration, Instant};

use nodedb::config::server::BackupScheduleSettings;
use nodedb::control::backup::schedule::marks::settled_through;
use nodedb::event::scheduler::backup_job::BackupJobs;
use nodedb::event::scheduler::dispatcher::{JobDispatcher, JobDispatcherConfig};
use nodedb_types::DatabaseId;

const COLLECTION: &str = "bsf_orders";
pub(super) const DATABASE: &str = "default";
const TARGET_DIR: &str = "nightly";
pub(super) const CONVERGE: Duration = Duration::from_secs(30);
pub(super) const STEP: Duration = Duration::from_millis(100);

/// The leader of vShard 0's Raft group as `node`'s Raft status reports it,
/// `0` while none is known. The scheduler decides who fires from the same
/// status.
fn vshard0_leader(node: &TestClusterNode) -> u64 {
    let group = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(0)
        .expect("vShard 0 maps to a group");
    node.all_group_leaders()
        .into_iter()
        .find_map(|(id, leader)| (id == group).then_some(leader))
        .unwrap_or(0)
}

/// One node's scheduler: its backup jobs and dispatcher.
struct Ticker {
    jobs: BackupJobs,
    dispatcher: JobDispatcher,
}

impl Ticker {
    fn new(schedule: &BackupScheduleSettings) -> Self {
        Self {
            jobs: BackupJobs::new(std::slice::from_ref(schedule)),
            dispatcher: JobDispatcher::new(JobDispatcherConfig {
                max_concurrent_jobs: 4,
                max_result_bytes: u64::MAX,
            }),
        }
    }

    /// Fire one tick at `now_secs` on `node` and wait for its work.
    async fn tick(&self, node: &TestClusterNode, now_secs: u64) {
        self.jobs.fire(
            &node.shared,
            &self.dispatcher,
            &node.shared.job_history,
            now_secs,
        );
        let deadline = Instant::now() + Duration::from_secs(120);
        while self.jobs.in_flight() > 0 {
            assert!(
                Instant::now() < deadline,
                "node {}: a scheduled backup did not finish",
                node.node_id
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// A running cluster with one row to back up, its schedule, and the due
/// minute `M`.
pub(super) struct Fixture {
    _root: tempfile::TempDir,
    root: PathBuf,
    pub(super) cluster: TestCluster,
    pub(super) schedule: BackupScheduleSettings,
    tickers: Vec<Ticker>,
    pub(super) due: u64,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let dir = tempfile::tempdir().expect("backup root");
        let root = dir.path().canonicalize().expect("canonical backup root");
        let cluster = TestCluster::spawn_three_with_backup_root(root.clone())
            .await
            .expect("spawn 3-node cluster");
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLLECTION}"))
            .await
            .expect("create collection");
        let deadline = Instant::now() + CONVERGE;
        loop {
            match cluster.nodes[0]
                .client
                .simple_query(&format!("INSERT INTO {COLLECTION} {{ id: 'o1', n: 1 }}"))
                .await
            {
                Ok(_) => break,
                Err(e) if Instant::now() < deadline => {
                    tracing::debug!(error = %e, "insert not accepted yet; retrying");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => panic!("insert: {e}"),
            }
        }
        cluster.wait_for_full_apply_convergence(CONVERGE).await;

        let schedule = BackupScheduleSettings {
            database: DATABASE.into(),
            target: format!("file://{}/{TARGET_DIR}", root.display()),
            cron: "*/5 * * * *".into(),
            keep: 3,
        };
        let now_min = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_secs()
            / 60;
        let tickers = cluster
            .nodes
            .iter()
            .map(|_| Ticker::new(&schedule))
            .collect();
        wait_for("one node holds the vShard 0 lease", CONVERGE, STEP, || {
            let leader = vshard0_leader(&cluster.nodes[0]);
            leader != 0
                && cluster
                    .nodes
                    .iter()
                    .all(|node| vshard0_leader(node) == leader)
                && cluster
                    .nodes
                    .iter()
                    .filter(|node| holds_vshard0_lease(node))
                    .count()
                    == 1
        })
        .await;
        Self {
            _root: dir,
            root,
            cluster,
            schedule,
            tickers,
            // A multiple of 5, so neither neighbouring minute matches.
            due: (now_min / 5 + 1) * 5,
        }
    }

    /// The node holding the vShard 0 lease, else the leader Raft reports.
    pub(super) fn leader(&self) -> u64 {
        self.cluster
            .nodes
            .iter()
            .find(|node| holds_vshard0_lease(node))
            .map_or_else(
                || vshard0_leader(&self.cluster.nodes[0]),
                |node| node.node_id,
            )
    }

    /// Tick every node at `now_secs`.
    pub(super) async fn tick_all(&self, now_secs: u64) {
        for (node, ticker) in self.cluster.nodes.iter().zip(&self.tickers) {
            ticker.tick(node, now_secs).await;
        }
    }

    /// Tick every node but `node_id` at `now_secs`.
    pub(super) async fn tick_except(&self, node_id: u64, now_secs: u64) {
        for (node, ticker) in self.cluster.nodes.iter().zip(&self.tickers) {
            if node.node_id != node_id {
                ticker.tick(node, now_secs).await;
            }
        }
    }

    /// Cut node `node_id` off from every other node, both ways, or heal it.
    pub(super) fn partition(&self, node_id: u64, severed: bool) {
        let transport = |node: &TestClusterNode| {
            std::sync::Arc::clone(
                node.shared
                    .cluster_transport
                    .as_ref()
                    .expect("cluster transport"),
            )
        };
        let cut = self
            .cluster
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(transport)
            .expect("the node is a member");
        for node in self
            .cluster
            .nodes
            .iter()
            .filter(|node| node.node_id != node_id)
        {
            let peer = transport(node);
            if severed {
                peer.sever(node_id);
                cut.sever(node.node_id);
            } else {
                peer.heal(node_id);
                cut.heal(node.node_id);
            }
        }
    }

    /// Wait until one node other than `excluded` holds the vShard 0 lease.
    pub(super) async fn wait_new_coordinator(&self, excluded: u64) {
        let nodes = &self.cluster.nodes;
        wait_for(
            "a new node holds the vShard 0 lease",
            CONVERGE,
            STEP,
            || {
                nodes
                    .iter()
                    .filter(|node| node.node_id != excluded && holds_vshard0_lease(node))
                    .count()
                    == 1
            },
        )
        .await;
    }

    /// Tick node `node_id` alone at `now_secs`.
    pub(super) async fn tick_node(&self, node_id: u64, now_secs: u64) {
        for (node, ticker) in self.cluster.nodes.iter().zip(&self.tickers) {
            if node.node_id == node_id {
                ticker.tick(node, now_secs).await;
            }
        }
    }

    /// The mark in each node's local catalog, in node order.
    pub(super) fn local_marks(&self) -> Vec<Option<u64>> {
        self.cluster
            .nodes
            .iter()
            .map(|node| settled_through(&node.shared, &self.schedule).expect("read mark"))
            .collect()
    }

    pub(super) async fn wait_marks(&self, desc: &str, mark: u64) {
        wait_for(desc, CONVERGE, STEP, || {
            self.local_marks().iter().all(|local| *local == Some(mark))
        })
        .await;
    }

    /// Kill the vShard 0 leader, then wait for a new one and a live leader
    /// on every group: the backup reads every data group.
    pub(super) async fn kill_leader(&mut self) -> u64 {
        let leader = self.leader();
        let idx = self
            .cluster
            .nodes
            .iter()
            .position(|node| node.node_id == leader)
            .expect("the vShard 0 leader is a member");
        let dead = self.cluster.nodes.remove(idx);
        self.tickers.remove(idx);
        dead.shutdown().await;
        let nodes = &self.cluster.nodes;
        wait_for(
            "the survivors elect a new vShard 0 leader",
            CONVERGE,
            STEP,
            || {
                let next = vshard0_leader(&nodes[0]);
                next != 0 && next != leader && nodes.iter().all(|node| vshard0_leader(node) == next)
            },
        )
        .await;
        wait_for("every group has a live leader", CONVERGE, STEP, || {
            nodes.iter().all(|node| {
                node.all_group_leaders()
                    .into_iter()
                    .all(|(_, group_leader)| group_leader != 0 && group_leader != leader)
            })
        })
        .await;
        self.wait_new_coordinator(leader).await;
        leader
    }

    /// Successful and failed runs `node` recorded for the schedule.
    pub(super) fn runs(&self, node: &TestClusterNode) -> (usize, usize) {
        let runs = node.shared.job_history.last_runs(
            DatabaseId::DEFAULT.as_u64(),
            0,
            &self.schedule.job_name(),
            100,
        );
        let ok = runs.iter().filter(|run| run.success).count();
        (ok, runs.len() - ok)
    }

    /// Every envelope under the target, sorted.
    pub(super) fn envelopes(&self) -> Vec<String> {
        let dir = self.root.join(TARGET_DIR);
        if !dir.exists() {
            return Vec::new();
        }
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .expect("list target")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        names.sort();
        names
    }

    pub(super) async fn shutdown(self) {
        self.cluster.shutdown().await;
        for ticker in self.tickers {
            ticker.dispatcher.shutdown_and_drain().await;
        }
    }
}
