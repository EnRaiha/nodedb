// SPDX-License-Identifier: BUSL-1.1

//! A learner caught up by a real Raft `InstallSnapshot` on a multi-core node.
//!
//! - It holds every row on the core its collection routes to. Reads route to
//!   one owning core per collection, so a snapshot installed onto a single
//!   core leaves every collection homed elsewhere invisible to reads.
//! - Its install survives a restart together with the writes made after it.
//!   A boot that re-applies the installed snapshot erases the later
//!   writes and brings back the rows they deleted.
//!
//! Both tests spread rows over collections homed on several cores, and force
//! the learner onto the snapshot path by compacting the leader's logs.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use nodedb::types::TenantId;

use crate::common::cluster_harness::shared_steps::key_collection;
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

const COMPACTION_THRESHOLD: u64 = 4;
const CORES: usize = 4;
const COLLECTIONS: usize = 6;
const ROWS: usize = 12;
const POST_ROWS: usize = 6;

fn collection(i: usize) -> String {
    format!("snap_mc_{i}")
}

async fn count_rows(client: &tokio_postgres::Client, table: &str) -> Option<usize> {
    let rows = client
        .simple_query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .ok()?;
    rows.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).and_then(|s| s.parse().ok()),
        _ => None,
    })
}

async fn exec_on_any(cluster: &TestCluster, sql: &str) {
    cluster.nodes[0]
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A 3-node cluster whose collections' groups compacted, plus a learner caught
/// up by snapshot. Returns the cluster, the learner's node id, and the groups.
async fn cluster_with_snapshot_learner() -> (TestCluster, u64, HashSet<u64>) {
    let mut cluster = TestCluster::spawn_three_with_compaction_threshold_rf_and_cores(
        COMPACTION_THRESHOLD,
        4,
        CORES,
    )
    .await
    .expect("3-node cluster, rf=4, 4 cores per node");

    for i in 0..COLLECTIONS {
        cluster
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION {} (id TEXT PRIMARY KEY, payload TEXT) \
                 WITH (engine='document_strict')",
                collection(i)
            ))
            .await
            .expect("CREATE COLLECTION");
    }
    wait_for(
        "all nodes see the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= COLLECTIONS)
        },
    )
    .await;

    for i in 0..COLLECTIONS {
        for r in 0..ROWS {
            exec_on_any(
                &cluster,
                &format!(
                    "INSERT INTO {} (id, payload) VALUES ('r{r}', 'v{r}')",
                    collection(i)
                ),
            )
            .await;
        }
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    // Every group the collections use compacted, so the learner can only be
    // caught up by a snapshot.
    let groups: HashSet<u64> = (0..COLLECTIONS)
        .map(|i| {
            cluster.nodes[0]
                .group_id_for_collection(&collection(i))
                .expect("collection maps to a data group")
        })
        .collect();
    for gid in &groups {
        let compacted = cluster
            .nodes
            .iter()
            .map(|n| n.group_snapshot_index(*gid))
            .max()
            .unwrap_or(0);
        assert!(compacted > 0, "group {gid} must compact before the join");
    }

    let learner_id = cluster
        .add_learner_node()
        .await
        .expect("add learner node")
        .node_id;
    {
        let learner = node(&cluster, learner_id);
        wait_for(
            "the learner installs a snapshot of every group",
            Duration::from_secs(30),
            Duration::from_millis(100),
            || {
                groups.iter().all(|gid| {
                    learner.hosts_data_group(*gid)
                        && learner.local_snapshot_index_for_group(*gid) > 0
                })
            },
        )
        .await;
        assert_eq!(learner.num_cores(), CORES);
    }
    (cluster, learner_id, groups)
}

fn node(cluster: &TestCluster, node_id: u64) -> &TestClusterNode {
    cluster
        .nodes
        .iter()
        .find(|n| n.node_id == node_id)
        .expect("node present")
}

/// Each test collection's home core on `node`, checked to span a core other
/// than the one a single-core install would pick.
fn homes(node: &TestClusterNode) -> HashMap<String, usize> {
    let system_core = node.home_core_of("__system");
    let homes: HashMap<String, usize> = (0..COLLECTIONS)
        .map(|i| (collection(i), node.home_core_of(&collection(i))))
        .collect();
    assert!(
        homes.values().any(|core| *core != system_core),
        "test collections must home on a core other than core {system_core}"
    );
    homes
}

fn tenants(node: &TestClusterNode, homes: &HashMap<String, usize>) -> HashSet<u64> {
    node.shared
        .credentials
        .catalog()
        .load_all_collections_across_databases()
        .expect("catalog read")
        .into_iter()
        .filter(|c| homes.contains_key(&c.name))
        .map(|c| c.tenant_id)
        .collect()
}

/// Rows per test collection on its owning core of `node`. Panics on a row
/// stored on any other core.
async fn owned_rows(
    node: &TestClusterNode,
    homes: &HashMap<String, usize>,
    tenants: &HashSet<u64>,
) -> HashMap<String, usize> {
    let mut per_collection: HashMap<String, usize> = HashMap::new();
    let mut misplaced: Vec<String> = Vec::new();
    for core in 0..CORES {
        for tenant in tenants {
            for key in node
                .document_keys_on_core(core, TenantId::new(*tenant))
                .await
            {
                let Some(coll) = key_collection(&key) else {
                    continue;
                };
                let Some(home) = homes.get(coll) else {
                    continue;
                };
                if *home == core {
                    *per_collection.entry(coll.to_string()).or_default() += 1;
                } else {
                    misplaced.push(format!("{coll} on core {core}, home {home}"));
                }
            }
        }
    }
    assert!(
        misplaced.is_empty(),
        "rows off their owning core: {misplaced:?}"
    );
    per_collection
}

/// Poll until every test collection holds exactly `expected` rows on its
/// owning core of `node`.
async fn await_owned_rows(node: &TestClusterNode, expected: usize, what: &str) {
    let homes = homes(node);
    let tenants = tenants(node, &homes);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let per_collection = owned_rows(node, &homes, &tenants).await;
        if homes
            .keys()
            .all(|c| per_collection.get(c).copied().unwrap_or(0) == expected)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: owning cores never held {expected} rows per collection: {per_collection:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn learner_snapshot_install_places_rows_on_owning_cores() {
    let (cluster, learner_id, _) = cluster_with_snapshot_learner().await;
    let learner = node(&cluster, learner_id);

    await_owned_rows(learner, ROWS, "after the install").await;

    // A routed read on the learner sees every row of every collection.
    for i in 0..COLLECTIONS {
        let coll = collection(i);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if count_rows(&learner.client, &coll).await == Some(ROWS) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "learner COUNT(*) of {coll} never reached {ROWS}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    cluster.shutdown().await;
}

/// Writes after the install survive a restart of the installed node, and a
/// row deleted after the install stays deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn post_install_writes_survive_restart() {
    let (cluster, learner_id, _) = cluster_with_snapshot_learner().await;
    await_owned_rows(node(&cluster, learner_id), ROWS, "after the install").await;

    for i in 0..COLLECTIONS {
        let coll = collection(i);
        for p in 0..POST_ROWS {
            exec_on_any(
                &cluster,
                &format!("INSERT INTO {coll} (id, payload) VALUES ('p{p}', 'w{p}')"),
            )
            .await;
        }
        exec_on_any(&cluster, &format!("DELETE FROM {coll} WHERE id = 'r0'")).await;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    let expected = ROWS + POST_ROWS - 1;
    await_owned_rows(node(&cluster, learner_id), expected, "before the restart").await;

    let cluster = cluster.restart_all().await.expect("restart every node");
    await_owned_rows(node(&cluster, learner_id), expected, "after the restart").await;

    cluster.shutdown().await;
}
