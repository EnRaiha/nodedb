// SPDX-License-Identifier: BUSL-1.1

//! Graph reads that need several partitions return the whole graph's answer.
//!
//! A graph edge lives on the key vShard of each endpoint, so with a
//! replication factor of 1 a graph whose node keys cover every data group has
//! a share of its edges on every node. `GRAPH NEIGHBORS`, `SHOW GRAPH STATS`
//! and every `GRAPH ALGO` must answer from all of them. The reference is the
//! same graph on a single node.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use nodedb_types::id::VShardId;

use crate::common::cluster_harness::{TestCluster, wait_for};
use crate::common::pgwire_harness::TestServer;

const COLL: &str = "gown_xnode";
/// The smallest ring the graph uses, even when fewer names cover every group.
const MIN_NODES: usize = 24;
/// Algorithms whose answer is exact and deterministic on both sides.
const EXACT_ALGOS: [&str; 10] = [
    "LABEL_PROPAGATION",
    "LCC",
    "BETWEENNESS",
    "CLOSENESS",
    "HARMONIC",
    "DEGREE",
    "LOUVAIN",
    "TRIANGLES",
    "DIAMETER",
    "KCORE",
];
const PAGERANK_TOLERANCE: f64 = 1e-4;

/// Node names `n0, n1, …`, enough that their key vShards cover every group.
fn node_names(groups: &BTreeSet<u64>, group_of: impl Fn(u32) -> u64) -> Vec<String> {
    let mut names = Vec::new();
    let mut covered = BTreeSet::new();
    let mut i = 0usize;
    while names.len() < MIN_NODES || covered != *groups {
        let name = format!("n{i}");
        covered.insert(group_of(VShardId::from_key(name.as_bytes()).as_u32()));
        names.push(name);
        i += 1;
    }
    names
}

/// A ring over `names` plus a chord from every third node, so the graph has
/// cycles, triangles and nodes of different degree.
fn edges(names: &[String]) -> Vec<(String, String, &'static str)> {
    let n = names.len();
    let mut out = Vec::new();
    for i in 0..n {
        out.push((names[i].clone(), names[(i + 1) % n].clone(), "K"));
        if i % 3 == 0 {
            let j = (i * 7 + 3) % n;
            if j != i {
                out.push((names[i].clone(), names[j].clone(), "C"));
            }
        }
    }
    out
}

async fn cluster_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<String>> {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = Vec::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            rows.push(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("").to_string())
                    .collect(),
            );
        }
    }
    rows
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// `node → value` from a two-column algorithm result.
fn by_node(rows: &[Vec<String>]) -> HashMap<String, String> {
    rows.iter()
        .map(|row| (row[0].clone(), row.get(1).cloned().unwrap_or_default()))
        .collect()
}

/// The partition of nodes by component id, independent of the id values.
fn components(rows: &[Vec<String>]) -> BTreeSet<BTreeSet<String>> {
    let mut by_id: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (node, id) in by_node(rows) {
        by_id.entry(id).or_default().insert(node);
    }
    by_id.into_values().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn graph_reads_spanning_every_group_match_a_single_node() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "all 3 nodes see the collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    let (groups, group_of_vshard): (BTreeSet<u64>, HashMap<u32, u64>) = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let groups: BTreeSet<u64> = routing
            .group_ids()
            .into_iter()
            .filter(|g| *g != 0)
            .collect();
        let mut map = HashMap::new();
        for &g in &groups {
            for vs in routing.vshards_for_group(g) {
                map.insert(vs, g);
            }
        }
        (groups, map)
    };
    // Placement convergence leaves one replica per group, so each node holds
    // only the groups it replicates.
    wait_for(
        "each data group has exactly one replica",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            groups.iter().all(|&g| {
                cluster
                    .nodes
                    .iter()
                    .filter(|node| node.replicates_data_group(g))
                    .count()
                    == 1
            })
        },
    )
    .await;

    let names = node_names(&groups, |vs| group_of_vshard.get(&vs).copied().unwrap_or(0));
    let edges = edges(&names);

    let reference = TestServer::start().await;
    reference
        .exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("reference CREATE COLLECTION");
    for (src, dst, label) in &edges {
        let insert =
            format!("GRAPH INSERT EDGE IN '{COLL}' FROM '{src}' TO '{dst}' TYPE '{label}'");
        reference.exec(&insert).await.expect("reference insert");
        cluster.nodes[0]
            .client
            .simple_query(&insert)
            .await
            .unwrap_or_else(|e| panic!("cluster insert {src}->{dst}: {e:?}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    let stats_sql = format!("SHOW GRAPH STATS '{COLL}'");
    let expected_stats = sorted(
        reference
            .query_rows(&stats_sql)
            .await
            .expect("reference stats"),
    );

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let client = &node.client;

        assert_eq!(
            sorted(cluster_rows(client, &stats_sql).await),
            expected_stats,
            "node {idx}: SHOW GRAPH STATS must count every partition"
        );

        for name in &names {
            let sql = format!("GRAPH NEIGHBORS IN '{COLL}' OF '{name}' DIRECTION both");
            let expected = sorted(
                reference
                    .query_rows(&sql)
                    .await
                    .expect("reference neighbors"),
            );
            assert_eq!(
                sorted(cluster_rows(client, &sql).await),
                expected,
                "node {idx}: GRAPH NEIGHBORS OF '{name}' must read the node's owner"
            );
        }

        for algo in EXACT_ALGOS {
            let sql = format!("GRAPH ALGO {algo} ON {COLL}");
            let expected = sorted(reference.query_rows(&sql).await.expect("reference algo"));
            assert!(!expected.is_empty(), "reference {algo} must return rows");
            assert_eq!(
                sorted(cluster_rows(client, &sql).await),
                expected,
                "node {idx}: GRAPH ALGO {algo} must equal the single-node answer"
            );
        }

        let sssp = format!("GRAPH ALGO SSSP ON {COLL} SOURCE '{}'", names[0]);
        let expected = sorted(reference.query_rows(&sssp).await.expect("reference sssp"));
        assert_eq!(
            sorted(cluster_rows(client, &sssp).await),
            expected,
            "node {idx}: GRAPH ALGO SSSP must equal the single-node answer"
        );

        let wcc = format!("GRAPH ALGO WCC ON {COLL}");
        let expected = reference.query_rows(&wcc).await.expect("reference wcc");
        assert_eq!(
            components(&cluster_rows(client, &wcc).await),
            components(&expected),
            "node {idx}: GRAPH ALGO WCC must find the single-node components"
        );

        let pagerank = format!("GRAPH ALGO PAGERANK ON {COLL}");
        let expected = by_node(
            &reference
                .query_rows(&pagerank)
                .await
                .expect("reference pagerank"),
        );
        let got = by_node(&cluster_rows(client, &pagerank).await);
        assert_eq!(
            got.keys().collect::<BTreeSet<_>>(),
            expected.keys().collect::<BTreeSet<_>>(),
            "node {idx}: GRAPH ALGO PAGERANK must rank every node"
        );
        for (node_name, rank) in &expected {
            let want: f64 = rank.parse().expect("reference rank");
            let have: f64 = got[node_name].parse().expect("cluster rank");
            assert!(
                (want - have).abs() <= PAGERANK_TOLERANCE,
                "node {idx}: PAGERANK of {node_name} is {have}, single node gives {want}"
            );
        }
    }

    cluster.shutdown().await;
}
