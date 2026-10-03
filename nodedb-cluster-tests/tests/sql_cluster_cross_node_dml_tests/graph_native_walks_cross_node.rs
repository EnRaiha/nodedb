// SPDX-License-Identifier: BUSL-1.1

//! Native graph walks sent as one plan give a single node's answer in a
//! cluster.
//!
//! With a replication factor of 1 and node keys that cover every data group,
//! a walk crosses every node's partitions. The reference is the same graph on
//! a single node, read over the same native opcodes.
//!
//! - `GraphHop` returns the reached node set. A single node's Data Plane
//!   collects it in a `HashSet` and returns it in that set's iteration order
//!   (`nodedb-graph/src/traversal.rs`), so the order is unspecified and the
//!   comparison is by set.
//! - `GraphPath` with no collection walks the edges of every collection. The
//!   test graph has one shortest path, laid over two collections, so the
//!   comparison is by the exact path.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_types::protocol::{OpCode, TextFields};

use super::graph_native_walks_support::{
    data_groups, names, native_session, spread_chain, wait_one_replica_per_group, walk, walk_names,
};
use crate::common::cluster_harness::{TestCluster, wait_for};
use crate::common::pgwire_harness::TestServer;

const HOP_COLL: &str = "gwalk_hop";
const PATH_COLL_A: &str = "gwalk_path_a";
const PATH_COLL_B: &str = "gwalk_path_b";
/// Nodes on the path chain: short enough for the default depth quota.
const PATH_LEN: usize = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_hop_and_collectionless_path_match_a_single_node() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    for coll in [HOP_COLL, PATH_COLL_A, PATH_COLL_B] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {coll}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {coll}: {e}"));
    }
    wait_for(
        "all 3 nodes see the collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 3)
        },
    )
    .await;

    let (groups, group_of) = data_groups(&cluster);
    wait_one_replica_per_group(&cluster, &groups).await;

    // Hop graph: a ring plus a chord from every third node.
    let hop_names = names("h", &groups, &group_of);
    let n = hop_names.len();
    let mut inserts: Vec<String> = Vec::new();
    for i in 0..n {
        inserts.push(format!(
            "GRAPH INSERT EDGE IN '{HOP_COLL}' FROM '{}' TO '{}' TYPE 'K'",
            hop_names[i],
            hop_names[(i + 1) % n]
        ));
        if i % 3 == 0 {
            let j = (i * 7 + 3) % n;
            if j != i {
                inserts.push(format!(
                    "GRAPH INSERT EDGE IN '{HOP_COLL}' FROM '{}' TO '{}' TYPE 'C'",
                    hop_names[i], hop_names[j]
                ));
            }
        }
    }
    // Path graph: one chain whose edges alternate between two collections, so
    // only a walk over every collection reaches its end.
    let path_names = spread_chain("p", PATH_LEN, &group_of);
    for (i, pair) in path_names.windows(2).enumerate() {
        let coll = if i % 2 == 0 { PATH_COLL_A } else { PATH_COLL_B };
        inserts.push(format!(
            "GRAPH INSERT EDGE IN '{coll}' FROM '{}' TO '{}' TYPE 'N'",
            pair[0], pair[1]
        ));
    }

    let reference = TestServer::start().await;
    for coll in [HOP_COLL, PATH_COLL_A, PATH_COLL_B] {
        reference
            .exec(&format!("CREATE COLLECTION {coll}"))
            .await
            .unwrap_or_else(|e| panic!("reference CREATE COLLECTION {coll}: {e}"));
    }
    for insert in &inserts {
        reference.exec(insert).await.expect("reference insert");
        cluster.nodes[0]
            .client
            .simple_query(insert)
            .await
            .unwrap_or_else(|e| panic!("cluster {insert}: {e:?}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    let mut single = native_session(reference.native_port).await;
    let mut seq = 10u64;
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let mut clustered = native_session(node.native_port).await;

        for start in hop_names.iter().step_by(5) {
            for (depth, direction) in [(1u32, "out"), (2, "out"), (3, "both")] {
                seq += 1;
                let fields = TextFields {
                    collection: Some(HOP_COLL.to_string()),
                    start_node: Some(start.clone()),
                    depth: Some(depth),
                    direction: Some(direction.to_string()),
                    ..Default::default()
                };
                let expected: BTreeSet<String> =
                    walk_names(&walk(&mut single, seq, OpCode::GraphHop, fields.clone()).await)
                        .into_iter()
                        .collect();
                assert!(
                    expected.len() > 1,
                    "the single-node hop from {start} reaches a neighbour"
                );
                let got: BTreeSet<String> =
                    walk_names(&walk(&mut clustered, seq, OpCode::GraphHop, fields).await)
                        .into_iter()
                        .collect();
                assert_eq!(
                    got, expected,
                    "node {idx}: native hop from {start}, depth {depth}, {direction}, \
                     must reach the single-node set"
                );
            }
        }

        let last = path_names.last().expect("path names");
        seq += 1;
        let fields = TextFields {
            start_node: Some(path_names[0].clone()),
            end_node: Some(last.clone()),
            depth: Some(path_names.len() as u32),
            ..Default::default()
        };
        let expected = walk_names(&walk(&mut single, seq, OpCode::GraphPath, fields.clone()).await);
        assert_eq!(
            expected, path_names,
            "the single-node path with no collection follows the chain over both collections"
        );
        let got = walk_names(&walk(&mut clustered, seq, OpCode::GraphPath, fields).await);
        assert_eq!(
            got, expected,
            "node {idx}: a native path with no collection must equal the single-node path"
        );
    }

    cluster.shutdown().await;
}
