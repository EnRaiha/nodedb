// SPDX-License-Identifier: BUSL-1.1

//! Native graph walks and `GRAPH TRAVERSE` under a visit cap admit the same
//! nodes in a cluster as on a single node.
//!
//! With a replication factor of 1 and node keys that cover every data group,
//! a capped walk crosses every node's partitions. The reference is the same
//! graph on a single node with the same cap.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb_test_support::native_harness::send_request;
use nodedb_types::protocol::{OpCode, ResponseStatus, TextFields};

use super::graph_native_walks_support::{
    data_groups, names, native_session, wait_one_replica_per_group, walk, walk_names,
};
use crate::common::cluster_harness::{TestCluster, wait_for};
use crate::common::pgwire_harness::TestServer;

/// The visit cap of the capped walks: the hub and its 12 neighbours leave
/// room for 7 of the 24 nodes one level further.
const CAP: usize = 20;
const CAPPED_COLL: &str = "gwalk_capped";

/// The node ids of a `GRAPH TRAVERSE` result: `{nodes:[{id,depth}], ...}`.
fn traverse_node_ids(msgs: &[tokio_postgres::SimpleQueryMessage]) -> BTreeSet<(String, u64)> {
    let cell = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("GRAPH TRAVERSE returns a row");
    let value: serde_json::Value = sonic_rs::from_str(&cell).expect("GRAPH TRAVERSE returns JSON");
    value["nodes"]
        .as_array()
        .expect("a nodes array")
        .iter()
        .map(|node| {
            (
                node["id"].as_str().expect("a node id").to_string(),
                node["depth"].as_u64().expect("a node depth"),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn capped_walks_admit_the_single_node_set() {
    let graph_tuning = nodedb_types::config::tuning::GraphTuning {
        max_visited: CAP,
        ..Default::default()
    };
    let cluster =
        TestCluster::spawn_three_with_replication_factor_and_graph_tuning(1, graph_tuning.clone())
            .await
            .expect("3-node RF1 cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {CAPPED_COLL}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {CAPPED_COLL}: {e}"));
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
    let (groups, group_of) = data_groups(&cluster);
    wait_one_replica_per_group(&cluster, &groups).await;

    // The hub points at 12 nodes, inserted in reverse name order, and each of
    // them at two more. Edge order differs from name order everywhere.
    let first = names("f", &groups, &group_of);
    let second = names("s", &groups, &group_of);
    let hub = "hub";
    let mut inserts: Vec<String> = Vec::new();
    for mid in first.iter().take(12).rev() {
        inserts.push(format!(
            "GRAPH INSERT EDGE IN '{CAPPED_COLL}' FROM '{hub}' TO '{mid}' TYPE 'K'"
        ));
    }
    for (i, mid) in first.iter().take(12).enumerate() {
        for leaf in [&second[2 * i + 1], &second[2 * i]] {
            inserts.push(format!(
                "GRAPH INSERT EDGE IN '{CAPPED_COLL}' FROM '{mid}' TO '{leaf}' TYPE 'K'"
            ));
        }
    }
    // Two paths of equal length to `x`, stored larger name first, and a tail
    // `t3` five hops out that the cap stops the path search short of.
    let (tie_low, tie_high) = if first[0] < first[1] {
        (&first[0], &first[1])
    } else {
        (&first[1], &first[0])
    };
    for (src, dst) in [
        (tie_high.as_str(), "x"),
        (tie_low.as_str(), "x"),
        (second[0].as_str(), "t1"),
        ("t1", "t2"),
        ("t2", "t3"),
    ] {
        inserts.push(format!(
            "GRAPH INSERT EDGE IN '{CAPPED_COLL}' FROM '{src}' TO '{dst}' TYPE 'K'"
        ));
    }

    let reference = TestServer::start_with_graph_tuning(graph_tuning).await;
    reference
        .exec(&format!("CREATE COLLECTION {CAPPED_COLL}"))
        .await
        .unwrap_or_else(|e| panic!("reference CREATE COLLECTION {CAPPED_COLL}: {e}"));
    for (i, insert) in inserts.iter().enumerate() {
        reference.exec(insert).await.expect("reference insert");
        cluster.nodes[i % cluster.nodes.len()]
            .client
            .simple_query(insert)
            .await
            .unwrap_or_else(|e| panic!("cluster {insert}: {e:?}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    let traverse_sql = format!("GRAPH TRAVERSE IN '{CAPPED_COLL}' FROM '{hub}' DEPTH 2");
    let expected_traverse = traverse_node_ids(
        &reference
            .client
            .simple_query(&traverse_sql)
            .await
            .expect("reference GRAPH TRAVERSE"),
    );
    assert_eq!(
        expected_traverse.len(),
        CAP,
        "the single-node traverse stops at the cap"
    );

    let mut single = native_session(reference.native_port).await;
    let mut seq = 100u64;
    let fields = TextFields {
        collection: Some(CAPPED_COLL.to_string()),
        start_node: Some(hub.to_string()),
        depth: Some(2),
        direction: Some("out".to_string()),
        ..Default::default()
    };
    seq += 1;
    let expected_hop: BTreeSet<String> =
        walk_names(&walk(&mut single, seq, OpCode::GraphHop, fields.clone()).await)
            .into_iter()
            .collect();
    assert_eq!(
        expected_hop.len(),
        CAP,
        "the single-node hop stops at the cap"
    );

    let path_between = |from: &str, to: &str| TextFields {
        collection: Some(CAPPED_COLL.to_string()),
        start_node: Some(from.to_string()),
        end_node: Some(to.to_string()),
        depth: Some(5),
        ..Default::default()
    };
    let path_fields = |to: &str| path_between(hub, to);
    // An endpoint no edge names is absent from the graph: no path, from
    // either end.
    let absent_ends = [(hub, "ghost"), ("ghost", "x")];
    let mut expected_absent = Vec::new();
    for (from, to) in absent_ends {
        seq += 1;
        let response =
            send_request(&mut single, seq, OpCode::GraphPath, path_between(from, to)).await;
        assert_ne!(
            response.status,
            ResponseStatus::Ok,
            "the single-node path from {from} to {to} finds an absent endpoint: {response:?}"
        );
        expected_absent.push(response.status);
    }
    seq += 1;
    let expected_tie =
        walk_names(&walk(&mut single, seq, OpCode::GraphPath, path_fields("x")).await);
    assert_eq!(
        expected_tie,
        vec![hub.to_string(), tie_low.clone(), "x".to_string()],
        "the single-node path takes the smaller-named of two equal paths"
    );
    seq += 1;
    let capped = send_request(&mut single, seq, OpCode::GraphPath, path_fields("t3")).await;
    assert_ne!(
        capped.status,
        ResponseStatus::Ok,
        "the single-node path search stops at the cap before reaching t3: {capped:?}"
    );

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let got_traverse = traverse_node_ids(
            &node
                .client
                .simple_query(&traverse_sql)
                .await
                .unwrap_or_else(|e| panic!("node {idx}: GRAPH TRAVERSE: {e}")),
        );
        assert_eq!(
            got_traverse, expected_traverse,
            "node {idx}: a capped GRAPH TRAVERSE admits the single-node nodes"
        );
        let mut clustered = native_session(node.native_port).await;
        seq += 1;
        let got_hop: BTreeSet<String> =
            walk_names(&walk(&mut clustered, seq, OpCode::GraphHop, fields.clone()).await)
                .into_iter()
                .collect();
        assert_eq!(
            got_hop, expected_hop,
            "node {idx}: a capped native hop admits the single-node set"
        );
        seq += 1;
        let got_tie =
            walk_names(&walk(&mut clustered, seq, OpCode::GraphPath, path_fields("x")).await);
        assert_eq!(
            got_tie, expected_tie,
            "node {idx}: a native path takes the single-node tie"
        );
        seq += 1;
        let got_capped =
            send_request(&mut clustered, seq, OpCode::GraphPath, path_fields("t3")).await;
        assert_eq!(
            got_capped.status, capped.status,
            "node {idx}: a capped native path answers as the single node does: {got_capped:?}"
        );
        for ((from, to), expected) in absent_ends.iter().zip(&expected_absent) {
            seq += 1;
            let got = send_request(
                &mut clustered,
                seq,
                OpCode::GraphPath,
                path_between(from, to),
            )
            .await;
            assert_eq!(
                &got.status, expected,
                "node {idx}: a path from {from} to {to} answers an absent endpoint as the \
                 single node does: {got:?}"
            );
        }
    }

    cluster.shutdown().await;
}
