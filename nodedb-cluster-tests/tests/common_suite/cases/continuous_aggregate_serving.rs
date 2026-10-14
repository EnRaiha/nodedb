// SPDX-License-Identifier: BUSL-1.1

//! A continuous aggregate read as a relation answers the same rows on every
//! node, and after its root source's leader dies.
//!
//! The aggregate is `Manual`. `REFRESH CONTINUOUS AGGREGATE` is a replicated
//! cut on the source's data group: every replica flushes the source at the
//! cut's log position and folds the same rows. A read on any node fetches
//! the finalized rows from the replica that owns the source's vShard. So a
//! read through a node that does not lead the source, and a read after the
//! leader is killed, both return the rows of the last cut.

use crate::common;
use common::cluster_harness::shared_steps::{db_detail, kill_node, leader_index_of};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};

use std::time::Duration;

const SRC: &str = "cagg_cluster_src";
const VIEW: &str = "cagg_cluster_view";

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// `(host, s, c)` rows of the aggregate read through `node`, or `None` while
/// the read fails. A failed read prints the server's error, so a timed-out
/// wait shows why the read never succeeded.
async fn view_rows(node: &TestClusterNode) -> Option<Vec<(String, String, String)>> {
    let messages = match node
        .client
        .simple_query(&format!("SELECT host, s, c FROM {VIEW} ORDER BY host"))
        .await
    {
        Ok(messages) => messages,
        Err(e) => {
            eprintln!("read {VIEW}: {}", db_detail(&e));
            return None;
        }
    };
    Some(
        messages
            .iter()
            .filter_map(|msg| match msg {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some((
                    row.get(0).unwrap_or_default().to_string(),
                    row.get(1).unwrap_or_default().to_string(),
                    row.get(2).unwrap_or_default().to_string(),
                )),
                _ => None,
            })
            .collect(),
    )
}

/// Whether `rows` are the buckets of the two seeded hosts.
fn are_seeded(rows: &[(String, String, String)]) -> bool {
    let num = |s: &str| s.parse::<f64>().ok();
    let seeded = rows.len() == 2
        && rows[0].0 == "a"
        && num(&rows[0].1) == Some(4.0)
        && rows[0].2 == "2"
        && rows[1].0 == "b"
        && num(&rows[1].1) == Some(30.0)
        && rows[1].2 == "2";
    if !seeded {
        eprintln!("read {VIEW}: unexpected rows {rows:?}");
    }
    seeded
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_aggregate_reads_from_source_leader() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {SRC} (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("create the source");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE CONTINUOUS AGGREGATE {VIEW} ON {SRC} BUCKET '1m' \
             AGGREGATE SUM(value) AS s, COUNT(*) AS c GROUP BY host \
             WITH (refresh_policy = 'manual')"
        ))
        .await
        .expect("create the aggregate");

    let leader = leader_index_of(&cluster, SRC);
    let reader = (leader + 1) % cluster.nodes.len();
    for (ts, host, value) in [
        (1_000, "a", 1.5),
        (2_000, "a", 2.5),
        (3_000, "b", 10.0),
        (4_000, "b", 20.0),
    ] {
        cluster.nodes[reader]
            .client
            .simple_query(&format!(
                "INSERT INTO {SRC} (ts, host, value) VALUES ({ts}, '{host}', {value})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row at {ts}: {}", db_detail(&e)));
    }
    cluster.nodes[reader]
        .client
        .simple_query(&format!("REFRESH CONTINUOUS AGGREGATE {VIEW}"))
        .await
        .unwrap_or_else(|e| panic!("refresh: {}", db_detail(&e)));

    let through_reader = &cluster.nodes[reader];
    wait_for_async(
        "a node that does not lead the source reads the refreshed buckets",
        CONVERGE,
        STEP,
        || async {
            view_rows(through_reader)
                .await
                .is_some_and(|rows| are_seeded(&rows))
        },
    )
    .await;

    kill_node(&mut cluster, leader).await;
    let survivor = &cluster.nodes[0];
    wait_for_async(
        "after the source's leader dies, the new leader serves the same cut",
        CONVERGE,
        STEP,
        || async {
            view_rows(survivor)
                .await
                .is_some_and(|rows| are_seeded(&rows))
        },
    )
    .await;

    cluster.shutdown().await;
}
