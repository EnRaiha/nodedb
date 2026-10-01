// SPDX-License-Identifier: BUSL-1.1

//! Two sessions on different nodes give a new timeseries column conflicting
//! types at the same time. The cluster stays up and every replica agrees.
//!
//! Each proposer resolves its ingest against its own replica. Both can
//! resolve before either entry applies, so both entries can reach the log
//! with the column under different types. The log orders them. The later
//! entry's conflicting rows are rejected at its log position on every
//! replica, since every replica holds the same schema there. Its client
//! learns the count as a warning. When the later ingest resolves after the
//! earlier one applied, its resolve rejects the same rows instead, with the
//! same warning.
//!
//! ## Test shape
//!
//! Bring up 3 nodes. Create a timeseries collection that declares only its
//! `ts` time key, with a change stream on it. For each round, open a native
//! session on node 0 and one on node 1, and ingest at once two raw ILP lines
//! each that give a fresh column a float from node 0 and a string from
//! node 1. A fresh column comes only from a raw ILP line (see
//! `ts_native_ingest`). Then:
//!
//! - no node fail-stopped a core;
//! - in each round exactly one of the two ingests reports its two lines
//!   rejected, and the other reports none;
//! - every replica, read from its own Data Plane, holds the same rows: two
//!   per round, plus the warm-up rows;
//! - every replica's change stream serves one event per stored row.

use crate::common;
use common::cluster_harness::shared_steps::{fail_stopped, local_timeseries_rows};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::time::{Duration, Instant};

use nodedb::event::cdc::consume::{ConsumeError, ConsumeParams, consume_local};
use nodedb_types::DatabaseId;

use super::ts_native_ingest::{
    assert_native_ok, ingest_native, ingest_until_accepted, native_session, rejection_warnings,
};

const COLLECTION: &str = "ts_conflicting_types";
const STREAM: &str = "ts_conflicting_types_feed";
const GROUP: &str = "ts_conflicting_types_readers";
const TENANT: u64 = 1;

/// Rounds of concurrent conflicting inserts, each on a fresh column.
const ROUNDS: usize = 8;
/// Rows each insert carries.
const ROWS_PER_INSERT: usize = 2;
/// Rows the warm-up inserts store, one per session node.
const WARM_UP_ROWS: usize = 2;
const TOTAL_ROWS: usize = WARM_UP_ROWS + ROUNDS * ROWS_PER_INSERT;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// The ILP lines of `round` from session `session`: two lines that give
/// column `c{round}` a float from session 0 and a string from session 1.
/// Every line has its own timestamp, in nanoseconds.
fn round_lines(round: usize, session: usize) -> String {
    let lines: Vec<String> = (0..ROWS_PER_INSERT)
        .map(|row| {
            let ts_ms = 10_000 * (round as u64 + 1) + 100 * session as u64 + row as u64;
            let column_value = if session == 0 {
                format!("{}.5", round + row)
            } else {
                format!("\"s{round}-{row}\"")
            };
            format!(
                "{COLLECTION} value={value},c{round}={column_value} {ts_ns}",
                value = row as f64 + 0.25,
                ts_ns = ts_ms * 1_000_000
            )
        })
        .collect();
    lines.join("\n")
}

/// Whether some core of `node` fail-stopped.
/// The rows `node`'s own replica stores, each rendered canonically, sorted.
/// The number of change events `node` serves after the group's committed
/// offsets.
fn event_count(node: &TestClusterNode) -> usize {
    let params = ConsumeParams {
        database_id: DatabaseId::DEFAULT,
        tenant_id: TENANT,
        stream_name: STREAM,
        group_name: GROUP,
        partition: None,
        limit: 10 * TOTAL_ROWS,
    };
    match consume_local(&node.shared, &params) {
        Ok(result) => result.events.len(),
        Err(ConsumeError::BufferEmpty(_)) => 0,
        Err(error) => panic!("node {}: consume failed: {error}", node.node_id),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_conflicting_types_stay_up_and_agree_on_every_replica() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (ts BIGINT TIME_KEY) WITH (engine='timeseries')"
        ))
        .await
        .expect("create the timeseries collection");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE CHANGE STREAM {STREAM} ON {COLLECTION}"))
        .await
        .expect("create the change stream");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE CONSUMER GROUP {GROUP} ON {STREAM}"))
        .await
        .expect("create the consumer group");
    wait_for(
        "every node registers the stream and the group",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                node.has_change_stream(DatabaseId::DEFAULT, TENANT, STREAM)
                    && node
                        .shared
                        .group_registry
                        .get(DatabaseId::DEFAULT, TENANT, STREAM, GROUP)
                        .is_some()
            })
        },
    )
    .await;

    // Both session nodes accept writes before the rounds start.
    for (session, node) in cluster.nodes.iter().take(2).enumerate() {
        ingest_until_accepted(
            node,
            COLLECTION,
            &format!(
                "{COLLECTION} value=0.5 {}",
                (session as u64 + 1) * 1_000_000
            ),
        )
        .await;
    }

    for round in 0..ROUNDS {
        let mut float_session = native_session(&cluster.nodes[0]).await;
        let mut string_session = native_session(&cluster.nodes[1]).await;
        let float_lines = round_lines(round, 0);
        let string_lines = round_lines(round, 1);
        let (float_reply, string_reply) = tokio::join!(
            ingest_native(&mut float_session, 1, COLLECTION, &float_lines),
            ingest_native(&mut string_session, 1, COLLECTION, &string_lines),
        );
        assert_native_ok(&float_reply, "the float ingest");
        assert_native_ok(&string_reply, "the string ingest");
        let float_notices = rejection_warnings(&float_reply, COLLECTION);
        let string_notices = rejection_warnings(&string_reply, COLLECTION);
        let expected = format!("{ROWS_PER_INSERT} line(s)");
        let reported: Vec<&String> = float_notices.iter().chain(&string_notices).collect();
        assert_eq!(
            reported.len(),
            1,
            "round {round}: exactly the later statement reports its rows rejected; \
             float session {float_notices:?}, string session {string_notices:?}"
        );
        assert!(
            reported.iter().all(|notice| notice.contains(&expected)),
            "round {round}: the later statement reports all {ROWS_PER_INSERT} rows \
             rejected, got {reported:?}"
        );
    }
    cluster.wait_for_full_apply_convergence(CONVERGE).await;

    for node in &cluster.nodes {
        assert!(
            !fail_stopped(node),
            "node {} fail-stopped a core on a type conflict",
            node.node_id
        );
    }

    // Every replica stores the same rows: the warm-up rows and the earlier
    // insert of each round.
    let deadline = Instant::now() + CONVERGE;
    let mut per_node: Vec<(u64, Vec<String>)> = Vec::new();
    while Instant::now() < deadline {
        per_node.clear();
        for node in &cluster.nodes {
            per_node.push((
                node.node_id,
                local_timeseries_rows(node, TENANT, COLLECTION).await,
            ));
        }
        if per_node.iter().all(|(_, rows)| rows.len() == TOTAL_ROWS) {
            break;
        }
        tokio::time::sleep(STEP).await;
    }
    for (node_id, rows) in &per_node {
        assert_eq!(
            rows.len(),
            TOTAL_ROWS,
            "node {node_id} stores {} of {TOTAL_ROWS} rows",
            rows.len()
        );
    }
    let (reference_node, reference) = &per_node[0];
    for (node_id, rows) in &per_node[1..] {
        assert_eq!(
            rows, reference,
            "node {node_id} stores other rows than node {reference_node}"
        );
    }

    // Every replica serves one event per stored row, and none for a
    // rejected row.
    wait_for(
        "every replica serves one event per stored row",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| event_count(node) == TOTAL_ROWS)
        },
    )
    .await;
    for node in &cluster.nodes {
        assert_eq!(
            event_count(node),
            TOTAL_ROWS,
            "node {} serves another event count than it stores rows",
            node.node_id
        );
        assert!(
            !fail_stopped(node),
            "node {} fail-stopped a core",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
