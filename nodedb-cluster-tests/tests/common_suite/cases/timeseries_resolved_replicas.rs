// SPDX-License-Identifier: BUSL-1.1

//! A timeseries ingest the leader accepts lands identically on every replica.
//!
//! The proposer resolves each ingest to its rows before the entry exists.
//! Every replica then installs exactly the rows the entry carries. A replica
//! whose memtable budget is far below the others flushes around every
//! record, and still stores every row: a memory limit at apply makes room
//! first and never drops a row.
//!
//! ## Test shape
//!
//! Bring up 3 nodes, node 3 with a memtable budget of a few hundred bytes.
//! Create a timeseries collection with a change stream on it. Through node
//! 0, insert several multi-row batches. Then, on every node:
//!
//! - its own replica, read from its own Data Plane, holds every row, with
//!   the same values as every other replica;
//! - its change stream serves one event per row, at the same positions as
//!   every other replica.

use crate::common;
use common::cluster_harness::shared_steps::local_timeseries_rows;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use nodedb::event::cdc::CdcOffset;
use nodedb::event::cdc::consume::{ConsumeError, ConsumeParams, consume_local};
use nodedb_types::DatabaseId;
use nodedb_types::config::tuning::TimeseriesToning;

const COLLECTION: &str = "ts_resolved_replicas";
const STREAM: &str = "ts_resolved_replicas_feed";
const GROUP: &str = "ts_resolved_replicas_readers";
const TENANT: u64 = 1;

/// The node whose memtable budget is set low.
const LOW_BUDGET_NODE: u64 = 3;
/// Insert statements, each one ingest.
const BATCHES: usize = 5;
/// Rows per insert statement.
const ROWS_PER_BATCH: usize = 20;
const TOTAL_ROWS: usize = BATCHES * ROWS_PER_BATCH;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// A budget every batch passes, so node 3 flushes around every record.
fn low_budget() -> TimeseriesToning {
    TimeseriesToning {
        memtable_budget_bytes: 256,
        memtable_hard_limit_bytes: 512,
        ..TimeseriesToning::default()
    }
}

/// The `INSERT` of batch `batch`: `ROWS_PER_BATCH` rows with distinct
/// timestamps, two hosts, and values that differ by row.
fn batch_insert(batch: usize) -> String {
    let rows: Vec<String> = (0..ROWS_PER_BATCH)
        .map(|row| {
            let n = batch * ROWS_PER_BATCH + row;
            let host = if n.is_multiple_of(2) { "alpha" } else { "beta" };
            format!(
                "('r-{n}', {ts}, '{host}', {value})",
                ts = 1_000 * (n as u64 + 1),
                value = n as f64 + 0.25
            )
        })
        .collect();
    format!(
        "INSERT INTO {COLLECTION} (id, ts, host, value) VALUES {}",
        rows.join(", ")
    )
}

/// Run `sql` through `node`, retrying while the cluster elects a leader.
async fn exec_retrying(node: &TestClusterNode, sql: &str) {
    let deadline = Instant::now() + CONVERGE;
    loop {
        match node.client.simple_query(sql).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(error = %pg_detail(&error), "statement not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("{sql}: {}", pg_detail(&error)),
        }
    }
}

/// The rows `node`'s own replica stores, each rendered canonically, sorted.
/// The change events `node` serves after the group's committed offsets, by
/// partition, each partition in the order it was served.
fn events(node: &TestClusterNode) -> BTreeMap<u32, Vec<(CdcOffset, String)>> {
    let params = ConsumeParams {
        database_id: DatabaseId::DEFAULT,
        tenant_id: TENANT,
        stream_name: STREAM,
        group_name: GROUP,
        partition: None,
        limit: 10 * TOTAL_ROWS,
    };
    let served = match consume_local(&node.shared, &params) {
        Ok(result) => result.events,
        Err(ConsumeError::BufferEmpty(_)) => Vec::new(),
        Err(error) => panic!("node {}: consume failed: {error}", node.node_id),
    };
    let mut out: BTreeMap<u32, Vec<(CdcOffset, String)>> = BTreeMap::new();
    for event in &served {
        out.entry(event.partition)
            .or_default()
            .push((event.position(), event.row_id.clone()));
    }
    out
}

fn event_count(events: &BTreeMap<u32, Vec<(CdcOffset, String)>>) -> usize {
    events.values().map(Vec::len).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_accepted_ingest_lands_identically_on_every_replica() {
    let cluster =
        TestCluster::spawn_three_with_node_timeseries_tuning(LOW_BUDGET_NODE, low_budget())
            .await
            .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
             WITH (engine='timeseries')"
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

    for batch in 0..BATCHES {
        exec_retrying(&cluster.nodes[0], &batch_insert(batch)).await;
    }
    cluster.wait_for_full_apply_convergence(CONVERGE).await;

    // Every replica, the low-budget one included, stores every row.
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
            "node {node_id} stores {} of {TOTAL_ROWS} rows; a replica that resolves or \
             drops rows on its own diverges",
            rows.len()
        );
    }
    let (reference_node, reference) = &per_node[0];
    for (node_id, rows) in &per_node[1..] {
        assert_eq!(
            rows, reference,
            "node {node_id} stores other values than node {reference_node}"
        );
    }

    // Every replica serves one event per row, at the same positions.
    wait_for(
        "every replica serves one event per row",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| event_count(&events(node)) == TOTAL_ROWS)
        },
    )
    .await;
    let reference_events = events(&cluster.nodes[0]);
    assert_eq!(event_count(&reference_events), TOTAL_ROWS);
    for node in &cluster.nodes[1..] {
        assert_eq!(
            events(node),
            reference_events,
            "node {} serves a different change sequence than node {}",
            node.node_id,
            cluster.nodes[0].node_id
        );
    }

    cluster.shutdown().await;
}
