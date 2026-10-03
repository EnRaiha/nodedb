// SPDX-License-Identifier: BUSL-1.1

//! A stored-procedure body in a cluster writes an array and reads it back in
//! the same body.
//!
//! The body runs as one system transaction. Its `INSERT INTO ARRAY` stages a
//! per-shard write. Its `ARRAY_AGG` and `ARRAY_SLICE` are cluster array
//! reads, which the transaction dispatches through this node's array
//! coordinator, carrying the transaction id so each shard folds in the
//! transaction's staged cells. A cluster array read has no Data-Plane
//! handler: dispatched to a core, it panics there and fails the statement.
//!
//! The test checks:
//! - `CALL` succeeds, so each body read ran through the coordinator;
//! - no node fail-stopped a core;
//! - both writes commit with the body, and every node reads them.
//!
//! A body statement's rows are discarded and a procedural expression cannot
//! query, so no body can report what its read returned. The body read asks
//! for the same staged-cell fold a client transaction's cluster array read
//! uses, which the second test checks: inside `BEGIN`, `ARRAY_AGG` and
//! `ARRAY_SLICE` see the transaction's staged cells on every shard they
//! span, another connection sees none of them, and after `COMMIT` both see
//! all of them.

use crate::common;

use common::cluster_harness::shared_steps::fail_stopped;
use common::cluster_harness::{TestCluster, TestClusterNode};

const CREATE_ARRAY: &str = "CREATE ARRAY bodygrid \
     DIMS (x INT64 [0..63], y INT64 [0..63]) \
     ATTRS (v FLOAT64) \
     TILE_EXTENTS (8, 8) \
     CELL_ORDER HILBERT";

/// Writes a cell, reads the array twice with that cell staged, then writes a
/// cell on a distant tile.
const CREATE_PROCEDURE: &str = "CREATE PROCEDURE fill_and_read_bodygrid() AS \
     BEGIN \
       INSERT INTO ARRAY bodygrid COORDS (1, 1) VALUES (1.5); \
       SELECT * FROM ARRAY_AGG('bodygrid', 'v', 'sum'); \
       SELECT * FROM ARRAY_SLICE('bodygrid', '{\"x\":[0,63],\"y\":[0,63]}', '*', 100); \
       INSERT INTO ARRAY bodygrid COORDS (60, 60) VALUES (2.5); \
     END";

const EXPECTED_SUM: f64 = 4.0;

/// Every row `sql` returns on `client`.
async fn rows_of(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Vec<tokio_postgres::SimpleQueryRow> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

/// The sum `ARRAY_AGG` reports over `array`'s `v` attribute on `client`.
/// `None` when the aggregate has no row or an empty result, as it has over
/// no cells.
async fn agg_sum(client: &tokio_postgres::Client, array: &str) -> Option<f64> {
    let rows = rows_of(
        client,
        &format!("SELECT * FROM ARRAY_AGG('{array}', 'v', 'sum')"),
    )
    .await;
    assert!(
        rows.len() <= 1,
        "a scalar ARRAY_AGG returns at most one row"
    );
    let text = rows.first()?.get("result")?;
    if text.is_empty() {
        return None;
    }
    Some(
        text.parse()
            .unwrap_or_else(|e| panic!("ARRAY_AGG result {text} is not a float: {e}")),
    )
}

/// How many cells an `ARRAY_SLICE` over the whole of `array` returns on
/// `client`.
async fn slice_count(client: &tokio_postgres::Client, array: &str) -> usize {
    rows_of(
        client,
        &format!("SELECT * FROM ARRAY_SLICE('{array}', '{{\"x\":[0,63],\"y\":[0,63]}}', '*', 100)"),
    )
    .await
    .len()
}

/// The `result` column of a one-row `ARRAY_AGG` sum over pgwire.
async fn pgwire_sum(node: &TestClusterNode) -> f64 {
    agg_sum(&node.client, "bodygrid")
        .await
        .unwrap_or_else(|| panic!("node {}: ARRAY_AGG over bodygrid is empty", node.node_id))
}

/// A second pgwire connection to `node`, as the harness superuser.
async fn second_connection(node: &TestClusterNode) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            node.pg_addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap_or_else(|e| panic!("second connection to node {}: {e:?}", node.node_id));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_procedure_body_reads_the_array_it_writes() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(CREATE_ARRAY)
        .await
        .expect("CREATE ARRAY bodygrid");
    let caller_idx = cluster
        .exec_ddl_on_any_leader(CREATE_PROCEDURE)
        .await
        .expect("CREATE PROCEDURE fill_and_read_bodygrid");
    let caller = &cluster.nodes[caller_idx];

    caller
        .client
        .simple_query("CALL fill_and_read_bodygrid()")
        .await
        .unwrap_or_else(|e| {
            panic!(
                "CALL on node {}: a body's cluster array read must run through \
                 the array coordinator: {e:?}",
                caller.node_id
            )
        });

    for node in &cluster.nodes {
        assert!(
            !fail_stopped(node),
            "node {} fail-stopped a core",
            node.node_id
        );
    }

    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;
    for node in &cluster.nodes {
        let sum = pgwire_sum(node).await;
        assert!(
            (sum - EXPECTED_SUM).abs() < 1e-9,
            "node {} reads both cells the body committed: sum {sum}",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

/// A cross-shard Calvin transaction that writes array cells on distant tiles
/// and a document row commits, and every node reads the committed cells.
///
/// Each staged cell write homes to the vShard its tile hashes to, and the
/// transaction routes and locks it there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_calvin_transaction_commits_array_cells_on_their_tiles() {
    const ARRAY: &str = "calvingrid";
    const SIDE: &str = "calvingrid_side";
    const COMMITTED_SUM: f64 = 9.0;
    const COMMITTED_CELLS: usize = 3;

    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    let ddl_idx = cluster
        .exec_ddl_on_any_leader(&CREATE_ARRAY.replace("bodygrid", ARRAY))
        .await
        .expect("CREATE ARRAY calvingrid");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {SIDE}"))
        .await
        .expect("CREATE COLLECTION calvingrid_side");
    let node = &cluster.nodes[(ddl_idx + 1) % cluster.nodes.len()];
    let client: &tokio_postgres::Client = &node.client;

    // The side collection's first write retries until this node serves it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match client
            .simple_query(&format!("INSERT INTO {SIDE} (id, v) VALUES ('seed', 'x')"))
            .await
        {
            Ok(_) => break,
            Err(e) if std::time::Instant::now() < deadline => {
                tracing::debug!(error = %e, "side collection not served yet; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => panic!("seed the side collection: {e:?}"),
        }
    }

    client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn");
    client.simple_query("BEGIN").await.expect("BEGIN");
    // Cells on distant tiles, so they stage on several shards.
    client
        .simple_query(&format!(
            "INSERT INTO ARRAY {ARRAY} \
             COORDS (1, 1) VALUES (2.0), \
             COORDS (40, 3) VALUES (3.0), \
             COORDS (60, 60) VALUES (4.0)"
        ))
        .await
        .unwrap_or_else(|e| panic!("staged INSERT INTO ARRAY: {e:?}"));
    client
        .simple_query(&format!("INSERT INTO {SIDE} (id, v) VALUES ('s1', 'side')"))
        .await
        .unwrap_or_else(|e| panic!("staged side insert: {e:?}"));
    client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| panic!("COMMIT of the array transaction: {e:?}"));

    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;
    for reader in &cluster.nodes {
        assert!(
            !fail_stopped(reader),
            "node {} fail-stopped a core",
            reader.node_id
        );
        assert_eq!(
            agg_sum(&reader.client, ARRAY).await,
            Some(COMMITTED_SUM),
            "node {}: ARRAY_AGG sees every committed cell",
            reader.node_id
        );
        assert_eq!(
            slice_count(&reader.client, ARRAY).await,
            COMMITTED_CELLS,
            "node {}: ARRAY_SLICE returns every committed cell",
            reader.node_id
        );
    }

    cluster.shutdown().await;
}

/// A client transaction's cluster array reads see its own staged cells, on
/// every shard the cells span. Another connection sees none of them until
/// COMMIT, and then both see all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_transaction_reads_its_own_staged_array_cells() {
    const ARRAY: &str = "txngrid";
    const STAGED_SUM: f64 = 7.0;
    const STAGED_CELLS: usize = 3;

    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    let ddl_idx = cluster
        .exec_ddl_on_any_leader(&CREATE_ARRAY.replace("bodygrid", ARRAY))
        .await
        .expect("CREATE ARRAY txngrid");
    let node = &cluster.nodes[(ddl_idx + 1) % cluster.nodes.len()];
    let txn: &tokio_postgres::Client = &node.client;
    let other = second_connection(node).await;

    txn.simple_query("BEGIN").await.expect("BEGIN");
    // Cells on distant tiles, so they stage on several shards.
    txn.simple_query(&format!(
        "INSERT INTO ARRAY {ARRAY} \
         COORDS (1, 1) VALUES (1.5), \
         COORDS (40, 3) VALUES (2.5), \
         COORDS (60, 60) VALUES (3.0)"
    ))
    .await
    .unwrap_or_else(|e| panic!("staged INSERT INTO ARRAY: {e:?}"));

    assert_eq!(
        agg_sum(txn, ARRAY).await,
        Some(STAGED_SUM),
        "ARRAY_AGG inside the transaction sums its staged cells"
    );
    assert_eq!(
        slice_count(txn, ARRAY).await,
        STAGED_CELLS,
        "ARRAY_SLICE inside the transaction returns its staged cells"
    );

    let outside_sum = agg_sum(&other, ARRAY).await;
    assert!(
        outside_sum.is_none_or(|sum| sum == 0.0),
        "another connection's ARRAY_AGG sees no staged cell: {outside_sum:?}"
    );
    assert_eq!(
        slice_count(&other, ARRAY).await,
        0,
        "another connection's ARRAY_SLICE sees no staged cell"
    );

    txn.simple_query("COMMIT").await.expect("COMMIT");
    cluster
        .wait_for_full_apply_convergence(std::time::Duration::from_secs(10))
        .await;

    for (label, client) in [("the committing", txn), ("the other", &other)] {
        assert_eq!(
            agg_sum(client, ARRAY).await,
            Some(STAGED_SUM),
            "{label} connection's ARRAY_AGG sees the committed cells"
        );
        assert_eq!(
            slice_count(client, ARRAY).await,
            STAGED_CELLS,
            "{label} connection's ARRAY_SLICE sees the committed cells"
        );
    }

    cluster.shutdown().await;
}
