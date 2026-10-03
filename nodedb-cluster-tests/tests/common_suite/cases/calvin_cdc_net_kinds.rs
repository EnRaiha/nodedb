// SPDX-License-Identifier: BUSL-1.1

//! A Calvin commit publishes each row's net kind on the Control-Plane change
//! stream of every node.
//!
//! - A transaction that writes two collections on distinct vShards commits
//!   through Calvin. Several writes to one row publish one event:
//!   insert-then-update publishes `Insert`, an update publishes `Update`,
//!   update-then-delete publishes `Delete`, and insert-then-delete publishes
//!   nothing.
//! - An autocommit UPDATE that moves an implicit edge to another vShard
//!   commits through Calvin, and publishes `Update`.
//! - A columnar collection and an array publish one `*` event per kind of
//!   net change a Calvin commit makes to them.
//!
//! Each node's feed is read by replay, so the check does not depend on when a
//! subscription opened. Events of different partitions have no fixed order,
//! so each check compares the set of `(row, operation)` a collection holds.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nodedb::control::change_stream::ReplayStart;
use nodedb_types::{DatabaseId, TenantId};

use super::calvin_multishard_fixture::{Fixture, keyed_ddl, schemaless_ddl};
use super::vshard_names::{distinct_vshard_collections, key_on_other_vshard};
use crate::common::cluster_harness::TestClusterNode;

const ARRIVAL: Duration = Duration::from_secs(20);

/// Every `(row, operation)` `node`'s feed holds for `collection`.
fn feed(node: &TestClusterNode, collection: &str) -> BTreeSet<(String, &'static str)> {
    node.shared
        .change_stream
        .query_changes_in_database(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            Some(collection),
            ReplayStart::Timestamp(0),
            1024,
        )
        .unwrap_or_else(|e| panic!("node {}: replay refused: {e:?}", node.node_id))
        .events
        .iter()
        .map(|change| {
            (
                change.document_id.as_str().to_owned(),
                change.operation.as_str(),
            )
        })
        .collect()
}

/// Wait until every node's feed of `collection` holds `expected`, then
/// check that it holds nothing more.
async fn expect_feeds(fx: &Fixture, collection: &str, expected: &BTreeSet<(String, &'static str)>) {
    let deadline = Instant::now() + ARRIVAL;
    while fx
        .cluster
        .nodes
        .iter()
        .any(|node| !feed(node, collection).is_superset(expected))
    {
        assert!(
            Instant::now() < deadline,
            "every node's feed of {collection} holds {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A duplicate or a stray event is published in the same apply round.
    tokio::time::sleep(Duration::from_secs(1)).await;
    for node in &fx.cluster.nodes {
        assert_eq!(
            &feed(node, collection),
            expected,
            "node {}: {collection} publishes each row's net kind once",
            node.node_id
        );
    }
}

fn rows(entries: &[(&str, &'static str)]) -> BTreeSet<(String, &'static str)> {
    entries
        .iter()
        .map(|(row, op)| ((*row).to_owned(), *op))
        .collect()
}

async fn run(fx: &Fixture, sql: &str) {
    fx.coordinator()
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_calvin_transaction_publishes_net_kinds() {
    let (left, right) = distinct_vshard_collections("calvin_net_left", "calvin_net_right");
    let fx = Fixture::spawn(&[keyed_ddl(&left), keyed_ddl(&right)]).await;
    fx.wait_group_mounted(&left).await;
    fx.wait_group_mounted(&right).await;

    for id in ["u1", "d1"] {
        run(
            &fx,
            &format!("INSERT INTO {left} (id, v) VALUES ('{id}', 'seed')"),
        )
        .await;
    }

    // A transaction that spans vShards commits through Calvin.
    run(&fx, "SET cross_shard_txn = 'strict'").await;
    run(&fx, "BEGIN").await;
    for sql in [
        format!("INSERT INTO {right} (id, v) VALUES ('n1', 'new')"),
        format!("UPDATE {right} SET v = 'changed' WHERE id = 'n1'"),
        format!("UPDATE {left} SET v = 'changed' WHERE id = 'u1'"),
        format!("UPDATE {left} SET v = 'changed' WHERE id = 'd1'"),
        format!("DELETE FROM {left} WHERE id = 'd1'"),
        format!("INSERT INTO {left} (id, v) VALUES ('x1', 'new')"),
        format!("DELETE FROM {left} WHERE id = 'x1'"),
    ] {
        run(&fx, &sql).await;
    }
    run(&fx, "COMMIT").await;
    fx.converge().await;

    expect_feeds(
        &fx,
        &left,
        &rows(&[
            ("u1", "INSERT"),
            ("d1", "INSERT"),
            ("u1", "UPDATE"),
            ("d1", "DELETE"),
        ]),
    )
    .await;
    expect_feeds(&fx, &right, &rows(&[("n1", "INSERT")])).await;

    fx.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_calvin_routed_autocommit_update_publishes_update() {
    let coll = "calvin_net_edges";
    let fx = Fixture::spawn(&[schemaless_ddl(coll)]).await;
    fx.wait_group_mounted(coll).await;

    let src = key_on_other_vshard(coll, "src_e1");
    let old_hub = key_on_other_vshard(coll, "hub_old");
    let new_hub = key_on_other_vshard(coll, "hub_new");
    run(
        &fx,
        &format!(
            "INSERT INTO {coll} \
             {{ id: 'e1', _from: '{src}', _to: '{old_hub}', _type: 'l', mark: 'move' }}"
        ),
    )
    .await;
    run(
        &fx,
        &format!("UPDATE {coll} SET _to = '{new_hub}' WHERE id = 'e1'"),
    )
    .await;
    fx.converge().await;

    expect_feeds(&fx, coll, &rows(&[("e1", "INSERT"), ("e1", "UPDATE")])).await;

    fx.cluster.shutdown().await;
}

/// How many events of each `(row, operation)` `node`'s feed holds for
/// `collection`. A whole-collection event names every row as `*`, so the
/// count tells two inserts apart.
fn feed_counts(
    node: &TestClusterNode,
    collection: &str,
) -> std::collections::BTreeMap<(String, &'static str), usize> {
    let mut counts = std::collections::BTreeMap::new();
    for change in node
        .shared
        .change_stream
        .query_changes_in_database(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            Some(collection),
            ReplayStart::Timestamp(0),
            1024,
        )
        .unwrap_or_else(|e| panic!("node {}: replay refused: {e:?}", node.node_id))
        .events
    {
        *counts
            .entry((
                change.document_id.as_str().to_owned(),
                change.operation.as_str(),
            ))
            .or_insert(0) += 1;
    }
    counts
}

/// Wait until every node's feed of `collection` holds exactly `expected`
/// events of each kind.
async fn expect_feed_counts(
    fx: &Fixture,
    collection: &str,
    expected: &std::collections::BTreeMap<(String, &'static str), usize>,
) {
    let deadline = Instant::now() + ARRIVAL;
    while fx
        .cluster
        .nodes
        .iter()
        .any(|node| &feed_counts(node, collection) != expected)
    {
        assert!(
            Instant::now() < deadline,
            "every node's feed of {collection} holds {expected:?}; node 0 holds {:?}",
            feed_counts(&fx.cluster.nodes[0], collection)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // A duplicate or a stray event is published in the same apply round.
    tokio::time::sleep(Duration::from_secs(1)).await;
    for node in &fx.cluster.nodes {
        assert_eq!(
            &feed_counts(node, collection),
            expected,
            "node {}: {collection} publishes each net change once",
            node.node_id
        );
    }
}

/// Run `sql` on the coordinator until it is accepted: a statement on a
/// freshly created object waits for the object's group to serve it.
async fn run_until_accepted(fx: &Fixture, sql: &str) {
    let deadline = Instant::now() + ARRIVAL;
    loop {
        match fx.coordinator().client.simple_query(sql).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(%error, sql, "statement not accepted yet; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("{sql}: {error:?}"),
        }
    }
}

fn counts(
    entries: &[((&str, &'static str), usize)],
) -> std::collections::BTreeMap<(String, &'static str), usize> {
    entries
        .iter()
        .map(|((row, op), count)| (((*row).to_owned(), *op), *count))
        .collect()
}

/// A columnar collection publishes one `*` event per kind of net change a
/// Calvin commit makes to it: the seed row's insert, then the
/// transaction's update of it and its insert of a new row. The row it
/// inserted and deleted adds no kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_calvin_transaction_publishes_columnar_net_kinds() {
    let (columnar, side) = distinct_vshard_collections("calvin_net_cols", "calvin_net_cols_side");
    let fx = Fixture::spawn(&[
        format!(
            "CREATE COLLECTION {columnar} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='columnar')"
        ),
        keyed_ddl(&side),
    ])
    .await;
    fx.wait_group_mounted(&columnar).await;
    fx.wait_group_mounted(&side).await;
    run(
        &fx,
        &format!("INSERT INTO {columnar} (id, v) VALUES ('u1', 'seed')"),
    )
    .await;

    run(&fx, "SET cross_shard_txn = 'strict'").await;
    run(&fx, "BEGIN").await;
    for sql in [
        format!("UPDATE {columnar} SET v = 'changed' WHERE id = 'u1'"),
        format!("INSERT INTO {columnar} (id, v) VALUES ('n1', 'new')"),
        format!("INSERT INTO {columnar} (id, v) VALUES ('x1', 'new')"),
        format!("DELETE FROM {columnar} WHERE id = 'x1'"),
        format!("INSERT INTO {side} (id, v) VALUES ('s1', 'side')"),
    ] {
        run(&fx, &sql).await;
    }
    run(&fx, "COMMIT").await;
    fx.converge().await;

    expect_feed_counts(
        &fx,
        &columnar,
        &counts(&[(("*", "INSERT"), 2), (("*", "UPDATE"), 1)]),
    )
    .await;

    fx.cluster.shutdown().await;
}

/// An array publishes one `*` event per kind of net change a Calvin commit
/// makes to its cells: the seed cell's insert, then the transaction's update
/// of that cell and its insert of a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_calvin_transaction_publishes_array_net_kinds() {
    let grid = "calvin_net_grid";
    let side = "calvin_net_grid_side";
    let fx = Fixture::spawn(&[keyed_ddl(side)]).await;
    fx.wait_group_mounted(side).await;
    fx.cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE ARRAY {grid} DIMS (x INT64 [0..63], y INT64 [0..63]) \
             ATTRS (v INT64) TILE_EXTENTS (8, 8)"
        ))
        .await
        .expect("CREATE ARRAY");
    run_until_accepted(
        &fx,
        &format!("INSERT INTO ARRAY {grid} COORDS (1, 1) VALUES (5)"),
    )
    .await;

    run(&fx, "SET cross_shard_txn = 'strict'").await;
    run(&fx, "BEGIN").await;
    for sql in [
        format!("INSERT INTO ARRAY {grid} COORDS (1, 1) VALUES (6)"),
        format!("INSERT INTO ARRAY {grid} COORDS (40, 40) VALUES (7)"),
        format!("INSERT INTO {side} (id, v) VALUES ('s1', 'side')"),
    ] {
        run(&fx, &sql).await;
    }
    run(&fx, "COMMIT").await;
    fx.converge().await;

    expect_feed_counts(
        &fx,
        grid,
        &counts(&[(("*", "INSERT"), 2), (("*", "UPDATE"), 1)]),
    )
    .await;

    fx.cluster.shutdown().await;
}
