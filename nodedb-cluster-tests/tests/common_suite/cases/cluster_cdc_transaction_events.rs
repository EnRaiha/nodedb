// SPDX-License-Identifier: BUSL-1.1

//! A committed transaction publishes its rows on the Control-Plane change
//! stream of every node, once each, at the commit's position. A rolled-back
//! transaction publishes nothing.
//!
//! ## Test shape
//!
//! Bring up 3 nodes and subscribe on all of them. Through node 0, commit one
//! transaction (INSERT, INSERT, UPDATE), roll back another, then write one
//! autocommit sentinel row. Every node must yield the two committed rows at
//! one shared position, then the sentinel: nothing of the rolled-back
//! transaction and no duplicate arrives in between. A replay on the last
//! node, from the start of its feed, must serve the committed rows too.

use crate::common;
use common::cluster_harness::TestCluster;

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb::control::change_stream::{ReplayStart, SequencedChangeEvent, Subscription};
use nodedb_types::{DatabaseId, TenantId};

const COLLECTION: &str = "cdc_txn";

/// How long to wait for an event to reach a node.
const ARRIVAL_TIMEOUT: Duration = Duration::from_secs(10);

/// The next event `sub` receives on node `idx`, or a test error.
async fn next_event(sub: &mut Subscription, idx: usize, what: &str) -> SequencedChangeEvent {
    match tokio::time::timeout(ARRIVAL_TIMEOUT, sub.recv_sequenced()).await {
        Ok(Ok(event)) => event,
        Ok(Err(e)) => panic!("node {idx}: change stream closed while awaiting {what}: {e}"),
        Err(_) => panic!("node {idx}: no change event for {what}"),
    }
}

/// Run `sql` on node 0's session, failing the test on error.
async fn exec(cluster: &TestCluster, sql: &str) {
    cluster.nodes[0]
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_transaction_publishes_once_on_every_node() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {COLLECTION}: {e}"));

    // Subscribe on every node BEFORE the writes: the bus replays nothing to
    // a receiver that did not yet exist.
    let mut subs: Vec<Subscription> = cluster
        .nodes
        .iter()
        .map(|node| {
            node.shared
                .change_stream
                .subscribe(Some(COLLECTION.to_string()), None)
        })
        .collect();

    exec(&cluster, "BEGIN").await;
    exec(
        &cluster,
        &format!("INSERT INTO {COLLECTION} (id, payload) VALUES ('t-1', 'new')"),
    )
    .await;
    exec(
        &cluster,
        &format!("INSERT INTO {COLLECTION} (id, payload) VALUES ('t-2', 'new')"),
    )
    .await;
    exec(
        &cluster,
        &format!("UPDATE {COLLECTION} SET payload = 'updated' WHERE id = 't-1'"),
    )
    .await;
    exec(&cluster, "COMMIT").await;

    exec(&cluster, "BEGIN").await;
    exec(
        &cluster,
        &format!("INSERT INTO {COLLECTION} (id, payload) VALUES ('r-1', 'rolled back')"),
    )
    .await;
    exec(&cluster, "ROLLBACK").await;

    exec(
        &cluster,
        &format!("INSERT INTO {COLLECTION} (id, payload) VALUES ('s-1', 'sentinel')"),
    )
    .await;

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let committed = BTreeSet::from(["t-1".to_owned(), "t-2".to_owned()]);
    let mut commit_positions = Vec::new();
    for (idx, sub) in subs.iter_mut().enumerate() {
        let first = next_event(sub, idx, "the first committed row").await;
        let second = next_event(sub, idx, "the second committed row").await;
        let rows: BTreeSet<String> = [&first, &second]
            .iter()
            .map(|event| event.document_id.as_str().to_owned())
            .collect();
        assert_eq!(
            rows, committed,
            "node {idx}: the commit publishes each row it wrote once"
        );
        assert_eq!(
            (
                first.partition(),
                first.position().epoch,
                first.position().index
            ),
            (
                second.partition(),
                second.position().epoch,
                second.position().index
            ),
            "node {idx}: every row of the commit publishes at the commit's position"
        );
        commit_positions.push((first.partition(), first.position()));

        let sentinel = next_event(sub, idx, "the sentinel").await;
        assert_eq!(
            sentinel.document_id.as_str(),
            "s-1",
            "node {idx}: an event of the rolled-back transaction, or a duplicate, \
             arrived before the sentinel"
        );
    }
    assert!(
        commit_positions.windows(2).all(|pair| pair[0] == pair[1]),
        "every node gives the commit one shared position: {commit_positions:?}"
    );

    // A cursor on another node sees the committed rows.
    let replayed = cluster.nodes[2]
        .shared
        .change_stream
        .query_changes_in_database(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            Some(COLLECTION),
            ReplayStart::Timestamp(0),
            16,
        )
        .unwrap_or_else(|e| panic!("node 2: replay refused: {e:?}"));
    let replayed_rows: Vec<String> = replayed
        .events
        .iter()
        .map(|change| change.document_id.as_str().to_owned())
        .collect();
    let mut committed_replayed: Vec<String> = replayed_rows
        .iter()
        .filter(|row| row.starts_with("t-"))
        .cloned()
        .collect();
    committed_replayed.sort();
    assert_eq!(
        committed_replayed,
        vec!["t-1".to_owned(), "t-2".to_owned()],
        "node 2's feed serves each committed row once: {replayed_rows:?}"
    );
    assert!(
        !replayed_rows.iter().any(|row| row == "r-1"),
        "node 2's feed serves a rolled-back row: {replayed_rows:?}"
    );

    drop(subs);
    cluster.shutdown().await;
}
