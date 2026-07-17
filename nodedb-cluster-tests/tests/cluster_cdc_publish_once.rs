// SPDX-License-Identifier: BUSL-1.1

//! A replicated SQL write must raise EXACTLY ONE Control-Plane change event
//! per subscriber.
//!
//! ## The bug this guards against
//!
//! A Raft-replicated write is submitted to the local Data Plane independently
//! by EVERY replica's apply loop. If the change event were published from that
//! apply site, a replication factor of N would publish N events AND fan out N
//! cluster-wide NOTIFY broadcasts, one from each replica. `deliver_remote_notify`
//! forwards every NOTIFY straight to local subscribers and there is no dedup on
//! either side, so each subscriber would silently see the same write repeated —
//! no error, no warning, just duplicated CDC. The event is therefore published
//! once, by the node that handled the write (`dispatch_replicated_write`), after
//! commit + apply; replicas publish nothing.
//!
//! ## Test shape
//!
//! Bring up 3 nodes, subscribe on ALL of them, do ONE INSERT through one node,
//! and assert every node's subscription yields exactly one event: one
//! `recv_filtered` succeeds and a second one TIMES OUT.
//!
//! `ChangeStream::events_published()` is deliberately NOT used as the counter —
//! `deliver_remote_notify` sends straight to subscribers without touching it,
//! so a NOTIFY storm would not move it. Counting what a subscriber actually
//! receives is the only measure that sees the failure.

mod common;
use common::cluster_harness::TestCluster;

use std::time::Duration;

use nodedb::control::change_stream::{ChangeOperation, Subscription};

const COLLECTION: &str = "cdc_once";

/// How long to wait for the write's event to reach a node. Generous: on the
/// two non-origin nodes it travels as a QUIC NOTIFY.
const ARRIVAL_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait to prove NO second event follows. A duplicate from a
/// replica's apply loop is published in the same apply round as the original,
/// so it arrives well within this window.
const NO_DUPLICATE_WINDOW: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_write_publishes_exactly_one_change_event_per_node() {
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

    // Subscribe on every node BEFORE the write: the change stream is a
    // broadcast bus with no replay for a receiver that did not yet exist.
    let mut subs: Vec<Subscription> = cluster
        .nodes
        .iter()
        .map(|node| {
            node.shared
                .change_stream
                .subscribe(Some(COLLECTION.to_string()), None)
        })
        .collect();

    // Exactly ONE write, through ONE node. Every replica applies it.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {COLLECTION} (id, payload) VALUES ('row-0', 'payload-0')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert row-0: {e}"));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    for (idx, sub) in subs.iter_mut().enumerate() {
        let event = match tokio::time::timeout(ARRIVAL_TIMEOUT, sub.recv_filtered()).await {
            Ok(Ok(e)) => e,
            Ok(Err(e)) => panic!("node {idx}: change stream closed: {e}"),
            Err(_) => panic!("node {idx}: the replicated INSERT published no change event"),
        };
        assert_eq!(event.collection, COLLECTION, "node {idx}: wrong collection");
        assert_eq!(
            event.operation,
            ChangeOperation::Insert,
            "node {idx}: wrong operation kind"
        );

        // The publish-once guard: a second event for the same write means the
        // apply loop published per replica.
        match tokio::time::timeout(NO_DUPLICATE_WINDOW, sub.recv_filtered()).await {
            Err(_) => {}
            Ok(Ok(dup)) => panic!(
                "node {idx}: one INSERT produced a SECOND change event \
                 ({:?} on {} doc {}) — the write is being published once per \
                 replica instead of once by the node that handled it",
                dup.operation, dup.collection, dup.document_id
            ),
            Ok(Err(e)) => panic!("node {idx}: change stream closed: {e}"),
        }
    }

    drop(subs);
    cluster.shutdown().await;
}
