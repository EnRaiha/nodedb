// SPDX-License-Identifier: BUSL-1.1
//! A DDL that changes a collection's CRDT constraint set installs it before it
//! returns, so the constraint-version fence never holds the first synced delta
//! after the DDL.
//!
//! ## What this guards
//!
//! `CREATE COLLECTION` with a `PRIMARY KEY` and `CREATE UNIQUE INDEX` each bump
//! the collection's `constraint_version` in the catalog. A synced delta is
//! admitted against that version, and its data-group apply is fenced until the
//! version is installed on the applying replica. The DDL proposes the
//! matching `ConstraintChange` on the collection's data Raft log before it
//! returns. A delta proposed later on the same log therefore applies after the
//! install on every replica, wherever it entered the cluster.
//!
//! The DDL runs on one node and the delta enters through the sync listener of
//! another node, right after the DDL returns. The delta must apply on its
//! first push, never answer `Gap`.
//!
//! ## Why the install can only come from the DDL
//!
//! The test sets `NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS` to an hour before it
//! spawns the cluster. The reconcile loop's startup tick fires before the
//! collection exists, and its next tick is an hour away. No reconcile pass
//! runs during the test, so only the DDL can have installed the version.
//!
//! The fence itself, for a delta that does outrun its install, is covered by
//! the Data Plane unit test
//! `a_delta_ahead_of_the_installed_constraint_version_is_fenced`.

use crate::common;
use common::cluster_harness::TestCluster;

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::sync::wire::AckStatus;

const COLL: &str = "users";
const DOC: &str = "doc1";
const SEQ: u64 = 1;

/// A Loro delta inserting row `row_id` with `id` and `email`, and the peer
/// id that wrote it.
fn row_delta(collection: &str, row_id: &str, email: &str) -> (Vec<u8>, u64) {
    let doc = loro::LoroDoc::new();
    let row = doc
        .get_map(collection)
        .insert_container(row_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("id", row_id).expect("id field");
    row.insert("email", email).expect("email field");
    doc.commit();
    let delta = doc
        .export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot");
    (delta, doc.peer_id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_pushed_through_another_node_right_after_ddl_applies() {
    // Hold the background constraint-reconcile loop off for the lifetime of
    // this test, so only the DDL can install the constraint version.
    unsafe {
        std::env::set_var("NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS", "3600000");
    }

    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

    let ddl_node = cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, email TEXT) WITH (crdt='true')"
        ))
        .await
        .expect("CREATE COLLECTION users");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE UNIQUE INDEX users_email_uniq ON {COLL} (email)"
        ))
        .await
        .expect("CREATE UNIQUE INDEX users_email_uniq");

    // The delta enters through a node other than the one that ran CREATE.
    let push_node = (ddl_node + 1) % cluster.nodes.len();
    let cfg = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) =
        ShutdownBus::new(std::sync::Arc::new(ShutdownWatch::new()));
    let state = start_sync_listener(
        cfg,
        Some(std::sync::Arc::clone(&cluster.nodes[push_node].shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener");
    let mut client = SyncTestClient::connect(state.config.listen_addr)
        .await
        .expect("sync handshake");

    let (delta, peer_id) = row_delta(COLL, DOC, "a@b.com");
    let outcome = client
        .push_delta_at_seq(COLL, DOC, peer_id, 1, SEQ, delta)
        .await
        .expect("push delta");
    let ack = match outcome {
        DeltaOutcome::Ack(ack) => ack,
        DeltaOutcome::Reject(reject) => panic!(
            "the first delta after the DDL must apply, got DeltaReject (reason={:?}, \
             compensation={:?})",
            reject.reason, reject.compensation
        ),
    };
    assert_eq!(
        ack.status,
        AckStatus::Applied,
        "the DDL installed the constraint version before it returned, so the first push \
         through node {push_node} must apply, got {:?}; groups:\n{}",
        ack.status,
        cluster.group_positions_report()
    );

    cluster.shutdown().await;
}
