// SPDX-License-Identifier: BUSL-1.1
//! A UNIQUE-violating peer CRDT delta yields a precise `DeltaReject`, and
//! the sync stream keeps flowing afterward.
//!
//! ## What this guards
//!
//! Peer deltas are imported into a detached candidate and constraint-checked
//! before that candidate can replace authoritative state. A violation must
//! leave the existing state untouched while telling the edge precisely how to
//! compensate (regenerate the value, prompt the user, etc.).
//!
//! This test drives the sync WebSocket end-to-end: a delta that violates a
//! UNIQUE(email) constraint must come back as a `DeltaReject` carrying a
//! `CompensationHint::UniqueViolation` naming the exact field and
//! conflicting value — not a generic string. And critically, the
//! rejection must not wedge the stream: a subsequent valid delta on the
//! same connection is still acknowledged normally.

mod common;
use common::cluster_harness::{TestCluster, TestClusterNode};

use std::time::{Duration, Instant};

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb_crdt::{Constraint, ConstraintKind};
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::TenantId;
use nodedb_types::sync::compensation::CompensationHint;

const COLL: &str = "users";

/// Build a Loro snapshot delta inserting one row `row_id` with a single
/// `email` field, mirroring the pattern used by the CRDT replication tests.
fn row_delta(collection: &str, row_id: &str, email: &str) -> Vec<u8> {
    let doc = loro::LoroDoc::new();
    let coll = doc.get_map(collection);
    let row = coll
        .insert_container(row_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("email", email).expect("field");
    doc.commit();
    doc.export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot")
}

/// Poll node 0's validator until the UNIQUE(email) constraint on `COLL` has
/// converged, proving the validator is installed before deltas are pushed.
async fn await_unique_email_constraint(node: &TestClusterNode) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let constraints = node.crdt_constraints(TenantId::new(1), COLL).await;
        if constraints
            .iter()
            .any(|c: &Constraint| c.kind == ConstraintKind::Unique && c.field == "email")
        {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "UNIQUE(email) constraint on '{COLL}' did not converge within 30s; \
                 observed: {constraints:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_violation_rejects_without_wedging_stream() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION users");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE UNIQUE INDEX users_email_uniq ON {COLL} (email)"
        ))
        .await
        .expect("CREATE UNIQUE INDEX users_email_uniq");

    await_unique_email_constraint(&cluster.nodes[0]).await;

    let cfg = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) =
        ShutdownBus::new(std::sync::Arc::new(ShutdownWatch::new()));
    let state = start_sync_listener(
        cfg,
        Some(std::sync::Arc::clone(&cluster.nodes[0].shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener");
    let addr = state.config.listen_addr;

    let mut client = SyncTestClient::connect(addr)
        .await
        .expect("sync handshake with node 0");

    // Delta A: first row claims "x@y.com" — must be accepted.
    let outcome_a = client
        .push_delta(COLL, "doc1", 7, 1, row_delta(COLL, "doc1", "x@y.com"))
        .await
        .expect("push delta A");
    assert!(
        matches!(outcome_a, DeltaOutcome::Ack(_)),
        "expected delta A (first claim of x@y.com) to be acked, got: {outcome_a:?}"
    );

    // Delta B: a different row claims the SAME email — must be rejected
    // with a precise UniqueViolation hint naming the field and value.
    let outcome_b = client
        .push_delta(COLL, "doc2", 7, 2, row_delta(COLL, "doc2", "x@y.com"))
        .await
        .expect("push delta B");
    match outcome_b {
        DeltaOutcome::Reject(reject) => match reject.compensation {
            Some(CompensationHint::UniqueViolation {
                field,
                conflicting_value,
            }) => {
                assert_eq!(field, "email", "compensation hint named the wrong field");
                assert_eq!(
                    conflicting_value, "x@y.com",
                    "compensation hint named the wrong conflicting value"
                );
            }
            other => {
                panic!("expected CompensationHint::UniqueViolation for delta B, got: {other:?}")
            }
        },
        DeltaOutcome::Ack(_) => {
            panic!("expected delta B (duplicate email) to be rejected, but it was acked")
        }
    }

    // Delta C: a different row with a distinct email — the rejection above
    // must not have wedged the stream; this must still be acked normally.
    let outcome_c = client
        .push_delta(COLL, "doc3", 7, 3, row_delta(COLL, "doc3", "z@w.com"))
        .await
        .expect("push delta C");
    assert!(
        matches!(outcome_c, DeltaOutcome::Ack(_)),
        "expected delta C after a rejection to still be acked (stream must not wedge), got: {outcome_c:?}"
    );

    cluster.shutdown().await;
}
