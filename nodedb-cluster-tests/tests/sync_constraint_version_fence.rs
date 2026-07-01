// SPDX-License-Identifier: BUSL-1.1
//! A peer CRDT delta admitted against a `constraint_version` the accepting
//! replica has not installed yet is fenced — rejected with a retryable
//! `CompensationHint::Retry` and left unimported — rather than merged ahead
//! of the constraint that is supposed to police it. Once the constraint
//! installs, the re-pushed delta is accepted normally.
//!
//! ## What this guards
//!
//! `CREATE UNIQUE INDEX` bumps the collection's `constraint_version` in the
//! catalog immediately, but the constraint itself is delivered to each
//! data-group's per-core CRDT validator asynchronously by the metadata-leader
//! reconcile loop (`nodedb::bootstrap::constraint_reconcile`). If a peer
//! delta whose admitted `constraint_version_required` is newer than what a
//! replica has installed were allowed to import, that replica would
//! (temporarily, until reconcile catches up) accept rows a stricter
//! constraint would have rejected — a real, if narrow, correctness gap.
//! The fence closes it: such a delta is rejected with
//! `CompensationHint::Retry` and never merged into the tenant document.
//!
//! ## Why this test is deterministic, not racy
//!
//! The reconcile loop normally fires once a second, so a naive test that
//! pushes a delta right after `CREATE UNIQUE INDEX` would race the loop —
//! sometimes the constraint installs before the delta arrives, sometimes
//! after. To make the fence branch observable every run, this test sets
//! `NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS` to an hour before spawning the
//! cluster (the loop's immediate startup tick fires before the collection
//! even exists, so the next auto-tick is an hour away — effectively never,
//! for the test's lifetime) and then drives exactly one reconcile pass on
//! demand via `TestClusterNode::run_constraint_reconcile_once()` once the
//! fenced-rejection assertion has already been made.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb_crdt::{Constraint, ConstraintKind};
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::TenantId;
use nodedb_types::sync::compensation::CompensationHint;

const COLL: &str = "users";
const DOC: &str = "doc1";
const PEER_ID: u64 = 7;

/// Build a Loro snapshot delta inserting one row `row_id` with a single
/// `email` field, mirroring the pattern used by the sibling CRDT sync tests.
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

/// Read `crdt_state('users','doc1')` on `client`. Returns `Ok(Some(payload))`
/// when the document exists, `Ok(None)` when it is absent — the CRDT read path
/// answers a missing document with `ErrorCode::NotFound`, which pgwire renders
/// as SQLSTATE `NO_DATA` (`02000`), so absence is a deterministic, terminal
/// outcome rather than a transient error. Any OTHER error is treated as a
/// transient catch-up hiccup and retried until `timeout`.
async fn read_crdt_doc(
    client: &tokio_postgres::Client,
    timeout: Duration,
) -> Result<Option<String>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT crdt_state('{COLL}', '{DOC}')"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        return Ok(Some(r.get(0).unwrap_or("").to_string()));
                    }
                }
                return Ok(Some(String::new()));
            }
            // A missing document is terminal, not transient: the CRDT read path
            // answers `NotFound`, which the `crdt_state` scalar function surfaces
            // as an internal error whose message carries "NotFound". That is
            // exactly the "not imported" state the fence assertion expects.
            Err(e)
                if e.as_db_error()
                    .is_some_and(|d| d.message().contains("NotFound")) =>
            {
                return Ok(None);
            }
            Err(e) => {
                if Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    continue;
                }
                return Err(format!(
                    "crdt_state query failed: code={:?} msg={:?}",
                    e.code().map(|c| c.code()),
                    e.as_db_error().map(|d| d.message()),
                ));
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_delta_fenced_until_constraint_installed() {
    // Hold the background constraint-reconcile loop off for the lifetime of
    // this test: its only auto-tick fires before `users` even exists, and
    // the next one is an hour away, so `installed` for `users` stays at 0
    // until this test drives `run_constraint_reconcile_once()` explicitly.
    unsafe {
        std::env::set_var("NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS", "3600000");
    }

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

    // Prove the constraint has NOT been installed into the validator yet
    // (auto-reconcile is held off, so this must stay empty — no waiting for
    // convergence, because it will never converge on its own here).
    let immediate = cluster.nodes[0]
        .crdt_constraints(TenantId::new(1), COLL)
        .await;
    assert!(
        immediate.is_empty(),
        "expected no constraints installed before reconcile runs, got: {immediate:?}"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after_wait = cluster.nodes[0]
        .crdt_constraints(TenantId::new(1), COLL)
        .await;
    assert!(
        after_wait.is_empty(),
        "expected constraints to remain uninstalled with reconcile held off, got: {after_wait:?}"
    );

    let cfg = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let state = start_sync_listener(cfg, Some(std::sync::Arc::clone(&cluster.nodes[0].shared)))
        .await
        .expect("start sync listener");
    let addr = state.config.listen_addr;

    let mut client = SyncTestClient::connect(addr)
        .await
        .expect("sync handshake with node 0");

    // Delta A (seq 1): admitted against constraint_version=1 (the catalog
    // already bumped it at CREATE UNIQUE INDEX), but the replica has 0
    // installed — the fence must reject it, not merge it.
    let outcome_a = client
        .push_delta(COLL, DOC, PEER_ID, 1, row_delta(COLL, DOC, "a@b.com"))
        .await
        .expect("push delta A");
    match outcome_a {
        DeltaOutcome::Reject(reject) => match reject.compensation {
            Some(CompensationHint::Retry { .. }) => {}
            other => panic!(
                "expected CompensationHint::Retry for the version-fenced delta A, got: {other:?}"
            ),
        },
        DeltaOutcome::Ack(_) => {
            panic!("expected delta A to be fenced (constraint not installed yet), but it was acked")
        }
    }

    // The fenced delta must NOT have been imported — the document is absent.
    let landed = read_crdt_doc(&cluster.nodes[0].client, Duration::from_secs(5))
        .await
        .expect("crdt_state query after fenced delta");
    assert!(
        landed.is_none(),
        "fenced delta A must not be imported, but crdt_state returned: {landed:?}"
    );

    // Install the constraint deterministically: drive one reconcile pass on
    // every node (only the metadata leader actually proposes; the rest
    // no-op), then poll until the UNIQUE(email) constraint is observed
    // installed in node 0's validator.
    for node in &cluster.nodes {
        node.run_constraint_reconcile_once().await;
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let constraints = cluster.nodes[0]
            .crdt_constraints(TenantId::new(1), COLL)
            .await;
        if constraints
            .iter()
            .any(|c: &Constraint| c.kind == ConstraintKind::Unique && c.field == "email")
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "UNIQUE(email) constraint on '{COLL}' did not install within 15s of driving \
                 reconcile on demand; observed: {constraints:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Re-push the same row as a NEW seq — required(1) <= installed(1) now,
    // and a single fresh row has no unique conflict, so it must be acked.
    let outcome_b = client
        .push_delta(COLL, DOC, PEER_ID, 2, row_delta(COLL, DOC, "a@b.com"))
        .await
        .expect("push delta B");
    assert!(
        matches!(outcome_b, DeltaOutcome::Ack(_)),
        "expected delta B to be acked once the constraint is installed, got: {outcome_b:?}"
    );

    // Now it must be imported — the document exists with a non-empty payload.
    let landed_after = read_crdt_doc(&cluster.nodes[0].client, Duration::from_secs(10))
        .await
        .expect("crdt_state query after accepted delta");
    assert!(
        matches!(landed_after, Some(ref s) if !s.is_empty()),
        "accepted delta B must be imported, but crdt_state on node 0 returned: {landed_after:?}"
    );

    cluster.shutdown().await;
}
