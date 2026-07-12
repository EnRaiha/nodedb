// SPDX-License-Identifier: BUSL-1.1

//! In-transaction `INSERT ... SELECT` is resolved + staged at STATEMENT time
//! (`control::server::shared::session::expander_stage`), not expanded at COMMIT.
//!
//! Statement-time staging with FRESH registered surrogates has two observable
//! consequences a COMMIT-time expander could never provide, both proven here:
//!
//! 1. **Read-your-own-writes.** A `SELECT` issued after the `INSERT ... SELECT`
//!    but still inside the same transaction sees the copied rows — because the
//!    concrete `PointInsert` ops the copy expands to were staged into the
//!    transaction's overlay the moment the statement ran (not held as a raw
//!    `InsertSelect` plan until COMMIT).
//! 2. **Fresh cross-engine identity.** After COMMIT each copied row is searchable
//!    at its embedding and resolves through the target's vector index to its OWN
//!    primary key — proving it carries a fresh, catalog-registered surrogate with
//!    a `(target_collection, surrogate)→pk` binding, not the source row's
//!    surrogate (which has no target binding).
//!
//! Every copied row carries an explicit `id` primary key; no restart assertions
//! (durability of in-transaction writes is a separate unit).

mod common;

use common::pgwire_harness::TestServer;

/// (1) An in-transaction `SELECT` issued AFTER the `INSERT ... SELECT`, still
/// inside the transaction, must SEE the copied rows (read-your-own-writes) —
/// only possible because the copy was staged at statement time. (2) After COMMIT
/// each copied row is independently searchable at its embedding and resolves to
/// its OWN id via a fresh registered surrogate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txn_insert_select_staged_visible_and_fresh_identity() {
    let srv = TestServer::start().await;

    // Target: initially-empty document collection carrying a vector index.
    srv.exec("CREATE COLLECTION issx_target").await.unwrap();
    srv.exec("CREATE VECTOR INDEX idx_issx_target ON issx_target (embedding) METRIC cosine DIM 4")
        .await
        .unwrap();

    // Source: two rows, each with an embedding on a distinct axis.
    srv.exec("CREATE COLLECTION issx_source").await.unwrap();
    for (id, v) in [
        ("alpha", [1.0f32, 0.0, 0.0, 0.0]),
        ("beta", [0.0, 0.0, 0.0, 1.0]),
    ] {
        srv.exec(&format!(
            "INSERT INTO issx_source (id, embedding) VALUES ('{id}', ARRAY[{},{},{},{}])",
            v[0], v[1], v[2], v[3]
        ))
        .await
        .unwrap();
    }

    srv.exec("BEGIN").await.unwrap();
    srv.client
        .simple_query("INSERT INTO issx_target SELECT * FROM issx_source")
        .await
        .expect("in-tx insert-select should succeed at the statement");

    // Read-your-own-writes: the copied rows are visible to this later statement
    // WITHIN the same transaction (impossible under COMMIT-time expansion).
    let mut in_txn = srv.query_text("SELECT id FROM issx_target").await.unwrap();
    in_txn.sort();
    assert_eq!(
        in_txn,
        vec!["alpha".to_string(), "beta".to_string()],
        "in-txn SELECT after INSERT ... SELECT must see the staged copy \
         (read-your-own-writes); got {in_txn:?}"
    );

    srv.exec("COMMIT").await.unwrap();

    // Post-commit scan sees both copied rows.
    let mut scanned = srv.query_text("SELECT id FROM issx_target").await.unwrap();
    scanned.sort();
    assert_eq!(
        scanned,
        vec!["alpha".to_string(), "beta".to_string()],
        "post-commit scan must return both copied rows; got {scanned:?}"
    );

    // Each copied row resolves through the target's vector index to its OWN id —
    // only possible with a fresh registered surrogate bound under
    // (issx_target, id). A reused source surrogate has no target binding.
    let near_e1 = srv
        .query_text(
            "SELECT id FROM issx_target \
             WHERE embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e1.first().map(String::as_str),
        Some("alpha"),
        "vector search near E1 must resolve the copied 'alpha' to its own id; got {near_e1:?} \
         (stale source surrogate → unresolvable)"
    );

    let near_e2 = srv
        .query_text(
            "SELECT id FROM issx_target \
             WHERE embedding <-> ARRAY[0.0, 0.0, 0.0, 1.0] LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(
        near_e2.first().map(String::as_str),
        Some("beta"),
        "vector search near E2 must resolve the copied 'beta' to its own id; got {near_e2:?}"
    );
}
