// SPDX-License-Identifier: BUSL-1.1

//! Intra-transaction visibility for sequences.
//!
//! `CREATE SEQUENCE` is buffered like any other DDL, but the shared
//! `SequenceRegistry` only gains the entry at COMMIT (`post_apply` runs only
//! for `ProposeOutcome::needs_local_apply()`, which a buffered outcome never
//! satisfies). This codebase has no SQL-level `NEXTVAL(...)` expression —
//! `SequenceRegistry::nextval` is reached only from the SERIAL-column INSERT
//! path, which auto-creates its sequence outside the buffered-DDL mechanism
//! entirely. `ALTER SEQUENCE ... RESTART WITH` is the SQL surface that does
//! reach the buffered-DDL path: it validates the target through
//! `SequenceRegistry::exists` and `::get_def` — two of the six methods the
//! connection-scoped ephemeral overlay covers — before proposing the restart.
//! A create-then-restart in one transaction is therefore a direct,
//! SQL-reachable proof of the same fix `NEXTVAL`/`CURRVAL`/`SETVAL` rely on.

use crate::harness::TestServer;

/// Force the connection task to be repolled so the runtime can migrate it to
/// another worker between the statements of one transaction.
async fn hop_workers() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// A sequence created inside a transaction must resolve for an `ALTER
/// SEQUENCE ... RESTART WITH` in that same transaction, before COMMIT ever
/// writes the row to redb or installs it in the shared registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_restart_sequence_in_one_transaction() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SEQUENCE txn_vis_seq START 1 INCREMENT 1")
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("ALTER SEQUENCE txn_vis_seq RESTART WITH 100")
        .await
        .expect("ALTER SEQUENCE must resolve the sequence this transaction just created");
    server.exec("COMMIT").await.unwrap();

    let listed = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(
        listed.iter().any(|n| n == "txn_vis_seq"),
        "the committed sequence must be listed, got: {listed:?}"
    );
}

/// A sequence created and dropped inside one transaction resolves for the
/// `DROP`, and COMMIT leaves nothing usable behind — `DROP SEQUENCE` also
/// validates existence through `SequenceRegistry::exists`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_then_drop_sequence_in_one_transaction() {
    let server = TestServer::start().await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SEQUENCE txn_vis_seq_cd START 1 INCREMENT 1")
        .await
        .unwrap();
    hop_workers().await;
    server
        .exec("DROP SEQUENCE txn_vis_seq_cd")
        .await
        .expect("DROP SEQUENCE must resolve the sequence this transaction just created");
    server.exec("COMMIT").await.unwrap();

    let listed = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(
        !listed.iter().any(|n| n == "txn_vis_seq_cd"),
        "a sequence created and dropped in one transaction must not be listed, got: {listed:?}"
    );
    server
        .exec("CREATE SEQUENCE txn_vis_seq_cd START 1 INCREMENT 1")
        .await
        .expect("the name must be free to reuse after the transaction committed");
}
