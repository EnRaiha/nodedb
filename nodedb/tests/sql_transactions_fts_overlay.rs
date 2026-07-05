// SPDX-License-Identifier: BUSL-1.1

//! In-transaction full-text SEARCH must observe the transaction's own staged
//! document writes (read-your-own-writes for FTS): a document inserted or
//! updated earlier in the same transaction appears in `text_match(...)`
//! results, and one deleted in the transaction is excluded — all BEFORE
//! COMMIT. FTS indexing is an inline side effect of the document write, not
//! a stageable write of its own, so this is implemented by re-tokenizing
//! and BM25-scoring the transaction's staged document bodies at query time
//! and merging them into the base search result (see
//! `handlers/transaction/overlay/fts_merge.rs`).

mod common;

use common::pgwire_harness::TestServer;

async fn setup(server: &TestServer, coll: &str, storage_mode: &str) {
    match storage_mode {
        "document_strict" => {
            server
                .exec(&format!(
                    "CREATE COLLECTION {coll} \
                     (id STRING NOT NULL PRIMARY KEY, body STRING) \
                     WITH (engine='document_strict')"
                ))
                .await
                .unwrap();
        }
        _ => {
            server
                .exec(&format!(
                    "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
                ))
                .await
                .unwrap();
        }
    }
    for (id, body) in [
        ("a1", "the quick brown fox"),
        ("a2", "a lazy dog sleeps"),
        ("unrelated", "completely different topic"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {coll} (id, body) VALUES ('{id}', '{body}')"
            ))
            .await
            .unwrap();
    }
}

async fn matched_ids(server: &TestServer, coll: &str, term: &str) -> Vec<String> {
    let rows = server
        .query_rows(&format!(
            "SELECT id FROM {coll} WHERE text_match(body, '{term}') ORDER BY id"
        ))
        .await
        .unwrap();
    rows.into_iter().map(|r| r[0].clone()).collect()
}

/// BEGIN; INSERT a doc matching a query term; in-tx SEARCH includes it;
/// COMMIT persists it; a separate run verifies ROLLBACK excludes it.
async fn insert_visible_in_txn_then_commit(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    // Baseline: no doc mentions "elephant" yet.
    let base = matched_ids(&server, coll, "elephant").await;
    assert!(base.is_empty(), "{engine}: baseline must not match yet");

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, body) VALUES ('new1', 'an elephant never forgets')"
        ))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "elephant").await;
    assert_eq!(
        in_txn,
        vec!["new1".to_string()],
        "{engine}: in-tx search must include the staged insert before COMMIT"
    );

    server.client.simple_query("COMMIT").await.unwrap();
    let after_commit = matched_ids(&server, coll, "elephant").await;
    assert_eq!(
        after_commit,
        vec!["new1".to_string()],
        "{engine}: committed insert stays visible to search"
    );
}

/// BEGIN; INSERT a doc matching a query term; ROLLBACK; the doc must never
/// have been durably indexed and must not match post-rollback.
async fn insert_visible_in_txn_then_rollback(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, body) VALUES ('new2', 'a giraffe is very tall')"
        ))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "giraffe").await;
    assert_eq!(
        in_txn,
        vec!["new2".to_string()],
        "{engine}: in-tx search must include the staged insert"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = matched_ids(&server, coll, "giraffe").await;
    assert!(
        after_rollback.is_empty(),
        "{engine}: ROLLBACK must leave no durable index trace: {after_rollback:?}"
    );
}

/// BEGIN; DELETE a base doc that matched a term; in-tx SEARCH excludes it;
/// ROLLBACK restores it.
async fn delete_hides_in_txn_then_rollback_restores(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    let base = matched_ids(&server, coll, "fox").await;
    assert_eq!(base, vec!["a1".to_string()], "{engine}: baseline match");

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!("DELETE FROM {coll} WHERE id = 'a1'"))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "fox").await;
    assert!(
        in_txn.is_empty(),
        "{engine}: in-tx search must exclude the staged delete: {in_txn:?}"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = matched_ids(&server, coll, "fox").await;
    assert_eq!(
        after_rollback,
        vec!["a1".to_string()],
        "{engine}: ROLLBACK restores the deleted doc's match"
    );
}

/// An UPDATE that changes the text so it no longer matches (and a second
/// row updated so it newly matches) must be reflected in-tx.
async fn update_changes_match_in_txn(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    // 'a1' currently matches "fox" — update it away from that term.
    server
        .exec(&format!(
            "UPDATE {coll} SET body = 'nothing to see here' WHERE id = 'a1'"
        ))
        .await
        .unwrap();
    // 'a2' currently does not match "fox" — update it to match.
    server
        .exec(&format!(
            "UPDATE {coll} SET body = 'a fox in the henhouse' WHERE id = 'a2'"
        ))
        .await
        .unwrap();

    let in_txn = matched_ids(&server, coll, "fox").await;
    assert_eq!(
        in_txn,
        vec!["a2".to_string()],
        "{engine}: in-tx search must reflect both the moved-out and moved-in update"
    );

    server.client.simple_query("ROLLBACK").await.unwrap();
    let after_rollback = matched_ids(&server, coll, "fox").await;
    assert_eq!(
        after_rollback,
        vec!["a1".to_string()],
        "{engine}: ROLLBACK restores base match state"
    );
}

/// Unrelated base documents must be unaffected by staged writes to other
/// rows, and result ordering (by score, surfaced here via presence) must be
/// stable.
async fn unrelated_docs_unaffected(engine: &str, coll: &str) {
    let server = TestServer::start().await;
    setup(&server, coll, engine).await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, body) VALUES ('new3', 'brown fox and a quick dog')"
        ))
        .await
        .unwrap();

    // "dog" still matches the pre-existing 'a2' row plus the new staged one.
    let mut in_txn = matched_ids(&server, coll, "dog").await;
    in_txn.sort();
    assert_eq!(
        in_txn,
        vec!["a2".to_string(), "new3".to_string()],
        "{engine}: base match for 'a2' must be unaffected by an unrelated staged insert"
    );
    // A term present in no document (neither body nor id) matches nothing,
    // staged or not. (Avoid a term that collides with a doc id, since the id
    // field is indexed too.)
    let unrelated = matched_ids(&server, coll, "kangaroo").await;
    assert!(unrelated.is_empty());

    server.client.simple_query("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_visible_in_txn_then_commit() {
    insert_visible_in_txn_then_commit("document_schemaless", "fts_ov_ins_c").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_visible_in_txn_then_rollback() {
    insert_visible_in_txn_then_rollback("document_schemaless", "fts_ov_ins_r").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_delete_hides_in_txn_then_rollback_restores() {
    delete_hides_in_txn_then_rollback_restores("document_schemaless", "fts_ov_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_update_changes_match_in_txn() {
    update_changes_match_in_txn("document_schemaless", "fts_ov_upd").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_unrelated_docs_unaffected() {
    unrelated_docs_unaffected("document_schemaless", "fts_ov_unrel").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_visible_in_txn_then_commit() {
    insert_visible_in_txn_then_commit("document_strict", "fts_ov_st_ins_c").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_visible_in_txn_then_rollback() {
    insert_visible_in_txn_then_rollback("document_strict", "fts_ov_st_ins_r").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_delete_hides_in_txn_then_rollback_restores() {
    delete_hides_in_txn_then_rollback_restores("document_strict", "fts_ov_st_del").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_update_changes_match_in_txn() {
    update_changes_match_in_txn("document_strict", "fts_ov_st_upd").await;
}
