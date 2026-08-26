// SPDX-License-Identifier: BUSL-1.1

//! KV (and timeseries) DML whose `WHERE` resolves to no primary key. A KV
//! collection has no document store, so `KvOp::PredicateUpdate`/
//! `PredicateDelete` must do the row matching themselves. Every test
//! asserts row count and stored values, not just "no error".

use crate::harness::TestServer;

/// The row count a statement reports in its command tag.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        // Debug, not Display: `tokio_postgres::Error`'s Display is just
        // "db error" and the server's message hides in the source chain.
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    let mut count = None;
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = message {
            count = Some(n);
        }
    }
    count.unwrap_or_else(|| panic!("statement reported no command tag: {sql}"))
}

/// Every `(id, owner, note)` row in `collection`, ordered by id.
async fn stored(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!(
            "SELECT id, owner, note FROM {collection} ORDER BY id"
        ))
        .await
        .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
}

/// A KV collection holding three rows: two owned by `alice`, one by `bob`.
async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (id, owner) in [("r1", "alice"), ("r2", "alice"), ("r3", "bob")] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner, note) \
                 VALUES ('{id}', '{owner}', 'before')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}/{id}: {e}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_update_with_no_where_writes_every_row() {
    let server = TestServer::start().await;
    seed(&server, "kv_pred_upd_all").await;

    let count = affected(&server, "UPDATE kv_pred_upd_all SET note = 'touched'").await;
    assert_eq!(count, 3, "an unqualified UPDATE must report every row");
    assert_eq!(
        stored(&server, "kv_pred_upd_all").await,
        vec![
            vec!["r1".to_string(), "alice".to_string(), "touched".to_string()],
            vec!["r2".to_string(), "alice".to_string(), "touched".to_string()],
            vec!["r3".to_string(), "bob".to_string(), "touched".to_string()],
        ],
        "every stored row must carry the new value"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_update_on_a_non_key_predicate_writes_only_matching_rows() {
    let server = TestServer::start().await;
    seed(&server, "kv_pred_upd_owner").await;

    let count = affected(
        &server,
        "UPDATE kv_pred_upd_owner SET note = 'touched' WHERE owner = 'alice'",
    )
    .await;
    assert_eq!(count, 2, "only alice's two rows match");
    assert_eq!(
        stored(&server, "kv_pred_upd_owner").await,
        vec![
            vec!["r1".to_string(), "alice".to_string(), "touched".to_string()],
            vec!["r2".to_string(), "alice".to_string(), "touched".to_string()],
            vec!["r3".to_string(), "bob".to_string(), "before".to_string()],
        ],
        "bob's row must be left exactly as it was"
    );
}

/// `pk = literal AND other = literal` is not a shape `extract_point_keys`
/// reduces, so a fully key-qualified statement takes the predicate path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_update_on_a_conjunction_including_the_key_writes_that_row() {
    let server = TestServer::start().await;
    seed(&server, "kv_pred_upd_and").await;

    let count = affected(
        &server,
        "UPDATE kv_pred_upd_and SET note = 'touched' \
         WHERE id = 'r1' AND owner = 'alice'",
    )
    .await;
    assert_eq!(count, 1, "exactly one row satisfies both conjuncts");
    assert_eq!(
        stored(&server, "kv_pred_upd_and").await,
        vec![
            vec!["r1".to_string(), "alice".to_string(), "touched".to_string()],
            vec!["r2".to_string(), "alice".to_string(), "before".to_string()],
            vec!["r3".to_string(), "bob".to_string(), "before".to_string()],
        ],
        "only the conjunction's row may change"
    );
}

/// A conjunction that names the key but contradicts the other conjunct must
/// write nothing — the predicate is evaluated, not reduced to its key half.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_update_on_an_unsatisfiable_conjunction_writes_nothing() {
    let server = TestServer::start().await;
    seed(&server, "kv_pred_upd_none").await;
    let before = stored(&server, "kv_pred_upd_none").await;

    let count = affected(
        &server,
        "UPDATE kv_pred_upd_none SET note = 'touched' \
         WHERE id = 'r1' AND owner = 'bob'",
    )
    .await;
    assert_eq!(count, 0, "no row satisfies both conjuncts");
    assert_eq!(
        stored(&server, "kv_pred_upd_none").await,
        before,
        "an unsatisfiable predicate must leave every row untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_delete_with_no_where_removes_every_row() {
    let server = TestServer::start().await;
    seed(&server, "kv_pred_del_all").await;

    let count = affected(&server, "DELETE FROM kv_pred_del_all").await;
    assert_eq!(count, 3, "an unqualified DELETE must report every row");
    assert!(
        stored(&server, "kv_pred_del_all").await.is_empty(),
        "the collection must be empty afterwards"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_delete_on_a_non_key_predicate_removes_only_matching_rows() {
    let server = TestServer::start().await;
    seed(&server, "kv_pred_del_owner").await;

    let count = affected(
        &server,
        "DELETE FROM kv_pred_del_owner WHERE owner = 'alice'",
    )
    .await;
    assert_eq!(count, 2, "only alice's two rows match");
    assert_eq!(
        stored(&server, "kv_pred_del_owner").await,
        vec![vec![
            "r3".to_string(),
            "bob".to_string(),
            "before".to_string()
        ]],
        "bob's row must survive"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_delete_on_a_conjunction_including_the_key_removes_that_row() {
    let server = TestServer::start().await;
    seed(&server, "kv_pred_del_and").await;

    let count = affected(
        &server,
        "DELETE FROM kv_pred_del_and WHERE id = 'r1' AND owner = 'alice'",
    )
    .await;
    assert_eq!(count, 1, "exactly one row satisfies both conjuncts");
    assert_eq!(
        stored(&server, "kv_pred_del_and").await,
        vec![
            vec!["r2".to_string(), "alice".to_string(), "before".to_string()],
            vec!["r3".to_string(), "bob".to_string(), "before".to_string()],
        ],
        "only the conjunction's row may be removed"
    );
}

/// Timeseries has no row-level delete; rows expire only via retention policy.
/// The refusal must be an error: a zero-row success is indistinguishable
/// from the old silent no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_delete_is_refused_and_keeps_every_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ts_pred_del \
             COLUMNS (ts BIGINT TIME_KEY, device TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("create timeseries collection");
    for ts in [100_i64, 200, 300] {
        server
            .exec(&format!(
                "INSERT INTO ts_pred_del (ts, device, value) VALUES ({ts}, 'd1', {ts})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed ts_pred_del/{ts}: {e}"));
    }

    let refused = server
        .exec("DELETE FROM ts_pred_del WHERE ts < 250")
        .await
        .expect_err("timeseries DELETE must be refused, never silently applied");
    let text = format!("{refused:?}");
    assert!(
        text.contains("timeseries"),
        "the refusal must name the engine so the caller knows why: {text}"
    );

    let remaining = server
        .query_rows("SELECT ts FROM ts_pred_del ORDER BY ts")
        .await
        .expect("read back ts_pred_del");
    assert_eq!(
        remaining,
        vec![
            vec!["100".to_string()],
            vec!["200".to_string()],
            vec!["300".to_string()]
        ],
        "a refused delete must leave every row in place"
    );
}
