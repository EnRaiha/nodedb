// SPDX-License-Identifier: BUSL-1.1

//! INSERT conflict semantics for schemaless document engine.
//!
//! Shares the same spec as the strict engine — the schemaless path also
//! routes through `DocumentOp::PointPut`, so the same invariants apply:
//! duplicate-id INSERT raises 23505, `ON CONFLICT DO NOTHING` is a no-op.

mod common;

use common::pgwire_harness::TestServer;

// ── Schemaless document ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_duplicate_id_raises_unique_violation() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    match server
        .client
        .simple_query("INSERT INTO docs (id, n) VALUES ('dup', 2)")
        .await
    {
        Ok(_) => panic!("expected unique_violation, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
            assert!(
                db_err.message().to_lowercase().contains("dup"),
                "error must name the conflicting id, got: {}",
                db_err.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_duplicate_id_preserves_original_row() {
    // Silent-overwrite regression guard on the schemaless path — the
    // DocumentSchemaless arm of `convert_insert` shares the PointPut
    // routing, so the same check must hold.
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    let _ = server
        .client
        .simple_query("INSERT INTO docs (id, n) VALUES ('dup', 2)")
        .await;

    let rows = server
        .query_text("SELECT n FROM docs WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
    assert_eq!(
        rows[0], "1",
        "duplicate-id INSERT must not overwrite the original row, got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_insert_on_conflict_do_nothing_is_noop() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION docs").await.unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO docs (id, n) VALUES ('dup', 2) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM docs WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], "1", "got: {}", rows[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_natural_key_pk_on_non_id_column_accepts_distinct_rows() {
    // Sibling-surface guard for the strict-engine natural-key fix: the
    // schemaless arm shares the same `convert_insert` doc_id extraction, so a
    // user PRIMARY KEY on a non-`id` column must drive uniqueness here too.
    // Distinct natural keys must both persist — no phantom empty-`id` collision.
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION nk (sku STRING NOT NULL PRIMARY KEY, name STRING)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO nk (sku, name) VALUES ('a', 'first')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO nk (sku, name) VALUES ('b', 'second')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT name FROM nk ORDER BY sku")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "both natural-key rows must persist, got {rows:?}"
    );
    assert_eq!(rows, vec!["first".to_string(), "second".to_string()]);
}
