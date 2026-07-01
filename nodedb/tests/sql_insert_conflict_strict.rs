// SPDX-License-Identifier: BUSL-1.1

//! INSERT conflict semantics for strict document engine.
//!
//! `INSERT` is not `UPSERT`: duplicate primary key raises SQLSTATE 23505,
//! `ON CONFLICT DO NOTHING` is a no-op, and `ON CONFLICT (pk) DO UPDATE` /
//! `UPSERT` are the only opt-in overwrite paths.

mod common;

use common::pgwire_harness::TestServer;

// ── Strict document ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_duplicate_pk_raises_unique_violation() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t  \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    // Second INSERT must fail with SQLSTATE 23505 (unique_violation) and the
    // error message must name the conflicting PK value so drivers/users can
    // handle it.
    match server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('dup', 2)")
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
            let msg = db_err.message().to_lowercase();
            assert!(
                msg.contains("dup"),
                "error message should name the conflicting PK, got: {}",
                db_err.message()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_duplicate_pk_preserves_original_row() {
    // Regression guard against silent overwrite: even if the error surfaces,
    // a future routing regression to `PointPut` would overwrite the row
    // underneath the error. Assert the original n=1 survives.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t  \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    let _ = server
        .client
        .simple_query("INSERT INTO t (id, n) VALUES ('dup', 2)")
        .await;

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "expected exactly one row, got {rows:?}");
    assert_eq!(
        rows[0], "1",
        "duplicate-PK INSERT must not overwrite the original row, got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_on_conflict_do_nothing_is_noop() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t  \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    // Must succeed (no error), must not overwrite.
    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 2) ON CONFLICT DO NOTHING")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0], "1",
        "ON CONFLICT DO NOTHING must leave the original row intact, got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_insert_on_conflict_do_update_overwrites() {
    // Regression guard: the opt-in overwrite path must keep working so the
    // fix for the default-INSERT path doesn't strand users without an
    // overwrite option.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t  \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server
        .exec(
            "INSERT INTO t (id, n) VALUES ('dup', 2) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
        )
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], "2", "got: {}", rows[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_upsert_keyword_overwrites() {
    // Regression guard on the explicit UPSERT grammar path.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION t  \
             (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO t (id, n) VALUES ('dup', 1)")
        .await
        .unwrap();

    server
        .exec("UPSERT INTO t (id, n) VALUES ('dup', 2)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT n FROM t WHERE id = 'dup'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], "2", "got: {}", rows[0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_natural_key_pk_on_non_id_column_accepts_distinct_rows() {
    // A user-declared PRIMARY KEY on a column other than the built-in `id`
    // must make that column the uniqueness target. Two rows with DISTINCT
    // natural keys must both persist — the built-in `id` slot must not create
    // a phantom empty-string collision between them.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION nk  \
             (sku STRING NOT NULL PRIMARY KEY, name STRING) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO nk (sku, name) VALUES ('a', 'first')")
        .await
        .unwrap();

    // Distinct natural key: must succeed, must NOT collide on an empty `id`.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_natural_key_upsert_targets_declared_pk_column() {
    // The UPSERT doc_id path (`convert_upsert`) must also key off the declared
    // natural PK, not the built-in empty `id`. A distinct natural key inserts a
    // new row; a repeated natural key overwrites in place.
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION nk  \
             (sku STRING NOT NULL PRIMARY KEY, name STRING) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("UPSERT INTO nk (sku, name) VALUES ('a', 'first')")
        .await
        .unwrap();

    // Distinct natural key → second row, no empty-`id` collision.
    server
        .exec("UPSERT INTO nk (sku, name) VALUES ('b', 'second')")
        .await
        .unwrap();

    // Repeated natural key → overwrite in place, still two rows total.
    server
        .exec("UPSERT INTO nk (sku, name) VALUES ('a', 'first-updated')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT name FROM nk ORDER BY sku")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "expected two distinct natural-key rows, got {rows:?}"
    );
    assert_eq!(
        rows,
        vec!["first-updated".to_string(), "second".to_string()]
    );
}
