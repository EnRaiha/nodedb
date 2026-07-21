// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for non-trivial WHERE-clause expressions.
//!
//! Scalar functions (`LOWER`, `UPPER`, `LENGTH`, arithmetic, ...) on the
//! left-hand side of a WHERE comparison must be evaluated by the scan
//! filter — not silently dropped through a match-all fall-through.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lower_function_in_where_matches_row() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION entities (\
                id STRING PRIMARY KEY, \
                canonical_name STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO entities (id, canonical_name) VALUES ('ent_1', 'JavaScript')")
        .await
        .unwrap();

    // Baseline: direct equality resolves through the normal filter path.
    let direct = server
        .query_text("SELECT id FROM entities WHERE canonical_name = 'JavaScript'")
        .await
        .unwrap();
    assert_eq!(direct.len(), 1);

    // Scalar function on the LHS must be evaluated per-row, not dropped.
    let lowered = server
        .query_text("SELECT id FROM entities WHERE LOWER(canonical_name) = 'javascript'")
        .await
        .unwrap();
    assert_eq!(
        lowered.len(),
        1,
        "LOWER(canonical_name) = 'javascript' should match the inserted row"
    );
    assert!(lowered[0].contains("ent_1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lower_function_in_where_excludes_nonmatching_row() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION entities (\
                id STRING PRIMARY KEY, \
                canonical_name STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO entities (id, canonical_name) VALUES ('ent_1', 'JavaScript')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO entities (id, canonical_name) VALUES ('ent_2', 'Python')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM entities WHERE LOWER(canonical_name) = 'python'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("ent_2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_with_scalar_function_in_where_is_scoped() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION entities (\
                id STRING PRIMARY KEY, \
                canonical_name STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO entities (id, canonical_name) VALUES ('ent_1', 'JavaScript')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO entities (id, canonical_name) VALUES ('ent_2', 'Python')")
        .await
        .unwrap();

    // If the filter falls through to match-all, this wipes the table.
    server
        .exec("DELETE FROM entities WHERE LOWER(canonical_name) = 'javascript'")
        .await
        .unwrap();

    let rows = server.query_text("SELECT id FROM entities").await.unwrap();
    assert_eq!(rows.len(), 1, "DELETE should only remove the matching row");
    assert!(rows[0].contains("ent_2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_with_scalar_function_in_where_is_scoped() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION entities (\
                id STRING PRIMARY KEY, \
                canonical_name STRING NOT NULL, \
                status STRING) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO entities (id, canonical_name, status) VALUES ('ent_1', 'JavaScript', 'new')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO entities (id, canonical_name, status) VALUES ('ent_2', 'Python', 'new')")
        .await
        .unwrap();

    server
        .exec("UPDATE entities SET status = 'seen' WHERE LOWER(canonical_name) = 'javascript'")
        .await
        .unwrap();

    let touched = server
        .query_text("SELECT id FROM entities WHERE status = 'seen'")
        .await
        .unwrap();
    assert_eq!(
        touched.len(),
        1,
        "UPDATE should only affect the matching row — match-all would touch both"
    );
    assert!(touched[0].contains("ent_1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn between_with_non_literal_bounds_is_evaluated() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION items (\
                id STRING PRIMARY KEY, \
                qty INT NOT NULL, \
                lo INT NOT NULL, \
                hi INT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, qty, lo, hi) VALUES ('i1', 5, 1, 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, qty, lo, hi) VALUES ('i2', 50, 1, 10)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM items WHERE qty BETWEEN lo AND hi")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("i1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_expression_in_where_is_evaluated() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION items (\
                id STRING PRIMARY KEY, \
                status STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, status) VALUES ('a', 'open')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, status) VALUES ('b', 'closed')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM items WHERE NOT (status = 'closed')")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("a"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_list_with_expression_element_is_evaluated() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION items (\
                id STRING PRIMARY KEY, \
                qty INT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, qty) VALUES ('a', 3)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, qty) VALUES ('b', 7)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, qty) VALUES ('c', 9)")
        .await
        .unwrap();

    // `2 + 1` must be evaluated; today it is silently filtered out of the IN set.
    let rows = server
        .query_text("SELECT id FROM items WHERE qty IN (2 + 1, 7)")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "IN (2+1, 7) should match qty=3 and qty=7");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn column_arithmetic_in_where_is_evaluated() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION items (\
                id STRING PRIMARY KEY, \
                qty INT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, qty) VALUES ('i1', 4)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, qty) VALUES ('i2', 9)")
        .await
        .unwrap();

    // `qty + 1 = 5` — arithmetic on a column reference must not fall
    // through to match-all.
    let rows = server
        .query_text("SELECT id FROM items WHERE qty + 1 = 5")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("i1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn is_null_does_not_match_non_null_column() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION nn_rows (\
                id STRING PRIMARY KEY, \
                body STRING) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO nn_rows (id, body) VALUES ('seed_a1', 'x')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO nn_rows (id, body) VALUES ('seed_a2', 'y')")
        .await
        .unwrap();

    // `id` is non-NULL on every row, so `IS NULL` must match nothing and
    // `IS NOT NULL` must match everything. The reported degradation had this
    // inverted — `id IS NULL` matched the healthy, non-NULL rows.
    let is_null = server
        .query_text("SELECT id FROM nn_rows WHERE id IS NULL")
        .await
        .unwrap();
    assert!(
        is_null.is_empty(),
        "`id IS NULL` must not match rows whose id is non-NULL, got {is_null:?}"
    );

    let is_not_null = server
        .query_rows("SELECT id FROM nn_rows WHERE id IS NOT NULL")
        .await
        .unwrap();
    assert_eq!(
        is_not_null.len(),
        2,
        "`id IS NOT NULL` must match every non-NULL row, got {is_not_null:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_where_is_null_does_not_touch_non_null_rows() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION nn_del (\
                id STRING PRIMARY KEY, \
                body STRING) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for i in 1..=3u32 {
        server
            .exec(&format!(
                "INSERT INTO nn_del (id, body) VALUES ('seed_a{i}', 'v{i}')"
            ))
            .await
            .unwrap();
    }

    // Catastrophic-inversion guard: `DELETE WHERE id IS NULL` must remove zero
    // rows when every id is non-NULL. The reported inversion deleted the three
    // committed rows instead of leaving them untouched.
    server
        .exec("DELETE FROM nn_del WHERE id IS NULL")
        .await
        .unwrap();

    let remaining = server.query_rows("SELECT id FROM nn_del").await.unwrap();
    assert_eq!(
        remaining.len(),
        3,
        "DELETE WHERE id IS NULL must not delete rows whose id is non-NULL; \
         all 3 must remain, got {remaining:?}"
    );
}
