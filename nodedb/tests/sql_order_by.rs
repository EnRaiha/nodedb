// SPDX-License-Identifier: BUSL-1.1

//! ORDER BY edge case regression tests.
//!
//! Covers: ASC/DESC, mixed direction, NULL ordering, LIMIT interaction,
//! ORDER BY on expressions, and post-UNION ordering.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_id_asc() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION ob (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ob (id, val) VALUES ('c', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ob (id, val) VALUES ('a', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO ob (id, val) VALUES ('b', 20)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM ob ORDER BY id ASC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], "a", "first row should be a, got: {}", rows[0]);
    assert_eq!(rows[2], "c", "last row should be c, got: {}", rows[2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_val_asc_and_desc() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION obd (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('a', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('b', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('c', 20)")
        .await
        .unwrap();

    // ASC
    let rows = server
        .query_text("SELECT id FROM obd ORDER BY val ASC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("b"),
        "ASC first row should be b (val=10), got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("a"),
        "ASC last row should be a (val=30), got: {}",
        rows[2]
    );

    // DESC
    let rows = server
        .query_text("SELECT id FROM obd ORDER BY val DESC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("a"),
        "DESC first row should be a (val=30), got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("b"),
        "DESC last row should be b (val=10), got: {}",
        rows[2]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_desc() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION obd (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('a', 30)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('b', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obd (id, val) VALUES ('c', 20)")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT id FROM obd ORDER BY val DESC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("a"),
        "first row should be a (val=30), got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("b"),
        "last row should be b (val=10), got: {}",
        rows[2]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_with_limit() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION obl (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    for i in 1..=10 {
        server
            .exec(&format!("INSERT INTO obl (id, val) VALUES ('r{i}', {i})"))
            .await
            .unwrap();
    }

    let rows = server
        .query_text("SELECT id FROM obl ORDER BY val DESC LIMIT 3")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "LIMIT 3 should return 3 rows");
    assert!(
        rows[0].contains("r10"),
        "first should be r10 (highest), got: {}",
        rows[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_string_column() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION obs (id TEXT PRIMARY KEY, name TEXT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obs (id, name) VALUES ('1', 'Charlie')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obs (id, name) VALUES ('2', 'Alice')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO obs (id, name) VALUES ('3', 'Bob')")
        .await
        .unwrap();

    let rows = server
        .query_text("SELECT name FROM obs ORDER BY name ASC")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].contains("Alice"),
        "first should be Alice, got: {}",
        rows[0]
    );
    assert!(
        rows[2].contains("Charlie"),
        "last should be Charlie, got: {}",
        rows[2]
    );
}

/// `ORDER BY` applied to a `GROUP BY` result must sort the groups.
/// Surfaced while building B2 derived-FROM coverage: an aggregate
/// query with trailing `ORDER BY` returned rows in arbitrary order
/// (the planner only honours ORDER BY for plain `Scan` plans, not
/// for the `Aggregate` variant). Silent unordered output breaks every
/// dashboard / agent that consumes the rows in declared order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_after_group_by_sorts_groups() {
    let srv = common::pgwire_harness::TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION items_ord (id TEXT PRIMARY KEY, category TEXT, qty INTEGER) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO items_ord (id, category, qty) VALUES \
         ('i1','b',5),('i2','a',2),('i3','c',9),('i4','a',3)",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows(
            "SELECT category, SUM(qty) AS total \
             FROM items_ord GROUP BY category ORDER BY category",
        )
        .await
        .unwrap();

    let cats: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(
        cats,
        vec!["a", "b", "c"],
        "GROUP BY + ORDER BY must produce groups sorted by the key; got \
         {cats:?}. Unordered output is a silent data-correctness bug — \
         downstream consumers cannot rely on the declared sort."
    );
}

/// `ORDER BY <expression>` where the expression is NOT also a projected
/// column. The sort key must still be evaluated and applied; dropping it
/// returns rows in storage order while reporting success, so a client that
/// declared a sort silently receives unsorted data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_expression_absent_from_projection_sorts_rows() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION obx (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    // Inserted in an order that differs from the sorted order of `100 / val`,
    // so storage order cannot be mistaken for a correct sort.
    for (id, val) in [("a", 2), ("b", 20), ("c", 5)] {
        server
            .exec(&format!("INSERT INTO obx (id, val) VALUES ('{id}', {val})"))
            .await
            .unwrap();
    }

    // 100/val → a:50, b:5, c:20 → ascending by the expression: b, c, a.
    let rows = server
        .query_text("SELECT id FROM obx ORDER BY 100 / val")
        .await
        .expect("ORDER BY over an expression must plan and execute");

    assert_eq!(
        rows,
        vec!["b".to_string(), "c".to_string(), "a".to_string()],
        "ORDER BY over an expression not present in the SELECT list must \
         still sort; got {rows:?} (insertion order is a,b,c — an unchanged \
         order means the sort key was silently dropped)"
    );
}

// ── NULL ordering ────────────────────────────────────────────────────────────
//
// PostgreSQL treats the placement of NULLs as part of the sort: `ASC` defaults
// to NULLS LAST and `DESC` to NULLS FIRST, and an explicit `NULLS FIRST` /
// `NULLS LAST` overrides that default *without* being flipped by the sort
// direction. A key that carried only the direction had to guess, so half of
// these four spellings answered with NULLs on the wrong end while reporting
// success.

/// Seed a collection where one row's sort column is NULL.
async fn create_nullable(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, val INT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, val) VALUES ('a', 10)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, val) VALUES ('n', NULL)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, val) VALUES ('b', 20)"
        ))
        .await
        .unwrap();
}

/// `ORDER BY col` (ascending, no NULLS clause) puts NULLs last.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_ascending_defaults_to_nulls_last() {
    let server = TestServer::start().await;
    create_nullable(&server, "obn_asc").await;

    let rows = server
        .query_text("SELECT id FROM obn_asc ORDER BY val")
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec!["a".to_string(), "b".to_string(), "n".to_string()],
        "ascending sorts NULLs last by default; got {rows:?}"
    );
}

/// `ORDER BY col DESC` (no NULLS clause) puts NULLs first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_descending_defaults_to_nulls_first() {
    let server = TestServer::start().await;
    create_nullable(&server, "obn_desc").await;

    let rows = server
        .query_text("SELECT id FROM obn_desc ORDER BY val DESC")
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec!["n".to_string(), "b".to_string(), "a".to_string()],
        "descending sorts NULLs first by default; got {rows:?}"
    );
}

/// An explicit `NULLS FIRST` on an ascending sort overrides the default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_ascending_honors_explicit_nulls_first() {
    let server = TestServer::start().await;
    create_nullable(&server, "obn_af").await;

    let rows = server
        .query_text("SELECT id FROM obn_af ORDER BY val ASC NULLS FIRST")
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec!["n".to_string(), "a".to_string(), "b".to_string()],
        "explicit NULLS FIRST must override the ascending default; got {rows:?}"
    );
}

/// An explicit `NULLS LAST` on a descending sort overrides the default — the
/// NULL placement is not flipped by the direction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_descending_honors_explicit_nulls_last() {
    let server = TestServer::start().await;
    create_nullable(&server, "obn_dl").await;

    let rows = server
        .query_text("SELECT id FROM obn_dl ORDER BY val DESC NULLS LAST")
        .await
        .unwrap();

    assert_eq!(
        rows,
        vec!["b".to_string(), "a".to_string(), "n".to_string()],
        "explicit NULLS LAST must override the descending default; got {rows:?}"
    );
}
