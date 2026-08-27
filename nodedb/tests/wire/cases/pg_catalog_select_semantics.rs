// SPDX-License-Identifier: BUSL-1.1

//! SELECT semantics on virtual catalog tables.
//!
//! `_system.*` views and `pg_catalog.*` tables are intercepted by a
//! substring-based dispatcher that bypasses the SQL planner: it returns
//! the raw row set with only ad-hoc seq-range / LIMIT extraction from
//! the SQL string. Aggregates, projections, ORDER BY, and arbitrary
//! WHERE predicates are silently dropped — clients get rows but not
//! the rows they asked for.
//!
//! These tests assert the correct spec: a SELECT against a virtual
//! catalog table must observe full SQL semantics, just like any other
//! relation.

use crate::harness::TestServer;

// ---------- _system.audit_log ----------

/// `SELECT count(*) FROM _system.audit_log` must return one aggregate row,
/// not the full table.
#[tokio::test]
async fn audit_log_count_star_returns_single_aggregate_row() {
    let srv = TestServer::start().await;

    // Generate audit entries deterministically.
    srv.exec("CREATE USER audit_sem_a WITH PASSWORD 'pw'")
        .await
        .expect("create user a");
    srv.exec("CREATE USER audit_sem_b WITH PASSWORD 'pw'")
        .await
        .expect("create user b");

    let rows = srv
        .query_text("SELECT count(*) FROM _system.audit_log")
        .await
        .expect("count(*) on audit_log");
    assert_eq!(
        rows.len(),
        1,
        "count(*) must return exactly one aggregate row, got {} rows (table-dump bug)",
        rows.len()
    );
    let n: i64 = rows[0].parse().expect("count is an integer");
    assert!(n >= 2, "expected >=2 audit entries, got {n}");
}

/// WHERE clause must filter rows server-side. Exact repro from the bug:
/// `WHERE prev_hash IS NOT NULL` must exclude the seq=1 genesis row.
#[tokio::test]
async fn audit_log_where_prev_hash_is_not_null_filters_genesis() {
    let srv = TestServer::start().await;
    srv.exec("CREATE USER audit_sem_c WITH PASSWORD 'pw'")
        .await
        .expect("create user c");
    srv.exec("CREATE USER audit_sem_d WITH PASSWORD 'pw'")
        .await
        .expect("create user d");

    let all = srv
        .query_rows("SELECT seq, prev_hash FROM _system.audit_log")
        .await
        .expect("scan all");
    let filtered = srv
        .query_rows("SELECT seq, prev_hash FROM _system.audit_log WHERE prev_hash IS NOT NULL")
        .await
        .expect("scan filtered");

    // The unfiltered scan must include the genesis row (prev_hash = '').
    let has_genesis = all
        .iter()
        .any(|r| r.get(1).map(|s| s.is_empty()).unwrap_or(false));
    assert!(
        has_genesis,
        "expected at least one row with empty prev_hash (genesis)"
    );
    // The filtered scan must NOT include any empty-prev_hash row.
    for r in &filtered {
        let ph = r.get(1).map(String::as_str).unwrap_or("");
        assert!(
            !ph.is_empty(),
            "WHERE prev_hash IS NOT NULL leaked a row with empty prev_hash: {r:?}"
        );
    }
    assert!(
        filtered.len() < all.len(),
        "filtered scan ({}) must be strictly smaller than full scan ({}) — WHERE was dropped",
        filtered.len(),
        all.len()
    );
}

/// `WHERE event = '<name>'` must filter rows by the event column.
#[tokio::test]
async fn audit_log_where_event_equality_filters() {
    let srv = TestServer::start().await;
    srv.exec("CREATE USER audit_sem_e WITH PASSWORD 'pw'")
        .await
        .expect("create user e");

    let rows = srv
        .query_rows("SELECT event FROM _system.audit_log WHERE event = 'PrivilegeChange'")
        .await
        .expect("scan filtered by event");
    assert!(
        !rows.is_empty(),
        "expected at least one PrivilegeChange row"
    );
    for r in &rows {
        assert_eq!(
            r.first().map(String::as_str),
            Some("PrivilegeChange"),
            "WHERE event = 'PrivilegeChange' returned a non-matching row: {r:?}"
        );
    }
}

/// A bare-column projection must return only the requested column,
/// not the full 7-column row.
#[tokio::test]
async fn audit_log_projection_returns_only_selected_columns() {
    let srv = TestServer::start().await;
    srv.exec("CREATE USER audit_sem_f WITH PASSWORD 'pw'")
        .await
        .expect("create user f");

    let rows = srv
        .query_rows("SELECT seq FROM _system.audit_log LIMIT 5")
        .await
        .expect("projection scan");
    assert!(!rows.is_empty(), "expected at least one row");
    for r in &rows {
        assert_eq!(
            r.len(),
            1,
            "SELECT seq must project a single column, got {} columns: {r:?}",
            r.len()
        );
    }
}

/// ORDER BY must actually sort the rows server-side.
#[tokio::test]
async fn audit_log_order_by_seq_desc_sorts_rows() {
    let srv = TestServer::start().await;
    for u in ["audit_sem_g", "audit_sem_h", "audit_sem_i"] {
        srv.exec(&format!("CREATE USER {u} WITH PASSWORD 'pw'"))
            .await
            .expect("create user");
    }

    let rows = srv
        .query_text("SELECT seq FROM _system.audit_log ORDER BY seq DESC LIMIT 10")
        .await
        .expect("ordered scan");
    assert!(rows.len() >= 2, "need at least 2 rows to verify order");
    let seqs: Vec<i64> = rows.iter().map(|s| s.parse().expect("seq int")).collect();
    for w in seqs.windows(2) {
        assert!(w[0] >= w[1], "ORDER BY seq DESC violated: {seqs:?}");
    }
}

// ---------- pg_catalog.pg_class ----------

/// `SELECT count(*) FROM pg_class` must return one aggregate row.
#[tokio::test]
async fn pg_class_count_star_returns_single_aggregate_row() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION pg_class_count_a (id INTEGER PRIMARY KEY)")
        .await
        .expect("create a");
    srv.exec("CREATE COLLECTION pg_class_count_b (id INTEGER PRIMARY KEY)")
        .await
        .expect("create b");

    let rows = srv
        .query_text("SELECT count(*) FROM pg_class")
        .await
        .expect("count(*) on pg_class");
    assert_eq!(
        rows.len(),
        1,
        "count(*) must return exactly one aggregate row, got {}",
        rows.len()
    );
    let n: i64 = rows[0].parse().expect("count is an integer");
    assert!(n >= 2, "expected >=2 pg_class rows, got {n}");
}

/// `WHERE relname = 'X'` on pg_class must filter rows server-side.
/// Today the dispatcher returns every relation regardless of the predicate.
#[tokio::test]
async fn pg_class_where_relname_filters_rows() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION pg_class_filter_target (id INTEGER PRIMARY KEY)")
        .await
        .expect("create target");
    srv.exec("CREATE COLLECTION pg_class_filter_other (id INTEGER PRIMARY KEY)")
        .await
        .expect("create other");

    let rows = srv
        .query_rows("SELECT relname FROM pg_class WHERE relname = 'pg_class_filter_target'")
        .await
        .expect("filtered scan");
    assert!(
        !rows.is_empty(),
        "expected the target collection to show up in pg_class"
    );
    for r in &rows {
        assert_eq!(
            r.first().map(String::as_str),
            Some("pg_class_filter_target"),
            "WHERE relname = 'pg_class_filter_target' returned a non-matching row: {r:?}"
        );
    }
}

// ---------- pg_catalog.pg_namespace ----------

/// `SELECT count(*) FROM pg_namespace` must return one aggregate row.
#[tokio::test]
async fn pg_namespace_count_star_returns_single_aggregate_row() {
    let srv = TestServer::start().await;
    let rows = srv
        .query_text("SELECT count(*) FROM pg_namespace")
        .await
        .expect("count(*) on pg_namespace");
    assert_eq!(
        rows.len(),
        1,
        "count(*) must return one aggregate row, got {}",
        rows.len()
    );
}

// ---------- _system.dropped_collections ----------

/// WHERE on `_system.dropped_collections` must filter by collection name.
#[tokio::test]
async fn dropped_collections_where_name_filters_rows() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION dropped_filter_keep (id INTEGER PRIMARY KEY)")
        .await
        .expect("create keep");
    srv.exec("CREATE COLLECTION dropped_filter_target (id INTEGER PRIMARY KEY)")
        .await
        .expect("create target");
    srv.exec("DROP COLLECTION dropped_filter_keep")
        .await
        .expect("drop keep");
    srv.exec("DROP COLLECTION dropped_filter_target")
        .await
        .expect("drop target");

    let rows = srv
        .query_rows(
            "SELECT name FROM _system.dropped_collections WHERE name = 'dropped_filter_target'",
        )
        .await
        .expect("filtered scan");
    assert!(
        !rows.is_empty(),
        "expected the dropped target to appear in _system.dropped_collections"
    );
    for r in &rows {
        assert_eq!(
            r.first().map(String::as_str),
            Some("dropped_filter_target"),
            "WHERE name = 'dropped_filter_target' returned a non-matching row: {r:?}"
        );
    }
}
