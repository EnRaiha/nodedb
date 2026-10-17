// SPDX-License-Identifier: BUSL-1.1

//! End-to-end pgwire tests for security features: audit log, RLS, SHOW
//! USERS, SHOW GRANTS, SHOW CONSTRAINTS.

use crate::harness::TestServer;

// ── Audit Log ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_audit_log_succeeds() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION audit_test").await.unwrap();

    // SHOW AUDIT LOG should execute without error.
    // May return 0 rows if audit level is Minimal or entries aren't flushed yet.
    let result = server.query_text("SHOW AUDIT LOG LIMIT 10").await;
    assert!(
        result.is_ok(),
        "SHOW AUDIT LOG should not error: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_audit_log_with_limit() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION audit_lim").await.unwrap();

    let result = server.query_text("SHOW AUDIT LOG LIMIT 2").await;
    assert!(result.is_ok(), "SHOW AUDIT LOG LIMIT should not error");
    if let Ok(rows) = result {
        assert!(rows.len() <= 2, "LIMIT 2 should return at most 2 rows");
    }
}

/// Every audit reader returns an action right after it runs, with no wait for a flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_readers_see_action_immediately() {
    let server = TestServer::start().await;

    server
        .exec("CREATE USER audit_ryw WITH PASSWORD 'pw'")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION audit_ryw_coll")
        .await
        .unwrap();
    server
        .exec("GRANT READ ON audit_ryw_coll TO audit_ryw")
        .await
        .unwrap();

    let is_create = |d: &str| d.contains("created user 'audit_ryw'");
    let is_grant = |d: &str| d.contains("granted") && d.contains("'audit_ryw'");

    let log = server
        .query_named_rows("SHOW AUDIT LOG LIMIT 1000")
        .await
        .expect("SHOW AUDIT LOG");
    let details: Vec<&str> = log
        .iter()
        .filter_map(|r| r.get("detail").map(String::as_str))
        .collect();
    assert!(
        details.iter().any(|d| is_create(d)),
        "SHOW AUDIT LOG must list CREATE USER at once: {details:?}"
    );
    assert!(
        details.iter().any(|d| is_grant(d)),
        "SHOW AUDIT LOG must list GRANT at once: {details:?}"
    );
    let seqs: Vec<i64> = log
        .iter()
        .filter_map(|r| r.get("seq").and_then(|s| s.parse().ok()))
        .collect();
    assert!(
        seqs.windows(2).all(|w| w[0] > w[1]),
        "SHOW AUDIT LOG rows must be unique and most-recent-first: {seqs:?}"
    );

    let filtered = server
        .query_named_rows("SHOW AUDIT WHERE event_type = 'privilege_change' LIMIT 1000")
        .await
        .expect("SHOW AUDIT WHERE");
    assert!(
        filtered
            .iter()
            .all(|r| r.get("event").map(String::as_str) == Some("privilege_change")),
        "SHOW AUDIT WHERE must return only the filtered event: {filtered:?}"
    );
    assert!(
        filtered
            .iter()
            .filter_map(|r| r.get("detail"))
            .any(|d| is_create(d)),
        "SHOW AUDIT WHERE must list CREATE USER at once: {filtered:?}"
    );

    let table = server
        .query_named_rows("SELECT detail FROM _system.audit_log")
        .await
        .expect("SELECT FROM _system.audit_log");
    assert!(
        table
            .iter()
            .filter_map(|r| r.get("detail"))
            .any(|d| is_grant(d)),
        "_system.audit_log must list GRANT at once: {table:?}"
    );
}

/// `SHOW AUDIT IN DATABASE` applies the `event_type` filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_audit_in_database_applies_event_type_filter() {
    let server = TestServer::start().await;

    server
        .exec("CREATE DATABASE audit_db_filter")
        .await
        .unwrap();

    let all = server
        .query_named_rows("SHOW AUDIT IN DATABASE audit_db_filter LIMIT 1000")
        .await
        .expect("SHOW AUDIT IN DATABASE");
    assert!(
        all.iter()
            .any(|r| r.get("event").map(String::as_str) == Some("database_created")),
        "SHOW AUDIT IN DATABASE must list CREATE DATABASE: {all:?}"
    );

    let created = server
        .query_named_rows(
            "SHOW AUDIT IN DATABASE audit_db_filter \
             WHERE event_type = 'database_created' LIMIT 1000",
        )
        .await
        .expect("SHOW AUDIT IN DATABASE WHERE");
    assert!(
        !created.is_empty(),
        "filter must keep database_created rows"
    );
    assert!(
        created
            .iter()
            .all(|r| r.get("event").map(String::as_str) == Some("database_created")),
        "filter must return only database_created rows: {created:?}"
    );

    let other = server
        .query_named_rows(
            "SHOW AUDIT IN DATABASE audit_db_filter WHERE event_type = 'auth_failure'",
        )
        .await
        .expect("SHOW AUDIT IN DATABASE WHERE other event");
    assert!(
        other.is_empty(),
        "filter must drop database_created rows: {other:?}"
    );
}

/// Every SHOW AUDIT WHERE form other than `event_type = '...'` is refused with 42601.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_audit_refuses_unsupported_where() {
    let server = TestServer::start().await;

    server.exec("CREATE DATABASE audit_db_where").await.unwrap();

    for sql in [
        "SHOW AUDIT WHERE detail = 'x'",
        "SHOW AUDIT WHERE event_type = 'auth_success' AND seq > 5",
        "SHOW AUDIT IN DATABASE audit_db_where WHERE seq > 5",
        "SHOW AUDIT IN DATABASE audit_db_where WHERE event_type = 'a' OR event_type = 'b'",
    ] {
        let err = server
            .exec(sql)
            .await
            .expect_err(&format!("{sql} must be refused"));
        assert!(
            err.contains("SQLSTATE 42601"),
            "{sql} must fail with 42601: {err}"
        );
        assert!(
            err.contains("unsupported SHOW AUDIT WHERE clause"),
            "{sql} must name the unsupported clause: {err}"
        );
    }

    let err = server
        .exec("SHOW AUDIT IN DATABASE audit_db_where ORDER BY seq")
        .await
        .expect_err("a stray clause must be refused");
    assert!(err.contains("SQLSTATE 42601"), "stray clause: {err}");
}

// ── SHOW USERS ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_users_succeeds() {
    let server = TestServer::start().await;

    // SHOW USERS should execute without error (may return 0 rows in test mode).
    let result = server.query_text("SHOW USERS").await;
    assert!(result.is_ok(), "SHOW USERS should not error: {result:?}");
}

// ── SHOW CONSTRAINTS ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_constraints_with_all_kinds() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION sec_test").await.unwrap();

    // Add a state transition constraint.
    server
        .exec(
            "ALTER COLLECTION sec_test ADD CONSTRAINT status_flow \
             ON COLUMN status TRANSITIONS ('draft' -> 'active')",
        )
        .await
        .unwrap();

    // Add a general CHECK constraint.
    server
        .exec(
            "ALTER COLLECTION sec_test ADD CONSTRAINT pos_val \
             CHECK (NEW.val > 0)",
        )
        .await
        .unwrap();

    // Add a typeguard.
    server
        .exec("CREATE TYPEGUARD ON sec_test (name STRING)")
        .await
        .unwrap();

    // SHOW CONSTRAINTS should return all three kinds.
    let rows = server
        .query_text("SHOW CONSTRAINTS ON sec_test")
        .await
        .unwrap();
    assert!(
        rows.len() >= 3,
        "should have transition + check + typeguard: {rows:?}"
    );
}

// ── RLS Policy DDL ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_show_rls_policy() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION rls_orders").await.unwrap();

    // Create an RLS policy ($auth.id is the authenticated user's ID).
    server
        .exec(
            "CREATE RLS POLICY own_orders ON rls_orders FOR READ \
             USING (customer_id = $auth.id)",
        )
        .await
        .unwrap();

    // SHOW RLS POLICIES should succeed.
    let rows = server.query_text("SHOW RLS POLICIES").await.unwrap();
    // At least one policy should exist (the one we just created).
    assert!(!rows.is_empty(), "should have at least one policy");

    // Drop the policy.
    server
        .exec("DROP RLS POLICY own_orders ON rls_orders")
        .await
        .unwrap();
}

// ── Change Tracking via Typeguard DEFAULT/VALUE ──

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typeguard_created_at_and_updated_at() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION tracked_docs").await.unwrap();

    server
        .exec(
            "CREATE TYPEGUARD ON tracked_docs (\
                 created_at STRING DEFAULT 'auto-set',\
                 version INT DEFAULT 1\
             )",
        )
        .await
        .unwrap();

    // Insert — DEFAULT should fill created_at and version.
    server
        .exec("INSERT INTO tracked_docs { id: 't1', name: 'Alice' }")
        .await
        .unwrap();

    let rows = server
        .query_text_joined("SELECT * FROM tracked_docs WHERE id = 't1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].contains("auto-set"),
        "DEFAULT should inject created_at: {rows:?}"
    );
}
