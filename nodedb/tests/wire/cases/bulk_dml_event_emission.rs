// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for bulk DML `WriteEvent` emission. `BulkUpdate`,
//! `BulkDelete`, `UpdateFromJoin`, and `TRUNCATE` must each emit one
//! `WriteEvent` per affected row, like their single-row counterparts.
//! Targets the async/Event-Plane trigger path, since `CREATE SYNC TRIGGER`
//! bypasses `WriteEvent` emission entirely. Trigger bodies are
//! literal-only; binding `NEW`/`OLD` for bulk DML is a separate gap.

use std::time::Duration;

use crate::harness::TestServer;

/// How long to wait for asynchronous trigger dispatch to land. Generous — the
/// poll returns as soon as the count matches, so a high ceiling costs nothing.
const FIRE_LOG_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll `fire_log` until it holds exactly `expected` rows, or fail with the
/// last observed count once `timeout` elapses — async trigger dispatch lags
/// the statement's return.
async fn wait_for_fire_log_count(server: &TestServer, expected: usize, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let rows = server
            .query_text("SELECT marker FROM fire_log")
            .await
            .unwrap();
        if rows.len() == expected {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for fire_log to reach {expected} row(s), got {} row(s): {rows:?}",
            rows.len()
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A bulk `UPDATE ... WHERE` matching multiple rows must fire an
/// `AFTER UPDATE` `FOR EACH ROW` trigger once per affected row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_update_emits_one_update_event_per_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION src (id TEXT PRIMARY KEY, grp TEXT, v INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION fire_log").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER on_upd AFTER UPDATE ON src FOR EACH ROW \
             BEGIN INSERT INTO fire_log (marker) VALUES ('u'); END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO src (id, grp, v) VALUES ('a', 'g1', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO src (id, grp, v) VALUES ('b', 'g1', 2)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO src (id, grp, v) VALUES ('c', 'g2', 3)")
        .await
        .unwrap();

    // Bulk update: matches 'a' and 'b' (grp = 'g1'), not 'c'.
    server
        .exec("UPDATE src SET v = v + 100 WHERE grp = 'g1'")
        .await
        .expect("bulk UPDATE should succeed");

    wait_for_fire_log_count(&server, 2, FIRE_LOG_TIMEOUT).await;

    // The row that did NOT match the WHERE clause must be unaffected.
    let unmatched_v = server
        .query_text("SELECT v FROM src WHERE id = 'c'")
        .await
        .unwrap();
    assert_eq!(unmatched_v, vec!["3".to_string()]);
}

/// A bulk `DELETE FROM ... WHERE` matching multiple rows must fire an
/// `AFTER DELETE` `FOR EACH ROW` trigger once per removed row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_delete_emits_one_delete_event_per_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION src (id TEXT PRIMARY KEY, grp TEXT, v INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION fire_log").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER on_del AFTER DELETE ON src FOR EACH ROW \
             BEGIN INSERT INTO fire_log (marker) VALUES ('d'); END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO src (id, grp, v) VALUES ('a', 'g1', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO src (id, grp, v) VALUES ('b', 'g1', 2)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO src (id, grp, v) VALUES ('c', 'g2', 3)")
        .await
        .unwrap();

    // Bulk delete: removes 'a' and 'b' (grp = 'g1'), leaves 'c'.
    server
        .exec("DELETE FROM src WHERE grp = 'g1'")
        .await
        .expect("bulk DELETE should succeed");

    wait_for_fire_log_count(&server, 2, FIRE_LOG_TIMEOUT).await;

    // The row that did NOT match the WHERE clause must still exist.
    let remaining = server.query_text("SELECT id FROM src").await.unwrap();
    assert_eq!(remaining, vec!["c".to_string()]);
}

/// `UPDATE target SET ... FROM source WHERE ...` matching multiple rows must
/// fire an `AFTER UPDATE` `FOR EACH ROW` trigger once per joined-and-updated
/// target row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_join_emits_one_update_event_per_row() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION uf_target (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION uf_source (id TEXT PRIMARY KEY, new_name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server.exec("CREATE COLLECTION fire_log").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER on_upd AFTER UPDATE ON uf_target FOR EACH ROW \
             BEGIN INSERT INTO fire_log (marker) VALUES ('u'); END;",
        )
        .await
        .unwrap();

    for (id, name) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
        server
            .exec(&format!(
                "INSERT INTO uf_target (id, name) VALUES ('{id}', '{name}')"
            ))
            .await
            .unwrap();
    }
    // Only 'a' and 'b' have a matching source row — 'c' is not touched.
    server
        .exec("INSERT INTO uf_source (id, new_name) VALUES ('a', 'ALPHA_NEW')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO uf_source (id, new_name) VALUES ('b', 'BETA_NEW')")
        .await
        .unwrap();

    server
        .exec(
            "UPDATE uf_target SET name = uf_source.new_name \
             FROM uf_source \
             WHERE uf_target.id = uf_source.id",
        )
        .await
        .expect("UPDATE ... FROM should succeed");

    wait_for_fire_log_count(&server, 2, FIRE_LOG_TIMEOUT).await;

    // The row with no matching source row must be unaffected.
    let unmatched_name = server
        .query_text("SELECT name FROM uf_target WHERE id = 'c'")
        .await
        .unwrap();
    assert_eq!(unmatched_name, vec!["gamma".to_string()]);
}

/// `TRUNCATE` must fire an `AFTER DELETE` `FOR EACH ROW` trigger once per
/// removed row — same as a bulk `DELETE FROM` with no `WHERE` clause.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_emits_one_delete_event_per_row() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION src (id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION fire_log").await.unwrap();

    server
        .exec(
            "CREATE TRIGGER on_del AFTER DELETE ON src FOR EACH ROW \
             BEGIN INSERT INTO fire_log (marker) VALUES ('d'); END;",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO src (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO src (id, v) VALUES ('b', 2)")
        .await
        .unwrap();

    server
        .exec("TRUNCATE TABLE src")
        .await
        .expect("TRUNCATE should succeed");

    wait_for_fire_log_count(&server, 2, FIRE_LOG_TIMEOUT).await;

    let remaining = server.query_text("SELECT id FROM src").await.unwrap();
    assert!(remaining.is_empty(), "TRUNCATE must remove all rows");
}
