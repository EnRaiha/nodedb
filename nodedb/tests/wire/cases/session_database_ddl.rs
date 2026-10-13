// SPDX-License-Identifier: BUSL-1.1

//! DDL and SQL functions resolve their collections and catalog objects in the
//! session's database, after `USE DATABASE`.

use crate::harness::TestServer;

async fn in_session_database(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE DATABASE {name}"))
        .await
        .unwrap();
    server.exec(&format!("USE DATABASE {name}")).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constraints_and_period_locks_resolve_in_the_session_database() {
    let server = TestServer::start().await;
    in_session_database(&server, "sd_constraints").await;

    server
        .exec("CREATE COLLECTION orders (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("ALTER COLLECTION orders ADD CONSTRAINT positive_v CHECK (NEW.v > 0)")
        .await
        .unwrap();
    let constraints = server
        .query_text("SHOW CONSTRAINTS ON orders")
        .await
        .unwrap();
    assert!(
        constraints.iter().any(|row| row.contains("positive_v")),
        "{constraints:?}"
    );
    server
        .exec("DROP CONSTRAINT positive_v ON orders")
        .await
        .unwrap();

    server
        .exec("CREATE COLLECTION periods (period_key TEXT PRIMARY KEY, status TEXT)")
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION entries (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .unwrap();
    server
        .exec(
            "ALTER COLLECTION entries ADD PERIOD LOCK ON fiscal_period \
             REFERENCES periods(period_key) STATUS status \
             ALLOW WRITE WHEN status IN ('OPEN')",
        )
        .await
        .unwrap();
    server
        .exec("ALTER COLLECTION entries DROP PERIOD LOCK")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_functions_resolve_in_the_session_database() {
    let server = TestServer::start().await;
    in_session_database(&server, "sd_kv").await;

    server
        .exec("CREATE COLLECTION counters (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .query_text("SELECT KV_INCR('counters', 'a', 1)")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT n FROM counters WHERE key = 'a'")
        .await
        .unwrap();
    assert!(
        rows.first().is_some_and(|row| row.contains('1')),
        "the counter must land in the session database: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn functions_and_triggers_are_created_in_the_session_database() {
    let server = TestServer::start().await;
    in_session_database(&server, "sd_objects").await;

    server
        .exec("CREATE FUNCTION double_it(x INT) RETURNS INT AS SELECT x * 2")
        .await
        .unwrap();
    let functions = server.query_text("SHOW FUNCTIONS").await.unwrap();
    assert!(
        functions.iter().any(|row| row.contains("double_it")),
        "SHOW FUNCTIONS reads the session database: {functions:?}"
    );
    server.exec("DROP FUNCTION double_it").await.unwrap();

    server
        .exec("CREATE COLLECTION src (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION fire_log (id TEXT PRIMARY KEY, marker TEXT)")
        .await
        .unwrap();
    server
        .exec(
            "CREATE TRIGGER on_upd AFTER UPDATE ON src FOR EACH ROW \
             BEGIN INSERT INTO fire_log (marker) VALUES ('u'); END;",
        )
        .await
        .unwrap();
    let triggers = server.query_text("SHOW TRIGGERS").await.unwrap();
    assert!(
        triggers.iter().any(|row| row.contains("on_upd")),
        "SHOW TRIGGERS reads the session database: {triggers:?}"
    );
    server.exec("DROP TRIGGER on_upd").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_class_lists_the_session_database() {
    let server = TestServer::start().await;
    in_session_database(&server, "sd_catalog").await;

    server
        .exec("CREATE COLLECTION only_here (id TEXT PRIMARY KEY)")
        .await
        .unwrap();
    let rows = server
        .query_text("SELECT relname FROM pg_class")
        .await
        .unwrap();
    assert!(
        rows.iter().any(|row| row.contains("only_here")),
        "pg_class must list the session database's collections: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_ddl_resolves_in_the_session_database() {
    let server = TestServer::start().await;
    in_session_database(&server, "sd_timeseries").await;

    server
        .exec("CREATE TIMESERIES sd_metrics (timestamp TIMESTAMP, value FLOAT)")
        .await
        .unwrap();
    server
        .exec("ALTER TIMESERIES sd_metrics SET (partition_by = '3d', retention_period = '30d')")
        .await
        .unwrap();
    server
        .expect_error(
            "ALTER TIMESERIES sd_metrics SET (partition_by = 'soon')",
            "22023",
        )
        .await;
    server
        .exec("REWRITE PARTITIONS FOR sd_metrics")
        .await
        .unwrap();
}

/// The `__ts_partition_by` DESCRIBE row of `collection`, or `None`.
async fn described_partition_by(server: &TestServer, collection: &str) -> Option<String> {
    let rows = server
        .query_text_joined(&format!("DESCRIBE {collection}"))
        .await
        .unwrap();
    rows.into_iter()
        .find(|row| row.starts_with("__ts_partition_by\t"))
        .and_then(|row| row.split('\t').nth(1).map(str::to_string))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_named_timeseries_in_two_databases_stay_apart() {
    let server = TestServer::start().await;
    for db in ["sd_ts_one", "sd_ts_two"] {
        in_session_database(&server, db).await;
        server
            .exec(
                "CREATE TIMESERIES shared_metrics (timestamp TIMESTAMP, value FLOAT) \
                 WITH (partition_by = '1d')",
            )
            .await
            .unwrap();
    }

    // The session is in `sd_ts_two`: ALTER changes only its collection.
    server
        .exec("ALTER TIMESERIES shared_metrics SET (partition_by = '7d')")
        .await
        .unwrap();
    assert_eq!(
        described_partition_by(&server, "shared_metrics")
            .await
            .as_deref(),
        Some("7d")
    );

    // SHOW PARTITIONS FOR resolves the collection in each session database.
    server
        .query_text("SHOW PARTITIONS FOR shared_metrics")
        .await
        .unwrap();

    server.exec("USE DATABASE sd_ts_one").await.unwrap();
    assert_eq!(
        described_partition_by(&server, "shared_metrics")
            .await
            .as_deref(),
        Some("1d"),
        "the other database's timeseries keeps its config"
    );
    server
        .query_text("SHOW PARTITIONS FOR shared_metrics")
        .await
        .unwrap();
    server
        .exec("REWRITE PARTITIONS FOR shared_metrics")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_partitions_routes_each_grammar_to_its_handler() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TIMESERIES routed_metrics (timestamp TIMESTAMP, value FLOAT)")
        .await
        .unwrap();
    server.exec("CREATE COLLECTION plain_docs").await.unwrap();

    // `FOR` reaches the timeseries handler, which judges the collection type.
    server
        .query_text("SHOW PARTITIONS FOR routed_metrics")
        .await
        .unwrap();
    server
        .expect_error("SHOW PARTITIONS FOR plain_docs", "42809")
        .await;

    // `ON` reaches the consumer-group handler.
    let stream_rows = server.query_text("SHOW PARTITIONS ON no_such_stream").await;
    let judged_as_timeseries = match &stream_rows {
        Ok(rows) => !rows.is_empty(),
        Err(message) => message.contains("timeseries") || message.contains("FOR <collection>"),
    };
    assert!(
        !judged_as_timeseries,
        "the stream form must not reach the timeseries handler: {stream_rows:?}"
    );

    // Any other form names both grammars.
    server
        .expect_error("SHOW PARTITIONS routed_metrics", "42601")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_timeseries_refuses_an_invalid_partition_by() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "CREATE TIMESERIES bad_partition (timestamp TIMESTAMP, value FLOAT) \
             WITH (partition_by = 'soon')",
            "22023",
        )
        .await;
}
