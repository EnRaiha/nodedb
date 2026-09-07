// SPDX-License-Identifier: BUSL-1.1

//! An identifier naming no column of a closed-schema collection raises
//! SQLSTATE `42703` at plan time, in every read clause.
//! Closed engines: `document_strict`, `kv`, `columnar`, `timeseries`,
//! `spatial`. `document_strict` carries the full clause matrix. Each other
//! closed engine gets one representative check.
//! `document_schemaless` stays open (`docs/documents.md:43`). It accepts
//! undeclared fields on write, so unknown identifiers resolve to NULL.

use crate::harness::TestServer;

async fn seed_strict(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (a INT4 PRIMARY KEY, b INT8) WITH (engine = 'document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {name} (a, b) VALUES (1, 10)"))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {name} (a, b) VALUES (2, 20)"))
        .await
        .unwrap();
}

#[tokio::test]
async fn projection_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_proj").await;
    srv.expect_error("SELECT nonexistent_col FROM uc_proj", "42703")
        .await;
}

#[tokio::test]
async fn where_equality_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_where_eq").await;
    srv.expect_error(
        "SELECT count(*) FROM uc_where_eq WHERE nonexistent_col = 1",
        "42703",
    )
    .await;
}

/// `IS NULL` on an unknown column must not fold to a silent match-all.
#[tokio::test]
async fn where_is_null_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_where_null").await;
    srv.expect_error(
        "SELECT count(*) FROM uc_where_null WHERE nonexistent_col IS NULL",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn order_by_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_order").await;
    srv.expect_error("SELECT b FROM uc_order ORDER BY nonexistent_col", "42703")
        .await;
}

#[tokio::test]
async fn group_by_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_group").await;
    srv.expect_error(
        "SELECT count(*) FROM uc_group GROUP BY nonexistent_col",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn having_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_having").await;
    srv.expect_error(
        "SELECT count(*) FROM uc_having GROUP BY a HAVING nonexistent_col > 1",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn qualified_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_qual").await;
    srv.expect_error("SELECT t.nonexistent_col FROM uc_qual AS t", "42703")
        .await;
}

#[tokio::test]
async fn join_on_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_join_l").await;
    seed_strict(&srv, "uc_join_r").await;
    srv.expect_error(
        "SELECT l.a FROM uc_join_l l JOIN uc_join_r r ON l.nonexistent_col = r.a",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn window_partition_by_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_win_part").await;
    srv.expect_error(
        "SELECT count(*) OVER (PARTITION BY nonexistent_col) FROM uc_win_part",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn window_order_by_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_win_order").await;
    srv.expect_error(
        "SELECT count(*) OVER (ORDER BY nonexistent_col) FROM uc_win_order",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn in_subquery_unknown_source_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_in_outer").await;
    seed_strict(&srv, "uc_in_source").await;
    srv.expect_error(
        "SELECT a FROM uc_in_outer WHERE a IN (SELECT nonexistent_col FROM uc_in_source)",
        "42703",
    )
    .await;
}

/// Plan-time proof: pairs an empty collection with `LIMIT 0` so no row ever
/// reaches the evaluator. An error here can only come from planning.
#[tokio::test]
async fn unknown_column_errors_with_zero_rows_scanned() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION uc_zero_rows (a INT4 PRIMARY KEY, b INT8) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();

    srv.expect_error("SELECT nonexistent_col FROM uc_zero_rows LIMIT 0", "42703")
        .await;
}

#[tokio::test]
async fn kv_engine_projection_unknown_column_errors() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION uc_kv_proj (k TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    srv.exec("INSERT INTO uc_kv_proj (k, v) VALUES ('k1', 'v1')")
        .await
        .unwrap();
    srv.expect_error("SELECT nonexistent_col FROM uc_kv_proj", "42703")
        .await;
}

#[tokio::test]
async fn kv_engine_where_unknown_column_errors() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION uc_kv_where (k TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    srv.exec("INSERT INTO uc_kv_where (k, v) VALUES ('k1', 'v1')")
        .await
        .unwrap();
    srv.expect_error(
        "SELECT k FROM uc_kv_where WHERE nonexistent_col = 'x'",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn columnar_engine_projection_unknown_column_errors() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION uc_columnar \
         COLUMNS (id TEXT, region TEXT, revenue FLOAT) \
         WITH (engine='columnar')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO uc_columnar (id, region, revenue) VALUES ('r1', 'us', 100.0)")
        .await
        .unwrap();
    srv.expect_error("SELECT nonexistent_col FROM uc_columnar", "42703")
        .await;
}

#[tokio::test]
async fn timeseries_engine_projection_unknown_column_errors() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION uc_timeseries (ts TIMESTAMP TIME_KEY, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO uc_timeseries (ts, value) VALUES ('2020-01-01 00:00:00', 1.0)")
        .await
        .unwrap();
    srv.expect_error("SELECT nonexistent_col FROM uc_timeseries", "42703")
        .await;
}

#[tokio::test]
async fn spatial_engine_projection_unknown_column_errors() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION uc_spatial \
         COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap();
    srv.exec(
        "INSERT INTO uc_spatial (id, location, name) \
         VALUES ('p1', ST_Point(-122.4, 37.8), 'SF')",
    )
    .await
    .unwrap();
    srv.expect_error("SELECT nonexistent_col FROM uc_spatial", "42703")
        .await;
}

/// Positive control: a declared column keeps selecting correctly.
#[tokio::test]
async fn declared_column_still_selects() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_declared").await;
    let rows = srv
        .query_rows("SELECT a, b FROM uc_declared ORDER BY a")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1", "10"], vec!["2", "20"]]);
}

/// Positive control: `SELECT *` still returns every row.
#[tokio::test]
async fn select_star_still_returns_rows() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_star").await;
    let rows = srv.query_rows("SELECT * FROM uc_star").await.unwrap();
    assert_eq!(rows.len(), 2);
}

/// Positive control: ORDER BY on a SELECT output alias must not be mistaken
/// for an unknown column.
#[tokio::test]
async fn order_by_output_alias_still_works() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_order_alias").await;
    let rows = srv
        .query_rows("SELECT b AS bee FROM uc_order_alias ORDER BY bee")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["10"], vec!["20"]]);
}

/// Positive control: GROUP BY on a SELECT output alias must not be mistaken
/// for an unknown column.
#[tokio::test]
async fn group_by_output_alias_still_works() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_group_alias").await;
    let rows = srv
        .query_rows("SELECT b AS bee, count(*) FROM uc_group_alias GROUP BY bee ORDER BY bee")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

/// Positive control: HAVING on a SELECT output alias must not be mistaken
/// for an unknown column.
#[tokio::test]
async fn having_output_alias_still_works() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_having_alias").await;
    let rows = srv
        .query_rows(
            "SELECT b AS bee, count(*) AS cnt FROM uc_having_alias \
             GROUP BY bee HAVING cnt > 0 ORDER BY bee",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

/// The schemaless engine accepts undeclared fields on write
/// (`docs/documents.md:43`), so a read must accept them too: an unknown
/// identifier resolves to NULL instead of erroring. This is the deliberate
/// open-schema boundary the closed-schema gate must not cross.
#[tokio::test]
async fn schemaless_unknown_column_resolves_to_null() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION uc_schemaless (id INT PRIMARY KEY, x INT)")
        .await
        .unwrap();
    srv.exec("INSERT INTO uc_schemaless (id, x) VALUES (1, 10)")
        .await
        .unwrap();
    srv.exec("INSERT INTO uc_schemaless (id, x) VALUES (2, 20)")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT nonexistent_col FROM uc_schemaless")
        .await
        .expect("an unknown identifier on a schemaless collection must not error");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(row[0].is_empty(), "expected NULL, got {row:?}");
    }

    let count = srv
        .query_text("SELECT count(*) FROM uc_schemaless WHERE nonexistent_col IS NULL")
        .await
        .unwrap();
    assert_eq!(count, vec!["2".to_string()]);
}

/// An undefined table still reports `42P01`, distinct from the `42703`
/// undefined-column path this file otherwise covers.
#[tokio::test]
async fn undefined_table_still_errors_42p01() {
    let srv = TestServer::start().await;
    srv.expect_error("SELECT * FROM uc_this_collection_does_not_exist", "42P01")
        .await;
}

/// An aggregate call's argument resolves in the same scope as any other
/// expression. An unknown column inside `SUM(...)` raises `42703`.
#[tokio::test]
async fn aggregate_argument_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_agg_arg").await;
    srv.expect_error("SELECT SUM(nonexistent_col) FROM uc_agg_arg", "42703")
        .await;
}

/// The same check inside a grouped aggregate.
#[tokio::test]
async fn grouped_aggregate_argument_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_agg_grouped").await;
    srv.expect_error(
        "SELECT a, AVG(nonexistent_col) FROM uc_agg_grouped GROUP BY a",
        "42703",
    )
    .await;
}

/// `COUNT(DISTINCT ...)` takes the same argument path.
#[tokio::test]
async fn count_distinct_unknown_column_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_agg_distinct").await;
    srv.expect_error(
        "SELECT COUNT(DISTINCT nonexistent_col) FROM uc_agg_distinct",
        "42703",
    )
    .await;
}

/// Positive control: a declared column in an aggregate still aggregates.
#[tokio::test]
async fn aggregate_argument_declared_column_still_works() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "uc_agg_ok").await;
    let rows = srv
        .query_text("SELECT SUM(b) FROM uc_agg_ok")
        .await
        .expect("SUM over a declared column must succeed");
    assert_eq!(rows.len(), 1);
}
