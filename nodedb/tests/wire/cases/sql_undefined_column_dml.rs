// SPDX-License-Identifier: BUSL-1.1

//! A write predicate or assignment target naming no column of a
//! closed-schema collection raises SQLSTATE `42703` at plan time.
//! Each test asserts the error and that stored rows are unchanged.
//! A silently-skipped write and a silently-wiped table are the two
//! outcomes guarded against.

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

async fn rows_of(server: &TestServer, name: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!("SELECT a, b FROM {name} ORDER BY a"))
        .await
        .unwrap()
}

#[tokio::test]
async fn update_where_unknown_column_errors_and_leaves_rows_unchanged() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_upd_where").await;

    srv.expect_error(
        "UPDATE ucd_upd_where SET b = 99 WHERE nonexistent_col = 1",
        "42703",
    )
    .await;

    let rows = rows_of(&srv, "ucd_upd_where").await;
    assert_eq!(rows, vec![vec!["1", "10"], vec!["2", "20"]]);
}

#[tokio::test]
async fn update_set_unknown_target_errors() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_upd_target").await;

    srv.expect_error(
        "UPDATE ucd_upd_target SET nonexistent_col = 1 WHERE a = 1",
        "42703",
    )
    .await;

    let rows = rows_of(&srv, "ucd_upd_target").await;
    assert_eq!(rows, vec![vec!["1", "10"], vec!["2", "20"]]);
}

#[tokio::test]
async fn update_set_unknown_rhs_errors_and_leaves_column_unchanged() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_upd_rhs").await;

    srv.expect_error(
        "UPDATE ucd_upd_rhs SET b = nonexistent_col WHERE a = 1",
        "42703",
    )
    .await;

    let rows = rows_of(&srv, "ucd_upd_rhs").await;
    assert_eq!(rows[0], vec!["1", "10"]);
    assert_ne!(rows[0][1], "", "b must keep its value");
}

#[tokio::test]
async fn delete_where_unknown_column_errors_and_row_count_unchanged() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_del_where").await;

    srv.expect_error(
        "DELETE FROM ucd_del_where WHERE nonexistent_col = 1",
        "42703",
    )
    .await;

    let rows = rows_of(&srv, "ucd_del_where").await;
    assert_eq!(rows.len(), 2);
}

/// The catastrophic full-wipe shape: an unknown column folding to NULL
/// turns `IS NULL` into an unqualified `DELETE`. Every original row
/// must survive.
#[tokio::test]
async fn delete_where_is_null_on_unknown_column_errors_and_wipes_nothing() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_del_isnull").await;

    srv.expect_error(
        "DELETE FROM ucd_del_isnull WHERE nonexistent_col IS NULL",
        "42703",
    )
    .await;

    let rows = rows_of(&srv, "ucd_del_isnull").await;
    assert_eq!(rows, vec![vec!["1", "10"], vec!["2", "20"]]);
}

#[tokio::test]
async fn kv_update_where_unknown_column_errors_and_value_unchanged() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ucd_kv_upd (k TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    srv.exec("INSERT INTO ucd_kv_upd (k, v) VALUES ('k1', 'orig')")
        .await
        .unwrap();

    srv.expect_error(
        "UPDATE ucd_kv_upd SET v = 'changed' WHERE nonexistent_col = 'x'",
        "42703",
    )
    .await;

    let rows = srv.query_rows("SELECT k, v FROM ucd_kv_upd").await.unwrap();
    assert_eq!(rows, vec![vec!["k1", "orig"]]);
}

#[tokio::test]
async fn kv_delete_where_unknown_column_errors_and_row_survives() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ucd_kv_del (k TEXT PRIMARY KEY, v TEXT) WITH (engine = 'kv')")
        .await
        .unwrap();
    srv.exec("INSERT INTO ucd_kv_del (k, v) VALUES ('k1', 'orig')")
        .await
        .unwrap();

    srv.expect_error(
        "DELETE FROM ucd_kv_del WHERE nonexistent_col = 'x'",
        "42703",
    )
    .await;

    let rows = srv.query_rows("SELECT k FROM ucd_kv_del").await.unwrap();
    assert_eq!(rows, vec![vec!["k1"]]);
}

/// `INSERT ... SELECT` plans its source through the ordinary SELECT path, so an
/// unknown column in the source `WHERE` must raise `42703`. The target stays
/// empty: an unknown column folding to NULL copies zero rows and reports success.
#[tokio::test]
async fn insert_select_unknown_source_column_errors_and_target_stays_empty() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_insel_src").await;
    srv.exec(
        "CREATE COLLECTION ucd_insel_dst (a INT4 PRIMARY KEY) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();

    srv.expect_error(
        "INSERT INTO ucd_insel_dst SELECT * FROM ucd_insel_src WHERE nonexistent_col = 1",
        "42703",
    )
    .await;

    let rows = rows_of_single(&srv, "ucd_insel_dst").await;
    assert!(rows.is_empty(), "target must stay empty, got {rows:?}");
}

async fn rows_of_single(server: &TestServer, name: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!("SELECT a FROM {name}"))
        .await
        .unwrap()
}

/// An explicit target column list does not open the source projection: an
/// unknown column named in the `SELECT` list must still raise `42703`, and
/// the target stays empty.
#[tokio::test]
async fn insert_select_explicit_projection_unknown_column_errors_and_target_stays_empty() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_insel_proj_src").await;
    srv.exec(
        "CREATE COLLECTION ucd_insel_proj_dst (a INT4 PRIMARY KEY) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();

    srv.expect_error(
        "INSERT INTO ucd_insel_proj_dst (a) SELECT nonexistent_col FROM ucd_insel_proj_src",
        "42703",
    )
    .await;

    let rows = rows_of_single(&srv, "ucd_insel_proj_dst").await;
    assert!(rows.is_empty(), "target must stay empty, got {rows:?}");
}

/// An explicit target column list does not open the source `WHERE` clause
/// either: an unknown column there must raise `42703`, and the target
/// stays empty.
#[tokio::test]
async fn insert_select_explicit_projection_unknown_where_column_errors_and_target_stays_empty() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_insel_projwhere_src").await;
    srv.exec(
        "CREATE COLLECTION ucd_insel_projwhere_dst (a INT4 PRIMARY KEY) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();

    srv.expect_error(
        "INSERT INTO ucd_insel_projwhere_dst (a) SELECT a FROM ucd_insel_projwhere_src WHERE nonexistent_col = 1",
        "42703",
    )
    .await;

    let rows = rows_of_single(&srv, "ucd_insel_projwhere_dst").await;
    assert!(rows.is_empty(), "target must stay empty, got {rows:?}");
}

/// Positive control: an explicit target column list over a real source
/// column succeeds and copies every source row.
#[tokio::test]
async fn insert_select_explicit_projection_known_column_copies_all_rows() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_insel_ok_src").await;
    srv.exec(
        "CREATE COLLECTION ucd_insel_ok_dst (a INT4 PRIMARY KEY) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO ucd_insel_ok_dst (a) SELECT a FROM ucd_insel_ok_src")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT a FROM ucd_insel_ok_dst ORDER BY a")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1"], vec!["2"]]);
}

/// Positive control: a `WHERE` clause on a real source column copies
/// exactly the matching row.
#[tokio::test]
async fn insert_select_explicit_projection_known_where_copies_matching_row() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_insel_okwhere_src").await;
    srv.exec(
        "CREATE COLLECTION ucd_insel_okwhere_dst (a INT4 PRIMARY KEY) WITH (engine = 'document_strict')",
    )
    .await
    .unwrap();

    srv.exec(
        "INSERT INTO ucd_insel_okwhere_dst (a) SELECT a FROM ucd_insel_okwhere_src WHERE a = 1",
    )
    .await
    .unwrap();

    let rows = srv
        .query_rows("SELECT a FROM ucd_insel_okwhere_dst")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1"]]);
}

async fn create_merge_target(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION ucd_merge_target (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                score INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
}

async fn create_merge_source(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION ucd_merge_source (\
                id TEXT PRIMARY KEY, \
                name TEXT, \
                score INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn merge_on_condition_unknown_column_errors() {
    let srv = TestServer::start().await;
    create_merge_target(&srv).await;
    create_merge_source(&srv).await;
    srv.exec("INSERT INTO ucd_merge_target (id, name, score) VALUES ('a', 'alpha', 10)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ucd_merge_source (id, name, score) VALUES ('a', 'ALPHA_UPD', 99)")
        .await
        .unwrap();

    srv.expect_error(
        "MERGE INTO ucd_merge_target t \
         USING ucd_merge_source s ON t.nonexistent_col = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
        "42703",
    )
    .await;
}

#[tokio::test]
async fn merge_when_matched_update_set_unknown_column_errors() {
    let srv = TestServer::start().await;
    create_merge_target(&srv).await;
    create_merge_source(&srv).await;
    srv.exec("INSERT INTO ucd_merge_target (id, name, score) VALUES ('a', 'alpha', 10)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ucd_merge_source (id, name, score) VALUES ('a', 'ALPHA_UPD', 99)")
        .await
        .unwrap();

    srv.expect_error(
        "MERGE INTO ucd_merge_target t \
         USING ucd_merge_source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET nonexistent_col = s.name",
        "42703",
    )
    .await;
}

/// Positive control: UPDATE on a real column applies on `document_strict`.
#[tokio::test]
async fn update_on_declared_column_still_applies() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_upd_ok").await;

    srv.exec("UPDATE ucd_upd_ok SET b = 99 WHERE a = 1")
        .await
        .unwrap();

    let rows = rows_of(&srv, "ucd_upd_ok").await;
    assert_eq!(rows, vec![vec!["1", "99"], vec!["2", "20"]]);
}

/// Positive control: DELETE on a real column removes exactly the matching
/// row.
#[tokio::test]
async fn delete_on_declared_column_still_removes_matching_row() {
    let srv = TestServer::start().await;
    seed_strict(&srv, "ucd_del_ok").await;

    srv.exec("DELETE FROM ucd_del_ok WHERE a = 1")
        .await
        .unwrap();

    let rows = rows_of(&srv, "ucd_del_ok").await;
    assert_eq!(rows, vec![vec!["2", "20"]]);
}

/// The schemaless engine treats undeclared fields as NULL by design, so an
/// `UPDATE ... WHERE nonexistent_col IS NULL` succeeds and matches every
/// row instead of erroring. This is the deliberate open-schema boundary
/// the closed-schema write gate must not cross.
#[tokio::test]
async fn schemaless_update_where_unknown_column_is_null_succeeds() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION ucd_schemaless (id INT PRIMARY KEY, x INT)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ucd_schemaless (id, x) VALUES (1, 1)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ucd_schemaless (id, x) VALUES (2, 1)")
        .await
        .unwrap();

    srv.exec("UPDATE ucd_schemaless SET x = 99 WHERE nonexistent_col IS NULL")
        .await
        .expect("an unknown identifier folding to NULL must not error on schemaless");

    let rows = srv
        .query_rows("SELECT id, x FROM ucd_schemaless ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec!["1", "99"], vec!["2", "99"]]);
}
