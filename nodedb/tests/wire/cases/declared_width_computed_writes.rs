// SPDX-License-Identifier: BUSL-1.1

//! A declared numeric column refuses a value past its declared width on every
//! write that stores one, with SQLSTATE 22003, and the row keeps its value.
//!
//! - Strict and columnar schemas carry the declared `SMALLINT` / `REAL`
//!   width. An INSERT and a computed UPDATE meet the same rule.
//! - KV operations that compute the value they store (`KV_INCR`,
//!   `KV_INCR_FLOAT`, `KV_CAS`, `KV_GETSET`, `TRANSFER`) meet the
//!   collection's declared columns before they store it.

use crate::harness::TestServer;

const OUT_OF_RANGE: &str = "SQLSTATE 22003";

/// The single cell `sql` reads.
async fn cell(srv: &TestServer, sql: &str) -> String {
    let rows = srv.query_rows(sql).await.unwrap();
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    rows[0][0].clone()
}

/// The strict and columnar `CREATE` statements for a collection `name` with
/// an `id` key and a `v` column of `declared` type.
fn typed_engines(name: &str, declared: &str) -> [(String, String); 2] {
    [
        (
            format!("{name}_strict"),
            format!(
                "CREATE COLLECTION {name}_strict (id TEXT PRIMARY KEY, v {declared}) \
                 WITH (engine='document_strict')"
            ),
        ),
        (
            format!("{name}_columnar"),
            format!(
                "CREATE COLLECTION {name}_columnar COLUMNS (id TEXT, v {declared}) \
                 WITH (engine='columnar')"
            ),
        ),
    ]
}

/// Assert `name`'s row `'a'` still holds `1` after a refused UPDATE.
async fn assert_row_kept(srv: &TestServer, name: &str) {
    assert_eq!(
        cell(srv, &format!("SELECT v FROM {name} WHERE id = 'a'")).await,
        "1",
        "{name}: the refused UPDATE keeps the row's value"
    );
}

/// A strict or columnar `SMALLINT` column refuses `40000` on INSERT and on
/// UPDATE, and advertises `int2` on the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smallint_refuses_a_value_past_its_width_on_insert_and_update() {
    let srv = TestServer::start().await;
    for (name, create) in typed_engines("dw_small", "SMALLINT") {
        srv.exec(&create).await.unwrap();
        srv.exec(&format!("INSERT INTO {name} (id, v) VALUES ('a', 1)"))
            .await
            .unwrap();

        srv.expect_error(
            &format!("INSERT INTO {name} (id, v) VALUES ('b', 40000)"),
            OUT_OF_RANGE,
        )
        .await;
        srv.expect_error(
            &format!("UPDATE {name} SET v = 40000 WHERE id = 'a'"),
            OUT_OF_RANGE,
        )
        .await;
        assert_row_kept(&srv, &name).await;
        assert!(
            srv.query_rows(&format!("SELECT v FROM {name} WHERE id = 'b'"))
                .await
                .unwrap()
                .is_empty(),
            "{name}: the refused INSERT stores no row"
        );

        let stmt = srv
            .client
            .prepare(&format!("SELECT v FROM {name} LIMIT 0"))
            .await
            .expect("prepare describe");
        assert_eq!(
            stmt.columns()[0].type_().oid(),
            21,
            "{name}: a SMALLINT column advertises int2"
        );
    }
}

/// A computed UPDATE past a strict `SMALLINT` is refused. The planner
/// validates only a literal, so the Data Plane encoder enforces the width.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_smallint_refuses_a_computed_update_past_its_width() {
    let srv = TestServer::start().await;
    let [(name, create), _] = typed_engines("dw_cstrict", "SMALLINT");
    srv.exec(&create).await.unwrap();
    srv.exec(&format!("INSERT INTO {name} (id, v) VALUES ('a', 1)"))
        .await
        .unwrap();
    srv.expect_error(
        &format!("UPDATE {name} SET v = v + 39999 WHERE id = 'a'"),
        OUT_OF_RANGE,
    )
    .await;
    assert_row_kept(&srv, &name).await;
}

/// A computed UPDATE past a columnar `SMALLINT` is refused. The columnar
/// UPDATE handlers fit every post-image to the schema before the first row
/// changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_smallint_refuses_a_computed_update_past_its_width() {
    let srv = TestServer::start().await;
    let [_, (name, create)] = typed_engines("dw_ccol", "SMALLINT");
    srv.exec(&create).await.unwrap();
    srv.exec(&format!("INSERT INTO {name} (id, v) VALUES ('a', 1)"))
        .await
        .unwrap();
    srv.expect_error(
        &format!("UPDATE {name} SET v = v + 39999 WHERE id = 'a'"),
        OUT_OF_RANGE,
    )
    .await;
    assert_row_kept(&srv, &name).await;
}

/// A strict or columnar `REAL` column refuses `1e39`, past `f32`, on INSERT
/// and on UPDATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_refuses_a_value_past_f32() {
    let srv = TestServer::start().await;
    for (name, create) in typed_engines("dw_real", "REAL") {
        srv.exec(&create).await.unwrap();
        srv.exec(&format!("INSERT INTO {name} (id, v) VALUES ('a', 1.5)"))
            .await
            .unwrap();

        srv.expect_error(
            &format!("INSERT INTO {name} (id, v) VALUES ('b', 1e39)"),
            OUT_OF_RANGE,
        )
        .await;
        srv.expect_error(
            &format!("UPDATE {name} SET v = 1e39 WHERE id = 'a'"),
            OUT_OF_RANGE,
        )
        .await;

        assert_eq!(
            cell(&srv, &format!("SELECT v FROM {name} WHERE id = 'a'")).await,
            "1.5",
            "{name}"
        );
    }
}

/// Narrowing a strict column's declared width is refused: existing rows can
/// already hold values past the narrower width.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn narrowing_a_declared_width_is_refused() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION dw_narrow (id TEXT PRIMARY KEY, v INT) \
         WITH (engine='document_strict')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO dw_narrow (id, v) VALUES ('a', 40000)")
        .await
        .unwrap();
    srv.expect_error(
        "ALTER COLLECTION dw_narrow ALTER COLUMN v TYPE SMALLINT",
        "SQLSTATE 0A000",
    )
    .await;
    assert_eq!(
        cell(&srv, "SELECT v FROM dw_narrow WHERE id = 'a'").await,
        "40000"
    );
}

/// `KV_INCR` past a declared `SMALLINT` is refused, and the row keeps its
/// value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_past_a_declared_smallint_is_refused() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION dw_kv_incr (key TEXT PRIMARY KEY, n SMALLINT, label TEXT) \
         WITH (engine='kv')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO dw_kv_incr (key, n, label) VALUES ('a', 32767, 'x')")
        .await
        .unwrap();

    srv.expect_error("SELECT KV_INCR('dw_kv_incr', 'a', 1)", OUT_OF_RANGE)
        .await;
    assert_eq!(
        cell(&srv, "SELECT n FROM dw_kv_incr WHERE key = 'a'").await,
        "32767"
    );

    srv.query_text("SELECT KV_INCR('dw_kv_incr', 'a', -1)")
        .await
        .unwrap();
    assert_eq!(
        cell(&srv, "SELECT n FROM dw_kv_incr WHERE key = 'a'").await,
        "32766"
    );
}

/// `KV_INCR_FLOAT` on a declared `DECIMAL(5,2)` value rounds to the declared
/// scale, and a result past the declared precision is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_incr_float_meets_a_declared_decimal() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION dw_kv_dec (key TEXT PRIMARY KEY, value DECIMAL(5,2)) \
         WITH (engine='kv')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO dw_kv_dec (key, value) VALUES ('a', 999.99)")
        .await
        .unwrap();
    srv.exec("INSERT INTO dw_kv_dec (key, value) VALUES ('b', 1.50)")
        .await
        .unwrap();

    srv.expect_error("SELECT KV_INCR_FLOAT('dw_kv_dec', 'a', 1)", OUT_OF_RANGE)
        .await;
    assert_eq!(
        cell(&srv, "SELECT value FROM dw_kv_dec WHERE key = 'a'").await,
        "999.99"
    );

    srv.query_text("SELECT KV_INCR_FLOAT('dw_kv_dec', 'b', 0.005)")
        .await
        .unwrap();
    assert_eq!(
        cell(&srv, "SELECT value FROM dw_kv_dec WHERE key = 'b'").await,
        "1.51"
    );
}

/// `KV_CAS` and `KV_GETSET` that would store a value past a declared
/// `SMALLINT` are refused, and the row keeps its value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_cas_and_getset_past_a_declared_smallint_are_refused() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION dw_kv_swap (key TEXT PRIMARY KEY, value SMALLINT) \
         WITH (engine='kv')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO dw_kv_swap (key, value) VALUES ('a', 5)")
        .await
        .unwrap();

    srv.expect_error(
        "SELECT KV_CAS('dw_kv_swap', 'a', '5', '40000')",
        OUT_OF_RANGE,
    )
    .await;
    srv.expect_error("SELECT KV_GETSET('dw_kv_swap', 'a', '40000')", OUT_OF_RANGE)
        .await;
    assert_eq!(
        cell(&srv, "SELECT value FROM dw_kv_swap WHERE key = 'a'").await,
        "5"
    );
}

/// A `TRANSFER` whose credit takes a declared `SMALLINT` balance past its
/// width is refused, and neither balance moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_transfer_past_a_declared_smallint_is_refused() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION dw_kv_acct (key TEXT PRIMARY KEY, balance SMALLINT, note TEXT) \
         WITH (engine='kv')",
    )
    .await
    .unwrap();
    srv.exec("INSERT INTO dw_kv_acct (key, balance, note) VALUES ('a', 100, 'x')")
        .await
        .unwrap();
    srv.exec("INSERT INTO dw_kv_acct (key, balance, note) VALUES ('b', 32700, 'y')")
        .await
        .unwrap();

    srv.expect_error(
        "SELECT TRANSFER('dw_kv_acct', 'a', 'b', 'balance', 100)",
        OUT_OF_RANGE,
    )
    .await;
    assert_eq!(
        cell(&srv, "SELECT balance FROM dw_kv_acct WHERE key = 'a'").await,
        "100"
    );
    assert_eq!(
        cell(&srv, "SELECT balance FROM dw_kv_acct WHERE key = 'b'").await,
        "32700"
    );
}
