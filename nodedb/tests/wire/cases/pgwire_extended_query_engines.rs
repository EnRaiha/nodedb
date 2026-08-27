// SPDX-License-Identifier: BUSL-1.1

//! Extended-query protocol coverage for KV, Columnar, and Timeseries engines.
//!
//! Spatial, Vector, Array, and error-path tests are in
//! `pgwire_extended_query_engines2.rs`.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::types::Type;

// ── Key-Value engine ─────────────────────────────────────────────────────────

/// KV point-get with typed text parameter; verifies column OIDs.
#[tokio::test]
async fn extended_query_kv_typed_text_param() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_txt (key STRING PRIMARY KEY, val STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION kv_txt");
    srv.exec("INSERT INTO kv_txt (key, val) VALUES ('hello', 'world')")
        .await
        .expect("INSERT kv_txt");

    let stmt = srv
        .client
        .prepare_typed("SELECT key, val FROM kv_txt WHERE key = $1", &[Type::TEXT])
        .await
        .expect("prepare kv_txt select");

    let rows = srv
        .client
        .query(&stmt, &[&"hello"])
        .await
        .expect("execute kv_txt select");

    assert_eq!(rows.len(), 1, "expected 1 row");
    let k: &str = rows[0].get("key");
    let v: &str = rows[0].get("val");
    assert_eq!(k, "hello");
    assert_eq!(v, "world");

    // RowDescription OID check: both columns are TEXT (OID 25).
    for col in rows[0].columns() {
        assert_eq!(
            col.type_().oid(),
            25,
            "KV TEXT column '{}' must have OID 25 (text), got {}",
            col.name(),
            col.type_().oid()
        );
    }
}

/// KV with an integer value column; verifies int8 OID (20) in RowDescription
/// via a zero-row prepared statement and that the key lookup succeeds.
#[tokio::test]
async fn extended_query_kv_integer_value() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_int (key STRING PRIMARY KEY, score INT) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION kv_int");
    srv.exec("INSERT INTO kv_int (key, score) VALUES ('a', 42)")
        .await
        .expect("INSERT kv_int");

    // Verify RowDescription OID via a LIMIT 0 prepare — no row data deserialized.
    let meta_stmt = srv
        .client
        .prepare("SELECT key, score FROM kv_int LIMIT 0")
        .await
        .expect("prepare kv_int meta");
    let col_score = meta_stmt
        .columns()
        .iter()
        .find(|c| c.name() == "score")
        .expect("score column must appear in RowDescription");
    // `score` is declared `INT`, the INT4 alias — this column previously
    // advertised OID 20 (int8) for every declared integer width. KV resolves
    // declared widths through the same catalog path as every other engine, so
    // it now correctly narrows to OID 23 (int4).
    assert_eq!(
        col_score.type_().oid(),
        23,
        "KV INT column 'score' must have OID 23 (int4), got {}",
        col_score.type_().oid()
    );

    // Verify the row is reachable via a parameterised text key lookup.
    let rows = srv
        .client
        .query("SELECT key FROM kv_int WHERE key = $1", &[&"a"])
        .await
        .expect("execute kv_int key lookup");
    assert_eq!(rows.len(), 1, "expected 1 row");
    let key: &str = rows[0].get("key");
    assert_eq!(key, "a");
}

/// KV NULL parameter: WHERE key = NULL must return 0 rows (not panic).
#[tokio::test]
async fn extended_query_kv_null_param() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION kv_null (key STRING PRIMARY KEY, val STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION kv_null");
    srv.exec("INSERT INTO kv_null (key, val) VALUES ('x', 'y')")
        .await
        .expect("INSERT kv_null");

    let stmt = srv
        .client
        .prepare_typed("SELECT key FROM kv_null WHERE key = $1", &[Type::TEXT])
        .await
        .expect("prepare kv_null");

    let null_key: Option<&str> = None;
    let rows = srv
        .client
        .query(&stmt, &[&null_key])
        .await
        .expect("null param on KV must not panic");

    assert_eq!(rows.len(), 0, "NULL param must match 0 rows");
}

/// Same $1 referenced twice — parameter reuse.
#[tokio::test]
async fn extended_query_kv_param_reuse() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION kv_reuse (key STRING PRIMARY KEY, alias STRING) WITH (engine='kv')",
    )
    .await
    .expect("CREATE kv_reuse");
    srv.exec("INSERT INTO kv_reuse (key, alias) VALUES ('dup', 'dup')")
        .await
        .expect("INSERT kv_reuse");

    let rows = srv
        .client
        .query(
            "SELECT key FROM kv_reuse WHERE key = $1 AND alias = $1",
            &[&"dup"],
        )
        .await
        .expect("param-reuse query should succeed");

    assert_eq!(rows.len(), 1, "param-reuse must match the single row");
}

// ── Columnar engine ──────────────────────────────────────────────────────────

/// Columnar multi-type scan with typed parameter; verify OIDs in RowDescription.
#[tokio::test]
async fn extended_query_columnar_typed_scan() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION col_typed (\
            id TEXT PRIMARY KEY, \
            n INT, \
            score FLOAT, \
            flag BOOL, \
            label TEXT \
         ) WITH (engine='columnar')",
    )
    .await
    .expect("CREATE col_typed");
    srv.exec(
        "INSERT INTO col_typed (id, n, score, flag, label) VALUES ('r1', 7, 3.14, true, 'alpha')",
    )
    .await
    .expect("INSERT col_typed");

    let stmt = srv
        .client
        .prepare_typed(
            "SELECT id, n, score, flag, label FROM col_typed WHERE id = $1",
            &[Type::TEXT],
        )
        .await
        .expect("prepare col_typed scan");

    let rows = srv
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute col_typed scan");

    assert_eq!(rows.len(), 1, "expected 1 columnar row");
    assert!(
        rows[0].len() >= 5,
        "expected ≥5 columns, got {}",
        rows[0].len()
    );

    let id: &str = rows[0].get("id");
    assert_eq!(id, "r1");

    // RowDescription: id→TEXT(25), n→INT4(23), score→FLOAT8(701), flag→BOOL(16), label→TEXT(25).
    // `n` is declared `INT` in the DDL above, which is the INT4 alias — this
    // column previously advertised OID 20 (int8) for every declared
    // integer width; it now correctly narrows to OID 23 (int4).
    let expected: &[(&str, u32)] = &[
        ("id", 25),
        ("n", 23),
        ("score", 701),
        ("flag", 16),
        ("label", 25),
    ];
    for (col_name, expected_oid) in expected {
        if let Some(col) = rows[0].columns().iter().find(|c| c.name() == *col_name) {
            assert_eq!(
                col.type_().oid(),
                *expected_oid,
                "columnar column '{}' OID must be {}, got {}",
                col_name,
                expected_oid,
                col.type_().oid()
            );
        }
    }
}

/// Columnar INT filter with typed INT8 param.
#[tokio::test]
async fn extended_query_columnar_int_filter() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION col_ints (id TEXT PRIMARY KEY, v INT) WITH (engine='columnar')")
        .await
        .expect("CREATE col_ints");
    for i in 1i64..=5 {
        srv.exec(&format!(
            "INSERT INTO col_ints (id, v) VALUES ('r{i}', {i})"
        ))
        .await
        .expect("INSERT col_ints");
    }

    let stmt = srv
        .client
        .prepare_typed("SELECT id FROM col_ints WHERE v = $1", &[Type::UNKNOWN])
        .await
        .expect("prepare col_ints int filter");

    let rows = srv
        .client
        .query(&stmt, &[&"3"])
        .await
        .expect("execute col_ints int filter");

    assert_eq!(rows.len(), 1, "int8 filter must match exactly one row");
    let id: &str = rows[0].get("id");
    assert_eq!(id, "r3");
}

// ── Timeseries engine ────────────────────────────────────────────────────────

/// Timeseries point-lookup by parameterised text id; verify columns are decoded.
#[tokio::test]
async fn extended_query_timeseries_typed_scan() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_events (\
            id TEXT, ts TIMESTAMP TIME_KEY, value FLOAT, region TEXT\
         ) WITH (engine='timeseries')",
    )
    .await
    .expect("CREATE ts_events");
    srv.exec(
        "INSERT INTO ts_events (id, ts, value, region) \
         VALUES ('e1', '2024-01-01T00:00:00Z', 42.0, 'us')",
    )
    .await
    .expect("INSERT ts_events");

    let rows = srv
        .client
        .query(
            "SELECT id, value, region FROM ts_events WHERE id = $1",
            &[&"e1"],
        )
        .await
        .expect("execute ts_events select");

    assert_eq!(rows.len(), 1, "timeseries: expected 1 row");
    assert!(
        rows[0].len() >= 3,
        "timeseries row must expose ≥3 columns, got {}",
        rows[0].len()
    );
    let id: &str = rows[0].get("id");
    assert_eq!(id, "e1");
}

/// Timeseries NULL text param — must not panic.
#[tokio::test]
async fn extended_query_timeseries_null_param() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION ts_null_test (\
            id TEXT, ts TIMESTAMP TIME_KEY, v FLOAT\
         ) WITH (engine='timeseries')",
    )
    .await
    .expect("CREATE ts_null_test");
    srv.exec("INSERT INTO ts_null_test (id, ts, v) VALUES ('a', '2024-01-01T00:00:00Z', 1.0)")
        .await
        .expect("INSERT ts_null_test");

    let stmt = srv
        .client
        .prepare_typed("SELECT id FROM ts_null_test WHERE id = $1", &[Type::TEXT])
        .await
        .expect("prepare ts_null_test");

    let null_param: Option<&str> = None;
    let rows = srv
        .client
        .query(&stmt, &[&null_param])
        .await
        .expect("null param on timeseries must not panic");

    assert_eq!(rows.len(), 0, "NULL param must match 0 rows");
}

// ── Schemaless engine (binary round-trip) ────────────────────────────────────

/// Binary-format round-trip for declared integer widths on a schemaless
/// collection. tokio_postgres decodes extended-query results per the
/// RowDescription OID, so reading each column back with a typed getter that
/// matches the *advertised* width (`i16`/`i32`/`i64`) fails the `get` if
/// either the OID or the binary payload width is wrong — this locks the
/// whole chain: declared width → OID 21/23/20 → binary encoder i16/i32/i64.
#[tokio::test]
async fn extended_query_schemaless_int_width_binary_roundtrip() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION bin_widths (id TEXT PRIMARY KEY, s SMALLINT, i INT, b BIGINT)")
        .await
        .expect("CREATE COLLECTION bin_widths");
    srv.exec("INSERT INTO bin_widths (id, s, i, b) VALUES ('r1', 123, 456789, 9876543210)")
        .await
        .expect("INSERT bin_widths");

    let stmt = srv
        .client
        .prepare_typed(
            "SELECT id, s, i, b FROM bin_widths WHERE id = $1",
            &[Type::TEXT],
        )
        .await
        .expect("prepare bin_widths select");

    let rows = srv
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute bin_widths select");

    assert_eq!(rows.len(), 1, "expected 1 row");

    // Typed getters matching the advertised OIDs — a wrong OID or a
    // wrong-width binary payload panics inside `get` before we ever reach
    // the assertions below.
    let s = rows[0].get::<_, i16>("s");
    let i = rows[0].get::<_, i32>("i");
    let b = rows[0].get::<_, i64>("b");
    assert_eq!(s, 123);
    assert_eq!(i, 456789);
    assert_eq!(b, 9876543210);

    // RowDescription OID check: s→INT2(21), i→INT4(23), b→INT8(20).
    let expected: &[(&str, u32)] = &[("s", 21), ("i", 23), ("b", 20)];
    for (col_name, expected_oid) in expected {
        let col = rows[0]
            .columns()
            .iter()
            .find(|c| c.name() == *col_name)
            .unwrap_or_else(|| panic!("column '{col_name}' must appear in RowDescription"));
        assert_eq!(
            col.type_().oid(),
            *expected_oid,
            "bin_widths column '{}' OID must be {}, got {}",
            col_name,
            expected_oid,
            col.type_().oid()
        );
    }
}
