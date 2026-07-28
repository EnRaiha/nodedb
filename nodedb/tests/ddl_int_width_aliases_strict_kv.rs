// SPDX-License-Identifier: BUSL-1.1

//! `document_strict` and `kv` collections must accept
//! every PostgreSQL integer-width keyword in DDL *and* report each column's
//! declared width faithfully on the wire.
//!
//! Both engines validate declared column types via
//! `nodedb_types::columnar::ColumnType::from_str`
//! (`nodedb-sql/src/ddl_ast/collection_type.rs`'s `build_strict_schema` /
//! `build_kv_collection_type`), which previously recognized only
//! `BIGINT`/`INT64`/`INTEGER`/`INT` — `INT4`, `INT8`, `SMALLINT`, and `INT2`
//! were rejected as "unknown column type".
//!
//! Accepting those spellings is only half the fix. `ColumnType` deliberately
//! has one `Int64` variant for every declared width (nodedb stores all
//! integers as a full i64), so a strict/kv column's resolved type cannot say
//! how wide the author declared it. The declared width is recovered from the
//! catalog's raw `fields` entries — populated for strict and kv exactly as for
//! schemaless and columnar — and resolved to an `IntWidth` at the catalog
//! boundary. Without that, `CREATE ... (a SMALLINT)` would succeed and then
//! report OID 20, trading a loud DDL error for a silent width mismatch.
//!
//! Companion coverage: `pgwire_ddl_result_types.rs` for schemaless OID
//! fidelity, `pgwire_int_width_range_enforcement.rs` for the write-side range
//! constraint that makes these narrowed OIDs honest, and
//! `ddl_float_width_aliases_strict_kv.rs` for the float family (which has no
//! write-side constraint, because narrowing a float rounds rather than wraps).

mod common;

use common::pgwire_harness::TestServer;

/// Assert the exact `RowDescription` OID of each named column, failing loudly
/// if a column is missing entirely.
///
/// Deliberately not `if let Some(col)` — a lookup that silently skips turns
/// this into a test that passes when the columns vanish, which is precisely
/// the regression it exists to catch.
fn assert_column_oids(row: &tokio_postgres::Row, expected: &[(&str, u32)]) {
    for (col_name, expected_oid) in expected {
        let col = row
            .columns()
            .iter()
            .find(|c| c.name() == *col_name)
            .unwrap_or_else(|| {
                panic!(
                    "column '{col_name}' must appear in RowDescription; got {:?}",
                    row.columns().iter().map(|c| c.name()).collect::<Vec<_>>()
                )
            });
        assert_eq!(
            col.type_().oid(),
            *expected_oid,
            "column '{col_name}' must advertise OID {expected_oid}, got {}",
            col.type_().oid()
        );
    }
}

/// `document_strict` `CREATE COLLECTION` with every integer-width spelling
/// must succeed (previously "unknown column type: 'INT4'") and each column
/// must advertise its own declared width.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_create_collection_preserves_declared_int_widths() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION strict_int_widths (\
                id TEXT PRIMARY KEY, \
                a INT4, \
                b SMALLINT, \
                c INT2, \
                d INT8\
             ) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION with INT4/SMALLINT/INT2/INT8 columns must succeed on document_strict");

    server
        .exec("INSERT INTO strict_int_widths (id, a, b, c, d) VALUES ('r1', 1, 2, 3, 4)")
        .await
        .expect("INSERT into strict_int_widths must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT id, a, b, c, d FROM strict_int_widths WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare strict_int_widths select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute strict_int_widths select");
    assert_eq!(rows.len(), 1, "expected 1 row back from strict_int_widths");

    assert_column_oids(
        &rows[0],
        &[("a", 23), ("b", 21), ("c", 21), ("d", 20), ("id", 25)],
    );

    // Typed getters matching the advertised widths: a wrong OID or a
    // wrong-width binary payload panics inside `get` before the comparison.
    assert_eq!(rows[0].get::<_, i32>("a"), 1);
    assert_eq!(rows[0].get::<_, i16>("b"), 2);
    assert_eq!(rows[0].get::<_, i16>("c"), 3);
    assert_eq!(rows[0].get::<_, i64>("d"), 4);
}

/// `kv` `CREATE COLLECTION` with `INT4`/`SMALLINT` columns must succeed and
/// report each declared width, exactly as strict does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_create_collection_preserves_declared_int_widths() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION kv_int_widths (\
                key TEXT PRIMARY KEY, \
                a INT4, \
                b SMALLINT\
             ) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION with INT4/SMALLINT columns must succeed on kv");

    server
        .exec("INSERT INTO kv_int_widths (key, a, b) VALUES ('r1', 1, 2)")
        .await
        .expect("INSERT into kv_int_widths must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT key, a, b FROM kv_int_widths WHERE key = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare kv_int_widths select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute kv_int_widths select");
    assert_eq!(rows.len(), 1, "expected 1 row back from kv_int_widths");

    assert_column_oids(&rows[0], &[("a", 23), ("b", 21)]);
    assert_eq!(rows[0].get::<_, i32>("a"), 1);
    assert_eq!(rows[0].get::<_, i16>("b"), 2);
}

/// A column whose declared type carries no width (`BIGINT`) stays at OID 20,
/// and a non-integer column is untouched by width resolution — the narrowing
/// must apply to declared narrow integers only, never by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undeclared_and_non_integer_widths_are_untouched() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION strict_mixed_widths (\
                id TEXT PRIMARY KEY, \
                big BIGINT, \
                label TEXT, \
                ratio DOUBLE, \
                flag BOOL\
             ) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION strict_mixed_widths must succeed");

    server
        .exec(
            "INSERT INTO strict_mixed_widths (id, big, label, ratio, flag) \
             VALUES ('r1', 9876543210, 'x', 1.5, true)",
        )
        .await
        .expect("INSERT into strict_mixed_widths must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT id, big, label, ratio, flag FROM strict_mixed_widths WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare strict_mixed_widths select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute strict_mixed_widths select");
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row back from strict_mixed_widths"
    );

    assert_column_oids(
        &rows[0],
        &[
            ("big", 20),
            ("label", 25),
            ("ratio", 701),
            ("flag", 16),
            ("id", 25),
        ],
    );
    // A BIGINT column must still carry values no narrower type could hold.
    assert_eq!(rows[0].get::<_, i64>("big"), 9876543210);
}
