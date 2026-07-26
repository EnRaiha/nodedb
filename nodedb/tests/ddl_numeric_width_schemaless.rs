// SPDX-License-Identifier: BUSL-1.1

//! The schemaless-document counterpart to `ddl_int_width_aliases_strict_kv.rs`
//! and `ddl_float_width_aliases_strict_kv.rs`.
//!
//! A `CREATE COLLECTION` with no `WITH (engine=...)` clause is a schemaless
//! document collection, and its declared column types are recorded in the
//! catalog's `fields` exactly as they are for every other engine — so
//! `RowDescription` advertises `real` (OID 700) for a `REAL` column and `int4`
//! (OID 23) for an `INT` one, and a client reading either in binary format
//! decodes exactly four bytes.
//!
//! Advertising that width is only honest if the stored cell is a number of the
//! declared family to begin with. Schemaless shares the key-value engine's
//! property of persisting the planner's value verbatim — it has no typed write
//! path re-typing each field against a schema, as the strict, columnar,
//! timeseries, and spatial engines do. A fractional SQL literal resolves to an
//! exact `Decimal` (the parser prefers exact arithmetic over a lossy `f64`),
//! which has no msgpack scalar form and serializes as a *string*: the column
//! then holds `"1.5"` under an OID that promises four bytes of `real`, and the
//! encoder — correctly refusing to put a non-number behind a numeric OID —
//! transmits SQL NULL. The stored value is lost in flight with nothing
//! signalled.
//!
//! Declared column types are therefore applied to `VALUES` literals when the
//! plan is built (`nodedb_sql::planner::declared_type_coerce`), for both
//! numeric families at once. This file pins both halves on the engine that has
//! no other line of defence.

mod common;

use common::pgwire_harness::TestServer;

/// PostgreSQL type OIDs: `real`, `double precision`, `int4`, `int8`.
const FLOAT4_OID: u32 = 700;
const FLOAT8_OID: u32 = 701;
const INT4_OID: u32 = 23;
const INT8_OID: u32 = 20;

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

/// A schemaless collection — no `WITH (engine=...)` clause at all — must report
/// each declared numeric width AND return a payload of that width's byte
/// count.
///
/// The `r REAL` column is the case that fails without the declared-type
/// coercion: OID 700 is advertised correctly, but `1.5` reaches storage as the
/// decimal's text form and the row arrives as NULL. `n INT` is the integer
/// half of the same contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_declared_numeric_widths_round_trip_at_their_widths() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION schemaless_widths (id TEXT PRIMARY KEY, r REAL, n INT)")
        .await
        .expect("CREATE COLLECTION with no engine clause must succeed");

    server
        .exec("INSERT INTO schemaless_widths (id, r, n) VALUES ('r1', 1.5, 7)")
        .await
        .expect("INSERT into schemaless_widths must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT r, n FROM schemaless_widths WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare schemaless_widths select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute schemaless_widths select");
    assert_eq!(rows.len(), 1, "expected 1 row back from schemaless_widths");

    assert_column_oids(&rows[0], &[("r", FLOAT4_OID), ("n", INT4_OID)]);
    // Typed getters matching the advertised widths: a wrong OID, a NULL, or a
    // wrong-width payload all fail inside `get` before the comparison runs.
    assert_eq!(rows[0].get::<_, f32>("r"), 1.5);
    assert_eq!(rows[0].get::<_, i32>("n"), 7);
}

/// The widest declared types are covered by the same path: `DOUBLE` stays
/// double precision (8 bytes) and `BIGINT` stays int8 (8 bytes). Narrowing is
/// driven by the declaration, never applied by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_wide_numeric_columns_are_untouched() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION schemaless_wide (id TEXT PRIMARY KEY, d DOUBLE, big BIGINT)")
        .await
        .expect("CREATE COLLECTION with no engine clause must succeed");

    server
        .exec("INSERT INTO schemaless_wide (id, d, big) VALUES ('r1', 2.25, 9000000000)")
        .await
        .expect("INSERT into schemaless_wide must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT d, big FROM schemaless_wide WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare schemaless_wide select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute schemaless_wide select");
    assert_eq!(rows.len(), 1, "expected 1 row back from schemaless_wide");

    assert_column_oids(&rows[0], &[("d", FLOAT8_OID), ("big", INT8_OID)]);
    assert_eq!(rows[0].get::<_, f64>("d"), 2.25);
    assert_eq!(rows[0].get::<_, i64>("big"), 9_000_000_000);
}

/// `UPDATE ... SET` carries the same contract as `INSERT`: rewriting a
/// declared numeric column with a fractional literal must leave a number
/// behind, not the literal's text form. Without the coercion on the update
/// path an inserted-then-updated `REAL` column reads back as NULL even though
/// the insert itself was correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_update_preserves_declared_numeric_widths() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION schemaless_update_widths (id TEXT PRIMARY KEY, r REAL, n INT)")
        .await
        .expect("CREATE COLLECTION with no engine clause must succeed");

    server
        .exec("INSERT INTO schemaless_update_widths (id, r, n) VALUES ('r1', 1.5, 7)")
        .await
        .expect("INSERT into schemaless_update_widths must succeed");

    server
        .exec("UPDATE schemaless_update_widths SET r = 2.5, n = 8 WHERE id = 'r1'")
        .await
        .expect("UPDATE of schemaless_update_widths must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT r, n FROM schemaless_update_widths WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare schemaless_update_widths select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute schemaless_update_widths select");
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row back from schemaless_update_widths"
    );

    assert_column_oids(&rows[0], &[("r", FLOAT4_OID), ("n", INT4_OID)]);
    assert_eq!(rows[0].get::<_, f32>("r"), 2.5);
    assert_eq!(rows[0].get::<_, i32>("n"), 8);
}
