// SPDX-License-Identifier: BUSL-1.1

//! The float analogue of `ddl_int_width_aliases_strict_kv.rs`:
//! `document_strict` and `kv` collections must accept every PostgreSQL
//! float-width keyword in DDL *and* report each column's declared width
//! faithfully on the wire.
//!
//! Both engines validate declared column types via
//! `nodedb_types::columnar::ColumnType::from_str`, which previously recognized
//! only `FLOAT64`/`DOUBLE`/`REAL`/`FLOAT` — `FLOAT4` and `FLOAT8` were rejected
//! as "unknown column type", exactly as `INT4` was.
//!
//! Accepting those spellings is only half the fix. `ColumnType` deliberately
//! has one `Float64` variant for every declared width (nodedb stores all
//! floats as a full f64), so a strict/kv column's resolved type cannot say how
//! wide the author declared it. The declared width is recovered from the
//! catalog's raw `fields` entries and resolved to a
//! `nodedb_types::columnar::FloatWidth` at the catalog boundary. Without that,
//! every float column reports OID 701 (`double precision`) no matter what was
//! declared, and a client that asked for `REAL` decodes eight bytes where it
//! expected four.
//!
//! The two directions this pins:
//!
//! * `REAL` / `FLOAT4` are **always** single precision (OID 700).
//! * Bare `FLOAT` is **double** precision (OID 701), matching PostgreSQL and
//!   the SQL standard. `FLOAT` is not a synonym for `REAL`.
//!
//! Unlike an out-of-range integer, narrowing a float rounds rather than
//! wraps, and PostgreSQL accepts-and-rounds too — so there is no full
//! write-side range constraint. The one failure mode — a finite `f64` beyond
//! `f32`'s range overflowing to infinity — is refused at write time by the
//! planner (mirroring the declared-width integer check), with the pgwire
//! encoder's SQLSTATE `22003` guard layered underneath as the backstop for
//! rows that reach it some other way. Covered by the last test here.

mod common;

use common::pgwire_harness::TestServer;

/// PostgreSQL type OIDs for the two float widths.
const FLOAT4_OID: u32 = 700;
const FLOAT8_OID: u32 = 701;

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

/// `document_strict` `CREATE COLLECTION` with every float-width spelling must
/// succeed (previously "unknown column type: 'FLOAT4'") and each column must
/// advertise its own declared width.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_create_collection_preserves_declared_float_widths() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION strict_float_widths (\
                id TEXT PRIMARY KEY, \
                r REAL, \
                f4 FLOAT4, \
                d DOUBLE, \
                f8 FLOAT8, \
                f FLOAT\
             ) WITH (engine='document_strict')",
        )
        .await
        .expect(
            "CREATE COLLECTION with REAL/FLOAT4/DOUBLE/FLOAT8/FLOAT columns must \
             succeed on document_strict",
        );

    server
        .exec(
            "INSERT INTO strict_float_widths (id, r, f4, d, f8, f) \
             VALUES ('r1', 1.5, 2.5, 3.5, 4.5, 5.5)",
        )
        .await
        .expect("INSERT into strict_float_widths must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT id, r, f4, d, f8, f FROM strict_float_widths WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare strict_float_widths select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute strict_float_widths select");
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row back from strict_float_widths"
    );

    assert_column_oids(
        &rows[0],
        &[
            ("r", FLOAT4_OID),
            ("f4", FLOAT4_OID),
            ("d", FLOAT8_OID),
            ("f8", FLOAT8_OID),
            // Bare FLOAT is double precision, not single.
            ("f", FLOAT8_OID),
            ("id", 25),
        ],
    );

    // Typed getters matching the advertised widths: a wrong OID or a
    // wrong-width payload panics inside `get` before the comparison.
    assert_eq!(rows[0].get::<_, f32>("r"), 1.5);
    assert_eq!(rows[0].get::<_, f32>("f4"), 2.5);
    assert_eq!(rows[0].get::<_, f64>("d"), 3.5);
    assert_eq!(rows[0].get::<_, f64>("f8"), 4.5);
    assert_eq!(rows[0].get::<_, f64>("f"), 5.5);
}

/// `kv` `CREATE COLLECTION` with the same spellings must succeed and report
/// each declared width, exactly as strict does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_create_collection_preserves_declared_float_widths() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION kv_float_widths (\
                key TEXT PRIMARY KEY, \
                r REAL, \
                f4 FLOAT4, \
                d DOUBLE, \
                f8 FLOAT8, \
                f FLOAT\
             ) WITH (engine='kv')",
        )
        .await
        .expect(
            "CREATE COLLECTION with REAL/FLOAT4/DOUBLE/FLOAT8/FLOAT columns must \
             succeed on kv",
        );

    server
        .exec(
            "INSERT INTO kv_float_widths (key, r, f4, d, f8, f) \
             VALUES ('r1', 1.5, 2.5, 3.5, 4.5, 5.5)",
        )
        .await
        .expect("INSERT into kv_float_widths must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT key, r, f4, d, f8, f FROM kv_float_widths WHERE key = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare kv_float_widths select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute kv_float_widths select");
    assert_eq!(rows.len(), 1, "expected 1 row back from kv_float_widths");

    assert_column_oids(
        &rows[0],
        &[
            ("r", FLOAT4_OID),
            ("f4", FLOAT4_OID),
            ("d", FLOAT8_OID),
            ("f8", FLOAT8_OID),
            ("f", FLOAT8_OID),
        ],
    );
    assert_eq!(rows[0].get::<_, f32>("r"), 1.5);
    assert_eq!(rows[0].get::<_, f32>("f4"), 2.5);
    assert_eq!(rows[0].get::<_, f64>("d"), 3.5);
    assert_eq!(rows[0].get::<_, f64>("f8"), 4.5);
    assert_eq!(rows[0].get::<_, f64>("f"), 5.5);
}

/// A `kv` collection mixing a declared integer column with a declared float
/// one: both must reach the client as their declared width *and* as a payload
/// of that width's byte count.
///
/// KV has no typed write path — its engine stores the value bytes the planner
/// hands it — so the declared column type is applied when the plan is built
/// (`nodedb_sql::planner::declared_type_coerce`). That coercion covers both
/// numeric families at once, which is exactly what this pins: `n` must still
/// round trip as `int4` after the float fix, and `r` as `real` alongside it.
/// The integer half is the regression lock — integers were already correct
/// because an integer literal resolves to an integer value, while a fractional
/// literal resolves to an exact decimal that serializes as text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_declared_int_and_float_columns_round_trip_at_their_widths() {
    /// PostgreSQL `int4`.
    const INT4_OID: u32 = 23;

    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION kv_int_and_float (\
                key TEXT PRIMARY KEY, \
                n INT, \
                r REAL\
             ) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION with an INT and a REAL column must succeed on kv");

    server
        .exec("INSERT INTO kv_int_and_float (key, n, r) VALUES ('r1', 7, 1.5)")
        .await
        .expect("INSERT into kv_int_and_float must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT n, r FROM kv_int_and_float WHERE key = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare kv_int_and_float select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute kv_int_and_float select");
    assert_eq!(rows.len(), 1, "expected 1 row back from kv_int_and_float");

    assert_column_oids(&rows[0], &[("n", INT4_OID), ("r", FLOAT4_OID)]);
    // Typed getters: a wrong-width payload behind either OID fails inside
    // `get` before the comparison runs.
    assert_eq!(rows[0].get::<_, i32>("n"), 7);
    assert_eq!(rows[0].get::<_, f32>("r"), 1.5);
}

/// Narrowing a float **rounds** — that is correct PostgreSQL `real` behaviour,
/// not an error. A value with no exact `f32` representation must still round
/// trip as the nearest `real`, or `REAL` columns would be unusable for almost
/// every value they hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_column_rounds_rather_than_rejecting() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION real_rounding (\
                id TEXT PRIMARY KEY, \
                r REAL\
             ) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION real_rounding must succeed");

    server
        .exec("INSERT INTO real_rounding (id, r) VALUES ('r1', 1.1)")
        .await
        .expect("a value with no exact f32 representation must be accepted");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT r FROM real_rounding WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare real_rounding select");
    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("execute real_rounding select");
    assert_eq!(rows.len(), 1, "expected 1 row back from real_rounding");
    assert_column_oids(&rows[0], &[("r", FLOAT4_OID)]);
    assert_eq!(
        rows[0].get::<_, f32>("r"),
        1.1_f32,
        "a REAL column returns the nearest f32, exactly as PostgreSQL does"
    );
}

/// The one float narrowing that is not value-preserving: a finite `f64`
/// beyond `f32`'s range would reach the client as `Infinity`, silently
/// replacing a real stored number. That is now caught at write time, in the
/// planner (`check_declared_float_ranges`), exactly as the declared-width
/// integer check rejects an out-of-range integer literal — so the bad row
/// never lands and the error surfaces immediately to the writer, not later to
/// whoever happens to read it. The read-side guard in the pgwire encoder
/// (`checked_narrow_f32`, SQLSTATE `22003`) still exists underneath as the
/// backstop for rows written before the column's width was declared, or via
/// non-SQL ingest — see its module docs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_column_overflow_is_rejected_at_write_time() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION real_overflow (\
                id TEXT PRIMARY KEY, \
                r REAL, \
                d DOUBLE\
             ) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION real_overflow must succeed");

    // Rejected on write — a finite value beyond f32 range would otherwise
    // silently become Infinity.
    let err = server
        .exec("INSERT INTO real_overflow (id, r, d) VALUES ('r1', 1e300, 1e300)")
        .await
        .expect_err(
            "a float beyond the declared REAL column's f32 range must be \
             rejected at write time, not silently stored and surfaced later \
             on read",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("out of range"),
        "rejection must say the value is out of range; got: {msg}"
    );

    // Nothing from the rejected statement may have landed.
    let rows = server
        .client
        .query("SELECT id FROM real_overflow", &[])
        .await
        .expect("scan real_overflow");
    assert!(
        rows.is_empty(),
        "no rejected row may be stored; found {} row(s)",
        rows.len()
    );

    // The same value into a DOUBLE column is in range and is accepted —
    // proving the rejection above is the declared-REAL-width guard and not a
    // blanket rejection of large floats.
    server
        .exec("INSERT INTO real_overflow (id, d) VALUES ('r2', 1e300)")
        .await
        .expect("a float within f64 range must be accepted into a DOUBLE column");

    let stmt_wide = server
        .client
        .prepare_typed(
            "SELECT d FROM real_overflow WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare real_overflow wide select");
    let rows = server
        .client
        .query(&stmt_wide, &[&"r2"])
        .await
        .expect("a double precision column holds the same value without error");
    assert_eq!(rows.len(), 1, "expected 1 row back from real_overflow");
    assert_column_oids(&rows[0], &[("d", FLOAT8_OID)]);
    assert_eq!(rows[0].get::<_, f64>("d"), 1e300);
}
