// SPDX-License-Identifier: BUSL-1.1

//! Declared integer widths are enforced on write, so a narrowed
//! `RowDescription` OID is a promise the data can keep.
//!
//! Advertising OID 23 for a column declared `INT` tells a pgwire client to
//! decode exactly four bytes. nodedb stores integers as a full `i64`, so
//! without a write-side constraint a value like `9876543210` could be stored
//! in that column and then either truncated to `-1294967296` on the wire (in
//! binary format, undetectably) or fail to parse client-side (in text format).
//!
//! The constraint closes that gap at the point the value enters, exactly as
//! PostgreSQL does (`value out of range for type integer`). These tests pin
//! both halves: the write is refused, and values that *are* accepted survive a
//! binary round-trip at the declared width.

mod common;

use common::pgwire_harness::TestServer;

/// The narrowest value that does not fit each declared width.
const OVER_I32: i64 = i32::MAX as i64 + 1;
const UNDER_I32: i64 = i32::MIN as i64 - 1;
const OVER_I16: i64 = i16::MAX as i64 + 1;
const UNDER_I16: i64 = i16::MIN as i64 - 1;

async fn server_with_widths(collection: &str, engine_clause: &str) -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (\
                id TEXT PRIMARY KEY, \
                small SMALLINT, \
                med INT, \
                big BIGINT\
             ){engine_clause}"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {collection} must succeed: {e}"));
    server
}

/// A value past the declared width is refused, on every declared narrow
/// column and at both ends of the range. The row must not be stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_beyond_declared_width_is_rejected() {
    let server = server_with_widths("width_reject", "").await;

    let cases: &[(&str, i64)] = &[
        ("med", OVER_I32),
        ("med", UNDER_I32),
        ("small", OVER_I16),
        ("small", UNDER_I16),
    ];
    for (idx, (column, value)) in cases.iter().enumerate() {
        let err = server
            .exec(&format!(
                "INSERT INTO width_reject (id, {column}) VALUES ('r{idx}', {value})"
            ))
            .await
            .expect_err(
                "INSERT of a value past the column's declared width must be \
                 rejected, not silently stored and truncated on read",
            );
        let msg = err.to_string();
        assert!(
            msg.contains("out of range"),
            "rejection for {column}={value} must say the value is out of \
             range; got: {msg}"
        );
    }

    // Nothing from the rejected statements may have landed.
    let rows = server
        .client
        .query("SELECT id FROM width_reject", &[])
        .await
        .expect("scan width_reject");
    assert!(
        rows.is_empty(),
        "no rejected row may be stored; found {} row(s)",
        rows.len()
    );
}

/// The boundary values themselves are accepted — the check must reject only
/// what genuinely does not fit, not shrink the usable range by one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boundary_values_are_accepted_and_round_trip() {
    let server = server_with_widths("width_boundary", "").await;

    server
        .exec(&format!(
            "INSERT INTO width_boundary (id, small, med, big) \
             VALUES ('max', {}, {}, {})",
            i16::MAX,
            i32::MAX,
            i64::MAX
        ))
        .await
        .expect("INSERT of each width's maximum must succeed");
    server
        .exec(&format!(
            "INSERT INTO width_boundary (id, small, med, big) \
             VALUES ('min', {}, {}, {})",
            i16::MIN,
            i32::MIN,
            i64::MIN
        ))
        .await
        .expect("INSERT of each width's minimum must succeed");

    let stmt = server
        .client
        .prepare_typed(
            "SELECT small, med, big FROM width_boundary WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare width_boundary select");

    for (id, small, med, big) in [
        ("max", i16::MAX, i32::MAX, i64::MAX),
        ("min", i16::MIN, i32::MIN, i64::MIN),
    ] {
        let rows = server
            .client
            .query(&stmt, &[&id])
            .await
            .unwrap_or_else(|e| panic!("execute width_boundary select for {id}: {e}"));
        assert_eq!(rows.len(), 1, "expected 1 row for id={id}");
        // Typed getters at the advertised widths: these decode the binary
        // payload as i16/i32/i64, so a wrapped value fails here.
        assert_eq!(rows[0].get::<_, i16>("small"), small, "small for id={id}");
        assert_eq!(rows[0].get::<_, i32>("med"), med, "med for id={id}");
        assert_eq!(rows[0].get::<_, i64>("big"), big, "big for id={id}");
    }
}

/// A bound parameter cannot smuggle an out-of-range value past the declared
/// width.
///
/// This is the case the planner check exists for. An inline literal could in
/// principle be rejected by the parser; a parameter arrives as an `int8` on
/// the wire, is decoded from its binary form, and is bound into the AST
/// *before* planning, so the planner's declared-width range check is the only
/// thing standing between it and storage.
///
/// The parameter is declared `INT8` (via `prepare_typed`) rather than left as
/// a bare `INSERT ... VALUES ($1)` placeholder: inference for a bare
/// placeholder reports `Unknown`, which no driver will serialize an integer
/// into, so an explicit `INT8` type is the shape that reaches the planner as
/// a bound integer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bound_parameter_beyond_declared_width_is_rejected() {
    let server = server_with_widths("width_param", "").await;

    let stmt = server
        .client
        .prepare_typed(
            "INSERT INTO width_param (id, med) VALUES ('p1', $1)",
            &[tokio_postgres::types::Type::INT8],
        )
        .await
        .expect("prepare width_param insert");
    let err = server
        .client
        .execute(&stmt, &[&OVER_I32])
        .await
        .expect_err("a bound parameter past the declared width must be rejected");
    // `tokio_postgres::Error`'s Display is just "db error"; the server's
    // message is only on the wrapped DbError.
    let msg = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        msg.contains("out of range"),
        "bound-parameter rejection must come from the range check and say out \
         of range; got: {msg}"
    );

    // The same parameter inside the range is accepted, proving the rejection
    // above is the width constraint and not a blanket parameter failure.
    let stmt_ok = server
        .client
        .prepare_typed(
            "INSERT INTO width_param (id, med) VALUES ('p2', $1)",
            &[tokio_postgres::types::Type::INT8],
        )
        .await
        .expect("prepare in-range width_param insert");
    server
        .client
        .execute(&stmt_ok, &[&(i32::MAX as i64)])
        .await
        .expect("an in-range bound parameter must be accepted");

    let rows = server
        .client
        .query("SELECT id FROM width_param", &[])
        .await
        .expect("scan width_param");
    let ids: Vec<&str> = rows.iter().map(|r| r.get("id")).collect();
    assert_eq!(
        ids,
        vec!["p2"],
        "only the in-range parameter may have been stored"
    );
}

/// `UPDATE ... SET col = <literal>` is checked too — a write that widens an
/// existing row past its declared width is the same violation as an insert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_beyond_declared_width_is_rejected() {
    let server = server_with_widths("width_update", "").await;

    server
        .exec("INSERT INTO width_update (id, med) VALUES ('u1', 1)")
        .await
        .expect("seed row must insert");

    let err = server
        .exec(&format!(
            "UPDATE width_update SET med = {OVER_I32} WHERE id = 'u1'"
        ))
        .await
        .expect_err("UPDATE past the declared width must be rejected");
    assert!(
        err.to_string().contains("out of range"),
        "UPDATE rejection must say out of range; got: {err}"
    );

    let rows = server
        .client
        .query("SELECT med FROM width_update WHERE id = 'u1'", &[])
        .await
        .expect("scan width_update");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, i32>("med"),
        1,
        "the rejected UPDATE must leave the original value intact"
    );
}

/// `ALTER COLUMN ... TYPE` moves the constraint with the declared type.
///
/// Widening `INT` to `BIGINT` is the *only* thing that alter supports (a
/// cross-type change needs a rewrite and is refused), so if the catalog's
/// record of the declared type did not move with it, the alter would be a
/// silent no-op: the column would keep advertising OID 23 and keep rejecting
/// the very values the widening was performed to allow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn altering_the_declared_type_moves_the_constraint() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION width_alter (id TEXT PRIMARY KEY, n INT NOT NULL) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION width_alter must succeed");

    // Before the widening the value is refused and the column reports int4.
    server
        .exec(&format!(
            "INSERT INTO width_alter (id, n) VALUES ('pre', {OVER_I32})"
        ))
        .await
        .expect_err("the value must not fit the column's original INT width");
    let before = server
        .client
        .prepare("SELECT n FROM width_alter LIMIT 0")
        .await
        .expect("prepare pre-alter describe");
    assert_eq!(
        before.columns()[0].type_().oid(),
        23,
        "an INT column must advertise int4 before the widening"
    );

    server
        .exec("ALTER COLLECTION width_alter ALTER COLUMN n TYPE BIGINT")
        .await
        .expect("widening INT to BIGINT must succeed");

    // After it, the same value is accepted and the column reports int8.
    server
        .exec(&format!(
            "INSERT INTO width_alter (id, n) VALUES ('post', {OVER_I32})"
        ))
        .await
        .expect("the widened column must accept a value that needs BIGINT");
    let after = server
        .client
        .prepare("SELECT n FROM width_alter LIMIT 0")
        .await
        .expect("prepare post-alter describe");
    assert_eq!(
        after.columns()[0].type_().oid(),
        20,
        "a widened BIGINT column must advertise int8"
    );

    let rows = server
        .client
        .query("SELECT n FROM width_alter WHERE id = 'post'", &[])
        .await
        .expect("scan width_alter");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>("n"), OVER_I32);
}

/// Strict and kv resolve declared widths through the same catalog path as
/// schemaless, so the constraint must hold there too rather than being a
/// schemaless-only behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declared_width_is_enforced_on_strict_and_kv() {
    let strict = server_with_widths("strict_width", " WITH (engine='document_strict')").await;
    let err = strict
        .exec(&format!(
            "INSERT INTO strict_width (id, med) VALUES ('s1', {OVER_I32})"
        ))
        .await
        .expect_err("document_strict must enforce the declared width");
    assert!(
        err.to_string().contains("out of range"),
        "strict rejection must say out of range; got: {err}"
    );

    let kv = TestServer::start().await;
    kv.exec("CREATE COLLECTION kv_width (key TEXT PRIMARY KEY, med INT) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION kv_width must succeed");
    let err = kv
        .exec(&format!(
            "INSERT INTO kv_width (key, med) VALUES ('k1', {OVER_I32})"
        ))
        .await
        .expect_err("kv must enforce the declared width");
    assert!(
        err.to_string().contains("out of range"),
        "kv rejection must say out of range; got: {err}"
    );
}
