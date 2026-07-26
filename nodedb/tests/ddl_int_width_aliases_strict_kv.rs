// SPDX-License-Identifier: BUSL-1.1

//! Upstream issue #223 regression: `document_strict` and `kv` engines must
//! accept `INT4`/`SMALLINT` (and the other PostgreSQL integer-width
//! keywords) as valid column types. Both engines validate declared column
//! types via `nodedb_types::columnar::ColumnType::from_str`
//! (`nodedb-sql/src/ddl_ast/collection_type.rs`'s `build_strict_schema` /
//! `build_kv_collection_type`), which previously only recognized
//! `BIGINT`/`INT64`/`INTEGER`/`INT` — `INT4`, `INT8`, `SMALLINT`, and `INT2`
//! were rejected with "unknown column type", even though they're valid
//! PostgreSQL integer aliases.
//!
//! This is a DDL-acceptance smoke test only: for these engines the wire OID
//! stays `INT8` (20) regardless of declared width — `raw_type` refinement
//! (issue #217) is schemaless/columnar-only by design, since strict/kv
//! columns are typed authoritatively by the resolved `ColumnType`, not a
//! separately-tracked raw string. See `pgwire_ddl_result_types.rs`'s
//! `create_collection_int_widths_preserve_wire_oids` for the schemaless OID
//! fidelity coverage.

mod common;

use common::pgwire_harness::TestServer;

/// `document_strict` `CREATE COLLECTION` with `INT4`/`SMALLINT` columns must
/// succeed (previously errored with "unknown column type: 'INT4'").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_create_collection_accepts_int_width_aliases() {
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

    // Load-bearing: document_strict stays INT8 (20) regardless of declared
    // width — strict columns carry no `raw_type` (they're already typed
    // authoritatively by `ColumnType`), so the issue #217 refinement never
    // applies here. Width narrowing is schemaless/columnar-only by design.
    for col_name in ["a", "b", "c", "d"] {
        if let Some(col) = rows[0].columns().iter().find(|c| c.name() == col_name) {
            assert_eq!(
                col.type_().oid(),
                20,
                "strict column '{col_name}' OID must stay INT8 (20), got {}",
                col.type_().oid()
            );
        }
    }
}

/// `kv` `CREATE COLLECTION` with `INT4`/`SMALLINT` columns must succeed
/// (previously errored with "unknown column type: 'INT4'"). The kv OID
/// stays INT8 (20) — do not assert a narrowed OID here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_create_collection_accepts_int_width_aliases() {
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

    // Load-bearing: kv stays INT8 (20) — the fix does not widen/narrow kv's
    // wire type, only schemaless/columnar's.
    for col_name in ["a", "b"] {
        if let Some(col) = rows[0].columns().iter().find(|c| c.name() == col_name) {
            assert_eq!(
                col.type_().oid(),
                20,
                "kv column '{col_name}' OID must stay INT8 (20), got {}",
                col.type_().oid()
            );
        }
    }
}
