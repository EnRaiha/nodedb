// SPDX-License-Identifier: BUSL-1.1

//! Sequence-backed DEFAULT expressions across the remaining engines.
//!
//! Part 1 (this PR's base) wired `DEFAULT nextval('name')` on the
//! doc-family insert path (typed engines verified: strict fills 1,2,3).
//! These tests pin the same guarantee on the kv and columnar families,
//! whose default-fill paths still use the pure stateless evaluator:
//! a DDL-accepted sequence DEFAULT must advance the CP-side registry per
//! row — never silently become NULL.

use crate::harness::TestServer;

/// kv keys the row on the DEFAULT'd column: the default must materialize
/// the key from the sequence before the key slot is built.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_default_nextval_fills_key() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE kvseq").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION kvn (k BIGINT DEFAULT nextval('kvseq') PRIMARY KEY, v TEXT) \
             WITH (engine = 'kv')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO kvn (v) VALUES ('one')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kvn (v) VALUES ('two')")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT k, v FROM kvn ORDER BY k")
        .await
        .expect("rows readable");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows.iter().map(|r| r.get("k")).collect::<Vec<_>>(),
        vec![Some(&"1".to_string()), Some(&"2".to_string())],
        "kv keys must be 1,2: {rows:?}"
    );
}

/// columnar routes defaults through the batch rows encoder: the sequence
/// default must fill per row there too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_default_nextval_fills_id() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE colseq").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION coln (id BIGINT DEFAULT nextval('colseq') PRIMARY KEY, v TEXT) \
             WITH (engine = 'columnar')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO coln (v) VALUES ('one')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO coln (v) VALUES ('two')")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT id, v FROM coln ORDER BY id")
        .await
        .expect("rows readable");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows.iter().map(|r| r.get("id")).collect::<Vec<_>>(),
        vec![Some(&"1".to_string()), Some(&"2".to_string())],
        "columnar ids must be 1,2: {rows:?}"
    );
}

/// Unknown sequence on kv must raise loudly, never silently NULL the key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_default_unknown_sequence_raises() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION kbad (k BIGINT DEFAULT nextval('no_kv_seq') PRIMARY KEY, v TEXT) \
             WITH (engine = 'kv')",
        )
        .await
        .unwrap();

    let err = server
        .exec("INSERT INTO kbad (v) VALUES ('x')")
        .await
        .unwrap_err();
    assert!(
        err.contains("no_kv_seq"),
        "error must name the missing sequence: {err}"
    );
}

/// The #294 repro engine: schemaless document with a declared column list.
/// Its DEFAULTs are currently dropped at the catalog adapter (default: None
/// hardcoded for every schemaless column) — these pin the storage-layer fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_default_nextval_fills_id() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE docseq").await.unwrap();
    server
        .exec("CREATE COLLECTION docn (id BIGINT DEFAULT nextval('docseq') PRIMARY KEY, v TEXT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO docn (v) VALUES ('one')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO docn (v) VALUES ('two')")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT id, v FROM docn ORDER BY id")
        .await
        .expect("rows readable");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows.iter().map(|r| r.get("id")).collect::<Vec<_>>(),
        vec![Some(&"1".to_string()), Some(&"2".to_string())],
        "document ids must be 1,2: {rows:?}"
    );
}

/// Stateless defaults (uuid) are part of the same storage layer: the adapter
/// must not drop them either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_uuid_default_fills_id() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION docu (id UUID DEFAULT uuid_v7() PRIMARY KEY, v TEXT)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO docu (v) VALUES ('a')")
        .await
        .unwrap();
    let rows = server
        .query_named_rows("SELECT id, v FROM docu")
        .await
        .expect("rows readable");
    let id = rows[0].get("id").expect("id filled");
    assert_eq!(id.len(), 36, "uuid_v7 must fill the id: {rows:?}");
}

/// UPSERT shares the INSERT default path: omitting a defaulted column on a
/// document UPSERT must materialize the default, not drop it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_upsert_default_nextval_fills_id() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE upseq").await.unwrap();
    server
        .exec("CREATE COLLECTION upn (id BIGINT DEFAULT nextval('upseq') PRIMARY KEY, v TEXT)")
        .await
        .unwrap();

    server
        .exec("UPSERT INTO upn (v) VALUES ('one')")
        .await
        .unwrap();
    server
        .exec("UPSERT INTO upn (v) VALUES ('two')")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT id, v FROM upn ORDER BY id")
        .await
        .expect("rows readable");
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        rows.iter().map(|r| r.get("id")).collect::<Vec<_>>(),
        vec![Some(&"1".to_string()), Some(&"2".to_string())],
        "upsert ids must be 1,2: {rows:?}"
    );
}

/// currval/setval have no per-row meaning in a DEFAULT: they must raise
/// loudly on every engine, never silently NULL the column (#294 family).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_currval_default_raises_not_null() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE cqseq").await.unwrap();
    server
        .exec("CREATE COLLECTION cqn (id BIGINT DEFAULT currval('cqseq') PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    let err = server
        .exec("INSERT INTO cqn (v) VALUES ('x')")
        .await
        .unwrap_err();
    assert!(
        err.contains("nextval"),
        "currval DEFAULT must point to nextval, not silently NULL: {err}"
    );
}

/// Malformed accessor bodies (empty name) must raise, not fall back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_nextval_default_raises() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION badn (id BIGINT DEFAULT nextval('') PRIMARY KEY, v TEXT)")
        .await
        .unwrap();
    let err = server
        .exec("INSERT INTO badn (v) VALUES ('x')")
        .await
        .unwrap_err();
    assert!(!err.is_empty(), "must raise, never silently NULL");
}
