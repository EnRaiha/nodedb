// SPDX-License-Identifier: BUSL-1.1

//! Sequence-backed DEFAULT expressions on typed engines.
//!
//! `DEFAULT nextval('name')` passed DDL and then silently produced NULL on
//! every insert (#294): the pure default evaluator does not know the
//! sequence accessors, so the expression evaluated to "no value" and the
//! column was omitted. The sequence machinery itself exists CP-side
//! (`SequenceRegistry`); these tests pin the wired path — the default
//! advances the sequence per row on a typed engine, and an unknown sequence
//! raises loudly instead of vanishing into NULL.

use crate::harness::TestServer;

/// document_strict stores column defaults; the default must advance the
/// sequence once per inserted row and never produce NULL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_default_nextval_fills_distinct_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE SEQUENCE seqdefault").await.unwrap();
    server
        .exec(
            "CREATE COLLECTION sst (id BIGINT DEFAULT nextval('seqdefault') PRIMARY KEY, v TEXT) \
             WITH (engine = 'document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO sst (v) VALUES ('one')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO sst (v) VALUES ('two')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO sst (v) VALUES ('three')")
        .await
        .unwrap();

    let rows = server
        .query_named_rows("SELECT id, v FROM sst ORDER BY id")
        .await
        .expect("rows readable");
    assert_eq!(rows.len(), 3, "{rows:?}");
    let ids: Vec<Option<&String>> = rows.iter().map(|r| r.get("id")).collect();
    assert_eq!(
        ids,
        vec![
            Some(&"1".to_string()),
            Some(&"2".to_string()),
            Some(&"3".to_string())
        ],
        "ids must advance 1..3, got {rows:?}"
    );
}

/// A DEFAULT naming an unknown sequence must raise at insert time — the
/// DDL-accepted expression must never silently become NULL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_default_unknown_sequence_raises() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION bad (id BIGINT DEFAULT nextval('no_such_seq') PRIMARY KEY, v TEXT) \
             WITH (engine = 'document_strict')",
        )
        .await
        .unwrap();

    let err = server
        .exec("INSERT INTO bad (v) VALUES ('x')")
        .await
        .unwrap_err();
    assert!(
        err.contains("42704"),
        "unknown-sequence DEFAULT must raise 42704 (undefined_object): {err}"
    );
    assert!(
        err.contains("no_such_seq"),
        "error must name the missing sequence: {err}"
    );
    assert!(
        !err.contains("NULL") && !err.is_empty(),
        "must not silently NULL: {err}"
    );
}

/// Sentinel: stateless defaults (uuid) keep working on typed engines
/// alongside the new sequence route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_uuid_default_still_fills() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ud (id UUID DEFAULT uuid_v7() PRIMARY KEY, v TEXT) \
             WITH (engine = 'document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO ud (v) VALUES ('a')")
        .await
        .unwrap();
    let rows = server
        .query_named_rows("SELECT id, v FROM ud")
        .await
        .expect("rows readable");
    let id = rows[0].get("id").expect("id filled");
    assert_eq!(id.len(), 36, "uuid_v7 must fill the id: {rows:?}");
}
