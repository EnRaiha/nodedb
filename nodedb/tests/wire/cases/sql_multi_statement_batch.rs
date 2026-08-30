// SPDX-License-Identifier: BUSL-1.1

//! Reproduction for a report of `SELECT count(*)` returning 32 after 64
//! `UPSERT` statements were sent as ONE multi-statement query over pgwire
//! (`tokio_postgres::simple_query`, the harness's own `exec` / `query_text`
//! path). 128 statements also reported 32; 32 statements reported 32; no
//! error was returned to the client at any size.
//!
//! Two explanations fit that symptom and this suite must tell them apart:
//!
//! - **Hypothesis A — writes are lost.** Only 32 rows are actually durable;
//!   the multi-statement batch silently drops writes past some point.
//! - **Hypothesis B — writes all land, `count(*)` under-reports.** All rows
//!   sent are durable, but the aggregate itself is wrong. This codebase has
//!   previously shipped and closed a defect where `count(*)` on a document
//!   collection drifted from the true row count, so B is not far-fetched.
//!
//! Every case below asserts on BOTH the ground truth (the actual row set,
//! read back key by key via `ORDER BY <key>`) and the number under
//! suspicion (`SELECT count(*)`). Each statement writes a distinct key
//! (`k0`, `k1`, ...) so a key collision can never masquerade as a lost
//! write, and so a failure names exactly which keys vanished.
//!
//! The original report was measured during an import into a non-default
//! database, and a separate defect from that same import session hardcoded
//! `DatabaseId::DEFAULT` in a subsystem that never saw collections in a
//! non-default database (id >= 1024). The default-database cases above did
//! not reproduce the loss, so the `_in_non_default_database` cases below run
//! the same batches after `USE DATABASE <name>` to test the leading
//! hypothesis: that the loss is database-scoped.

use crate::harness::TestServer;
use std::collections::HashSet;

async fn setup_document(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, n INT)"
        ))
        .await
        .unwrap();
}

async fn setup_kv(server: &TestServer, coll: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')"
        ))
        .await
        .unwrap();
}

/// Creates a non-default database and switches the session to it, following
/// the pattern in `database_scoped_ddl_introspection.rs`
/// (`create_database` + `USE DATABASE <name>`, both via
/// `client.simple_query`). Every subsequent `server.exec` /
/// `server.query_text` / `server.client.simple_query` call on this
/// `TestServer` runs against `name` — the harness gives one session per
/// `TestServer`, and `USE DATABASE` is session state.
async fn use_non_default_database(server: &TestServer, name: &str) {
    server
        .client
        .simple_query(&format!("CREATE DATABASE {name}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE {name} failed: {e}"));
    server
        .client
        .simple_query(&format!("USE DATABASE {name}"))
        .await
        .unwrap_or_else(|e| panic!("USE DATABASE {name} failed: {e}"));
}

/// Joins `n` distinct-key UPSERTs with `; ` into the single string that gets
/// sent as one `simple_query` call — the multi-statement simple-query path
/// the report used.
fn build_upsert_batch(coll: &str, key_col: &str, n: usize) -> String {
    (0..n)
        .map(|i| format!("UPSERT INTO {coll} ({key_col}, n) VALUES ('k{i}', {i})"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Sends `n` UPSERTs as one multi-statement batch, then checks the actual
/// row set (ground truth) and `count(*)` (the number under suspicion)
/// against each other and against `n`, naming which hypothesis the failure
/// supports.
async fn assert_batch_all_lands(server: &TestServer, coll: &str, key_col: &str, n: usize) {
    let batch = build_upsert_batch(coll, key_col, n);
    server.client.simple_query(&batch).await.unwrap_or_else(|e| {
        panic!("multi-statement batch of {n} UPSERTs into one simple_query call must succeed, got: {e}")
    });

    let expected: HashSet<String> = (0..n).map(|i| format!("k{i}")).collect();
    let actual: HashSet<String> = server
        .query_text(&format!("SELECT {key_col} FROM {coll} ORDER BY {key_col}"))
        .await
        .unwrap()
        .into_iter()
        .collect();

    let mut missing: Vec<&String> = expected.difference(&actual).collect();
    missing.sort();
    let mut extra: Vec<&String> = actual.difference(&expected).collect();
    extra.sort();

    let count_rows = server
        .query_text(&format!("SELECT count(*) FROM {coll}"))
        .await
        .unwrap();
    let count_text = count_rows.first().cloned().unwrap_or_default();

    assert!(
        missing.is_empty() && extra.is_empty(),
        "hypothesis A (writes lost): sent {n} UPSERTs in one multi-statement batch, \
         but {} of {n} keys are missing from the actual row set: {missing:?}{}. \
         SELECT count(*) separately reported {count_text} — {}",
        missing.len(),
        if extra.is_empty() {
            String::new()
        } else {
            format!(" (and {} unexpected extra keys: {extra:?})", extra.len())
        },
        if count_text == n.to_string() {
            "count(*) agrees with the incomplete row set, consistent with hypothesis A \
             (writes are actually lost), not hypothesis B"
        } else {
            "count(*) ALSO disagrees with both n and the actual row set, so the write path \
             and the count(*) aggregate are both wrong"
        }
    );

    assert_eq!(
        count_text,
        n.to_string(),
        "hypothesis B (count(*) under-reports): the row-level SELECT (ground truth) found all \
         {n} expected keys present in {coll} with no extras, so the write path is intact — but \
         SELECT count(*) FROM {coll} returned {count_text} instead of {n}. The aggregate itself \
         is wrong, not the writes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_multi_statement_batch_32_upserts_all_land() {
    let server = TestServer::start().await;
    setup_document(&server, "docs_batch_32").await;
    assert_batch_all_lands(&server, "docs_batch_32", "id", 32).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_multi_statement_batch_64_upserts_all_land() {
    let server = TestServer::start().await;
    setup_document(&server, "docs_batch_64").await;
    assert_batch_all_lands(&server, "docs_batch_64", "id", 64).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_multi_statement_batch_128_upserts_all_land() {
    let server = TestServer::start().await;
    setup_document(&server, "docs_batch_128").await;
    assert_batch_all_lands(&server, "docs_batch_128", "id", 128).await;
}

/// KV collections route `UPSERT` through the same DSL dispatch path as
/// document collections but through a different engine, hash-indexed
/// storage instead of MessagePack blobs with secondary indexes. Running the
/// same 64-statement batch against KV tells whether a reproduction is
/// engine-specific to `document` or a defect in the shared multi-statement
/// dispatch / count(*) path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_multi_statement_batch_64_upserts_all_land() {
    let server = TestServer::start().await;
    setup_kv(&server, "kv_batch_64").await;
    assert_batch_all_lands(&server, "kv_batch_64", "key", 64).await;
}

/// The original report was measured during an import into a non-default
/// database. A separate, independently-confirmed defect from that same
/// import session hardcoded `DatabaseId::DEFAULT` in a subsystem, so it
/// never saw collections living in a non-default database (id >= 1024).
/// That makes "the batch loss is database-scoped" the leading hypothesis for
/// this report too — these cases run the identical 64/128-statement batches
/// against a collection in a non-default database (`USE DATABASE <name>`,
/// see `use_non_default_database` above) to test it directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_multi_statement_batch_64_upserts_all_land_in_non_default_database() {
    let server = TestServer::start().await;
    use_non_default_database(&server, "batch_db_64").await;
    setup_document(&server, "docs_batch_64").await;
    assert_batch_all_lands(&server, "docs_batch_64", "id", 64).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_multi_statement_batch_128_upserts_all_land_in_non_default_database() {
    let server = TestServer::start().await;
    use_non_default_database(&server, "batch_db_128").await;
    setup_document(&server, "docs_batch_128").await;
    assert_batch_all_lands(&server, "docs_batch_128", "id", 128).await;
}

/// Same non-default-database check for KV, mirroring
/// `kv_multi_statement_batch_64_upserts_all_land` above — tells whether a
/// database-scoped reproduction is specific to the document engine or a
/// defect in the shared multi-statement dispatch / count(*) path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_multi_statement_batch_64_upserts_all_land_in_non_default_database() {
    let server = TestServer::start().await;
    use_non_default_database(&server, "batch_db_kv").await;
    setup_kv(&server, "kv_batch_64").await;
    assert_batch_all_lands(&server, "kv_batch_64", "key", 64).await;
}

/// Every case above runs in autocommit. An import tool driving its own
/// transaction takes a different code path: writes are staged into the
/// per-transaction overlay (`tx_buffer`) and flushed at COMMIT, rather than
/// going straight through the autocommit write funnel exercised above. This
/// section runs the same UPSERT batches inside an explicit transaction to
/// test whether the loss is specific to that staged-flush path.
///
/// Sends `BEGIN; <n UPSERTs>; COMMIT;` as ONE multi-statement simple query,
/// then applies the same ground-truth + count(*) assertions as
/// `assert_batch_all_lands`.
async fn assert_batch_all_lands_explicit_tx_single_message(
    server: &TestServer,
    coll: &str,
    key_col: &str,
    n: usize,
) {
    let batch = build_upsert_batch(coll, key_col, n);
    let wrapped = format!("BEGIN; {batch}; COMMIT;");
    server
        .client
        .simple_query(&wrapped)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "BEGIN + {n} UPSERTs + COMMIT sent as one multi-statement simple_query call \
             must succeed, got: {e}"
            )
        });

    assert_batch_landed(server, coll, key_col, n).await;
}

/// Drives the transaction across three separate wire round trips — `BEGIN`
/// alone, the joined UPSERTs as one multi-statement message, then `COMMIT`
/// alone — mirroring a client library that issues `BEGIN`/`COMMIT` as their
/// own calls around a batched write, which is the shape an import tool is
/// more likely to use than a single combined message.
async fn assert_batch_all_lands_explicit_tx_multi_message(
    server: &TestServer,
    coll: &str,
    key_col: &str,
    n: usize,
) {
    server
        .client
        .simple_query("BEGIN")
        .await
        .unwrap_or_else(|e| panic!("BEGIN must succeed, got: {e}"));

    let batch = build_upsert_batch(coll, key_col, n);
    server.client.simple_query(&batch).await.unwrap_or_else(|e| {
        panic!("multi-statement batch of {n} UPSERTs inside an open transaction must succeed, got: {e}")
    });

    server
        .client
        .simple_query("COMMIT")
        .await
        .unwrap_or_else(|e| panic!("COMMIT must succeed, got: {e}"));

    assert_batch_landed(server, coll, key_col, n).await;
}

/// Shared ground-truth + count(*) assertion used by the explicit-transaction
/// variants above, identical in substance to the checks in
/// `assert_batch_all_lands` (kept separate because the autocommit variant
/// also has to send the batch itself, which the transaction variants do
/// differently).
async fn assert_batch_landed(server: &TestServer, coll: &str, key_col: &str, n: usize) {
    let expected: HashSet<String> = (0..n).map(|i| format!("k{i}")).collect();
    let actual: HashSet<String> = server
        .query_text(&format!("SELECT {key_col} FROM {coll} ORDER BY {key_col}"))
        .await
        .unwrap()
        .into_iter()
        .collect();

    let mut missing: Vec<&String> = expected.difference(&actual).collect();
    missing.sort();
    let mut extra: Vec<&String> = actual.difference(&expected).collect();
    extra.sort();

    let count_rows = server
        .query_text(&format!("SELECT count(*) FROM {coll}"))
        .await
        .unwrap();
    let count_text = count_rows.first().cloned().unwrap_or_default();

    assert!(
        missing.is_empty() && extra.is_empty(),
        "hypothesis A (writes lost): sent {n} UPSERTs inside an explicit transaction, \
         but {} of {n} keys are missing from the actual row set: {missing:?}{}. \
         SELECT count(*) separately reported {count_text} — {}",
        missing.len(),
        if extra.is_empty() {
            String::new()
        } else {
            format!(" (and {} unexpected extra keys: {extra:?})", extra.len())
        },
        if count_text == n.to_string() {
            "count(*) agrees with the incomplete row set, consistent with hypothesis A \
             (writes are actually lost), not hypothesis B"
        } else {
            "count(*) ALSO disagrees with both n and the actual row set, so the write path \
             and the count(*) aggregate are both wrong"
        }
    );

    assert_eq!(
        count_text,
        n.to_string(),
        "hypothesis B (count(*) under-reports): the row-level SELECT (ground truth) found all \
         {n} expected keys present in {coll} with no extras after the explicit transaction \
         committed, so the write path is intact — but SELECT count(*) FROM {coll} returned \
         {count_text} instead of {n}. The aggregate itself is wrong, not the writes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_explicit_tx_single_message_64_upserts_all_land() {
    let server = TestServer::start().await;
    setup_document(&server, "docs_tx_single_64").await;
    assert_batch_all_lands_explicit_tx_single_message(&server, "docs_tx_single_64", "id", 64).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_explicit_tx_single_message_128_upserts_all_land() {
    let server = TestServer::start().await;
    setup_document(&server, "docs_tx_single_128").await;
    assert_batch_all_lands_explicit_tx_single_message(&server, "docs_tx_single_128", "id", 128)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_explicit_tx_multi_message_64_upserts_all_land() {
    let server = TestServer::start().await;
    setup_document(&server, "docs_tx_multi_64").await;
    assert_batch_all_lands_explicit_tx_multi_message(&server, "docs_tx_multi_64", "id", 64).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_explicit_tx_multi_message_128_upserts_all_land() {
    let server = TestServer::start().await;
    setup_document(&server, "docs_tx_multi_128").await;
    assert_batch_all_lands_explicit_tx_multi_message(&server, "docs_tx_multi_128", "id", 128).await;
}

/// KV routes UPSERT through the same DSL dispatch path as document
/// collections (see `sql_transactions_upsert_overlay.rs`), so one
/// multi-message KV case is enough to tell whether an explicit-transaction
/// loss is engine-specific or shared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_explicit_tx_multi_message_64_upserts_all_land() {
    let server = TestServer::start().await;
    setup_kv(&server, "kv_tx_multi_64").await;
    assert_batch_all_lands_explicit_tx_multi_message(&server, "kv_tx_multi_64", "key", 64).await;
}
