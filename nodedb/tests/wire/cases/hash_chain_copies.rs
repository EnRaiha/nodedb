// SPDX-License-Identifier: BUSL-1.1

//! A `HASH_CHAIN` collection's chain survives every path that copies or adds
//! its rows outside a plain INSERT.
//!
//! * RESTORE into an empty server re-links every row in its source position
//!   order. The links are byte-identical to the source's, and
//!   `VERIFY_HASH_CHAIN` reports the chain valid.
//! * A materialized CLONE re-links the source rows the same way, and a row
//!   inserted with no `id` keeps its minted id.
//! * A MERGE NOT MATCHED INSERT links each row it adds.
//! * DDL refuses every combination whose writes bypass the chain:
//!   `crdt=true`, a materialized-sum target, and CONVERT.

use super::backup_support::{drain_backup, push_restore};
use crate::harness::TestServer;

const TENANT: u64 = 1;

const CREATE: &str = "CREATE COLLECTION hc_ledger (id STRING PRIMARY KEY, amount INT) \
                      WITH (engine='document_schemaless') APPEND_ONLY HASH_CHAIN";

/// Rows inserted out of key order, so position order differs from key order.
const ROWS: [(&str, i64); 4] = [("k3", 30), ("k1", 10), ("k4", 40), ("k2", 20)];

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `(id, _chain_seq, _chain_hash)` of every row, in `id` order.
async fn links(server: &TestServer) -> Vec<Vec<String>> {
    server
        .query_rows("SELECT id, _chain_seq, _chain_hash FROM hc_ledger ORDER BY id")
        .await
        .unwrap_or_else(|e| panic!("read links: {e}"))
}

/// `VERIFY_HASH_CHAIN` reports an intact chain of `entries` links.
async fn assert_chain_valid(server: &TestServer, entries: usize, stage: &str) {
    let rows = server
        .query_rows("SELECT VERIFY_HASH_CHAIN('hc_ledger')")
        .await
        .unwrap_or_else(|e| panic!("{stage}: VERIFY_HASH_CHAIN: {e}"));
    let text = rows
        .first()
        .and_then(|row| row.first())
        .unwrap_or_else(|| panic!("{stage}: VERIFY_HASH_CHAIN returned no row"));
    let verdict: serde_json::Value = serde_json::from_str(text)
        .unwrap_or_else(|e| panic!("{stage}: verdict is not JSON ({e}): {text}"));
    assert_eq!(verdict["valid"], true, "{stage}: {verdict}");
    assert_eq!(verdict["entries"], entries, "{stage}: {verdict}");
}

async fn seed(server: &TestServer) {
    exec(server, CREATE).await;
    for (id, amount) in ROWS {
        exec(
            server,
            &format!("INSERT INTO hc_ledger (id, amount) VALUES ('{id}', {amount})"),
        )
        .await;
    }
    assert_chain_valid(server, ROWS.len(), "source").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_chain_verifies_with_the_source_links() {
    let source = TestServer::start().await;
    seed(&source).await;
    let source_links = links(&source).await;
    let backup = drain_backup(&source.client, TENANT)
        .await
        .expect("take the backup");

    let target = TestServer::start().await;
    push_restore(&target.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    assert_eq!(
        links(&target).await,
        source_links,
        "a restored row must carry the link its source row stores"
    );
    assert_chain_valid(&target, ROWS.len(), "after the restore").await;
}

/// A strict chained collection restores the same way: re-issue sends each
/// Binary Tuple row back as MessagePack, and the target relinks it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_strict_chain_verifies_with_the_source_links() {
    let source = TestServer::start().await;
    exec(
        &source,
        "CREATE COLLECTION hc_ledger (id STRING PRIMARY KEY, amount INT) \
         WITH (engine='document_strict') APPEND_ONLY HASH_CHAIN",
    )
    .await;
    for (id, amount) in ROWS {
        exec(
            &source,
            &format!("INSERT INTO hc_ledger (id, amount) VALUES ('{id}', {amount})"),
        )
        .await;
    }
    assert_chain_valid(&source, ROWS.len(), "strict source").await;
    let source_links = links(&source).await;
    let backup = drain_backup(&source.client, TENANT)
        .await
        .expect("take the backup");

    let target = TestServer::start().await;
    push_restore(&target.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    assert_eq!(
        links(&target).await,
        source_links,
        "a restored strict row must carry the link its source row stores"
    );
    assert_chain_valid(&target, ROWS.len(), "after the strict restore").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_materialized_clone_verifies_with_the_source_links() {
    let server = TestServer::start().await;
    exec(&server, "CREATE DATABASE hc_src").await;
    exec(&server, "USE DATABASE hc_src").await;
    seed(&server).await;
    let source_links = links(&server).await;

    exec(&server, "USE DATABASE default").await;
    exec(&server, "CLONE DATABASE hc_clone FROM hc_src").await;
    exec(&server, "ALTER DATABASE hc_clone MATERIALIZE").await;
    exec(&server, "USE DATABASE hc_clone").await;

    assert_eq!(
        links(&server).await,
        source_links,
        "a materialized clone row must carry the link its source row stores"
    );
    assert_chain_valid(&server, ROWS.len(), "after materialize").await;
}

/// A chained row inserted with no `id` stores its minted id inside the linked
/// contents. A materialized clone copies it under a new surrogate with the
/// same id and the same link, byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_minted_chained_row_keeps_its_id_and_link_through_a_clone() {
    const MINTED: &str = "SELECT id, amount, _chain_seq, _chain_hash FROM hc_minted \
                          ORDER BY _chain_seq";
    let server = TestServer::start().await;
    exec(&server, "CREATE DATABASE hcm_src").await;
    exec(&server, "USE DATABASE hcm_src").await;
    exec(
        &server,
        "CREATE COLLECTION hc_minted (amount INT) \
         WITH (engine='document_schemaless') APPEND_ONLY HASH_CHAIN",
    )
    .await;
    for amount in [30, 10, 40] {
        exec(
            &server,
            &format!("INSERT INTO hc_minted (amount) VALUES ({amount})"),
        )
        .await;
    }
    let source = server
        .query_rows(MINTED)
        .await
        .unwrap_or_else(|e| panic!("read source links: {e}"));
    assert_eq!(source.len(), 3, "source rows: {source:?}");
    assert!(
        source.iter().all(|row| !row[0].is_empty()),
        "every minted row shows an id: {source:?}"
    );

    exec(&server, "USE DATABASE default").await;
    exec(&server, "CLONE DATABASE hcm_clone FROM hcm_src").await;
    exec(&server, "ALTER DATABASE hcm_clone MATERIALIZE").await;
    exec(&server, "USE DATABASE hcm_clone").await;
    assert_eq!(
        server
            .query_rows(MINTED)
            .await
            .unwrap_or_else(|e| panic!("read clone links: {e}")),
        source,
        "a materialized minted row keeps its id and its source link"
    );
    let verdict = server
        .query_rows("SELECT VERIFY_HASH_CHAIN('hc_minted')")
        .await
        .unwrap_or_else(|e| panic!("VERIFY_HASH_CHAIN: {e}"));
    let verdict: serde_json::Value = serde_json::from_str(&verdict[0][0])
        .unwrap_or_else(|e| panic!("verdict is not JSON ({e}): {verdict:?}"));
    assert_eq!(verdict["valid"], true, "{verdict}");
    assert_eq!(verdict["entries"], 3, "{verdict}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merge_insert_links_its_rows() {
    let server = TestServer::start().await;
    seed(&server).await;
    exec(
        &server,
        "CREATE COLLECTION hc_incoming (id STRING PRIMARY KEY, amount INT) \
         WITH (engine='document_schemaless')",
    )
    .await;
    exec(
        &server,
        "INSERT INTO hc_incoming (id, amount) VALUES ('k5', 50)",
    )
    .await;
    exec(
        &server,
        "INSERT INTO hc_incoming (id, amount) VALUES ('k6', 60)",
    )
    .await;
    exec(
        &server,
        "MERGE INTO hc_ledger t USING hc_incoming s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)",
    )
    .await;
    assert_chain_valid(&server, ROWS.len() + 2, "after the merge").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_chain_on_a_crdt_collection_is_refused() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "CREATE COLLECTION hc_crdt (id TEXT PRIMARY KEY, v INT) \
             WITH (crdt='true') APPEND_ONLY HASH_CHAIN",
            "42P16",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hash_chained_materialized_sum_target_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;
    exec(
        &server,
        "CREATE COLLECTION hc_postings (id TEXT PRIMARY KEY, account_id TEXT, amount INT) \
         WITH (engine='document_schemaless')",
    )
    .await;
    server
        .expect_error(
            "ALTER COLLECTION hc_ledger ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE hc_postings \
             ON hc_postings.account_id = hc_ledger.id \
             VALUE hc_postings.amount",
            "42P16",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn converting_a_hash_chained_collection_is_refused() {
    let server = TestServer::start().await;
    seed(&server).await;
    server
        .expect_error(
            "CONVERT COLLECTION hc_ledger TO document_strict (id TEXT PRIMARY KEY, amount INT)",
            "42P16",
        )
        .await;
}
