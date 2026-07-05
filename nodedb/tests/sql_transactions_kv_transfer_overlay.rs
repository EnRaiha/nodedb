// SPDX-License-Identifier: BUSL-1.1

//! In-transaction read-your-own-writes for the two remaining multi-key KV
//! writes with a SQL surface: `TRANSFER` (fungible balance move) and
//! `TRANSFER_ITEM` (non-fungible item move).
//!
//! Extends `sql_transactions_kv_atomic_overlay.rs` to these two ops: staged
//! inside `BEGIN..COMMIT`, a second same-transaction call chains off the
//! FIRST staged write (not the original base value), an over-amount transfer
//! fails at statement time with the real (staged) balance, and `ROLLBACK`
//! discards the staged writes entirely. COMMIT's durable replay goes through
//! `ReplicatedWrite::KvTransfer` / `KvTransferItem`, which re-run the same
//! read-validate-write on the follower.
//!
//! `FieldSet` (the third op this unit adds staging + replication for) has NO
//! SQL surface at all -- there is no `KV_FIELD_SET(...)` function, only the
//! native binary protocol's `KvFieldSet` direct op
//! (`native/dispatch/direct_ops.rs`), which -- like every native direct op
//! (`PointGet`, `BatchPut`, etc.) -- dispatches straight to the Data Plane
//! with `txn_id: None` and never calls `route_in_tx_write`. It therefore
//! cannot exercise the staging overlay end-to-end today regardless of this
//! unit's changes (same gap `sql_transactions_kv_atomic_overlay.rs` documents
//! for `BatchPut`). `FieldSet`'s staging handler and its pure computation
//! (`field_compute::merge_field_updates`) are still implemented and unit
//! tested (`field_compute.rs`'s inline `#[cfg(test)]`) so a future
//! transactional surface picks up correct RYOW behavior with no further Data
//! Plane changes.

mod common;

use common::pgwire_harness::TestServer;

async fn setup(server: &TestServer) {
    server
        .exec("CREATE COLLECTION acct (key TEXT PRIMARY KEY, balance INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("CREATE COLLECTION items (key TEXT PRIMARY KEY, name TEXT) WITH (engine='kv')")
        .await
        .unwrap();
}

/// Parse the JSON payload `SELECT TRANSFER*(...)` returns as its single text column.
fn json_of(rows: &[String]) -> serde_json::Value {
    serde_json::from_str(&rows[0]).expect("TRANSFER* result must be JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_in_tx_chains_and_commits() {
    let server = TestServer::start().await;
    setup(&server).await;

    server
        .exec("INSERT INTO acct (key, balance) VALUES ('a', 100)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO acct (key, balance) VALUES ('b', 10)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    // First staged transfer: a 100->70, b 10->40.
    let rows = server
        .query_text("SELECT TRANSFER('acct', 'a', 'b', 'balance', 30)")
        .await
        .unwrap();
    let v = json_of(&rows);
    assert_eq!(v["source_balance"], 70.0);
    assert_eq!(v["dest_balance"], 40.0);

    // Second staged transfer in the same transaction must chain off the
    // FIRST staged balances (70 / 40), not the original base values.
    let rows2 = server
        .query_text("SELECT TRANSFER('acct', 'a', 'b', 'balance', 10)")
        .await
        .unwrap();
    let v2 = json_of(&rows2);
    assert_eq!(
        v2["source_balance"], 60.0,
        "second in-tx transfer must chain off the first staged source balance"
    );
    assert_eq!(
        v2["dest_balance"], 50.0,
        "second in-tx transfer must chain off the first staged dest balance"
    );

    server.exec("COMMIT").await.unwrap();

    // COMMIT durably persisted the chained result: a fresh (post-COMMIT)
    // transfer of 1 sees the chained source balance 60 (→ 59 after) and dest
    // 50 (→ 51 after), proving both staged transfers replayed at commit.
    let after = json_of(
        &server
            .query_text("SELECT TRANSFER('acct', 'a', 'b', 'balance', 1)")
            .await
            .unwrap(),
    );
    assert_eq!(
        after["source_balance"], 59.0,
        "post-COMMIT source balance must be the chained value 60 (−1 = 59): {after}"
    );
    assert_eq!(
        after["dest_balance"], 51.0,
        "post-COMMIT dest balance must be the chained value 50 (+1 = 51): {after}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_in_tx_rollback_reverts_both_balances() {
    let server = TestServer::start().await;
    setup(&server).await;

    server
        .exec("INSERT INTO acct (key, balance) VALUES ('a', 100)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO acct (key, balance) VALUES ('b', 10)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    let rows = server
        .query_text("SELECT TRANSFER('acct', 'a', 'b', 'balance', 50)")
        .await
        .unwrap();
    assert_eq!(json_of(&rows)["source_balance"], 50.0);
    server.exec("ROLLBACK").await.unwrap();

    // A transfer that would only succeed against the ORIGINAL balance (100)
    // proves ROLLBACK discarded the staged 50/60 split.
    let after = server
        .query_text("SELECT TRANSFER('acct', 'a', 'b', 'balance', 90)")
        .await
        .unwrap();
    assert_eq!(
        json_of(&after)["source_balance"],
        10.0,
        "ROLLBACK must revert 'a' to its original balance (100 - 90 = 10)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_in_tx_insufficient_balance_fails_at_statement_time() {
    let server = TestServer::start().await;
    setup(&server).await;

    server
        .exec("INSERT INTO acct (key, balance) VALUES ('a', 100)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO acct (key, balance) VALUES ('b', 0)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    // Stage a transfer that leaves 'a' with 20.
    server
        .query_text("SELECT TRANSFER('acct', 'a', 'b', 'balance', 80)")
        .await
        .unwrap();

    // A second transfer against the STAGED balance (20, not the original
    // 100) must fail immediately at statement time, reporting 20 as the
    // current balance.
    let err = server
        .query_text("SELECT TRANSFER('acct', 'a', 'b', 'balance', 21)")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("20"),
        "insufficient-balance error must report the STAGED balance (20), got: {err}"
    );

    server.exec("ROLLBACK").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_item_in_tx_visible_to_a_second_call_and_reverts_on_rollback() {
    let server = TestServer::start().await;
    setup(&server).await;

    server
        .exec("INSERT INTO items (key, name) VALUES ('ownerA:sword', 'Sword')")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();

    let rows = server
        .query_text("SELECT TRANSFER_ITEM('items', 'items', 'sword', 'ownerA', 'ownerB')")
        .await
        .unwrap();
    let v = json_of(&rows);
    assert_eq!(v["item_key"], "ownerA:sword");
    assert_eq!(v["dest_key"], "ownerB:sword");

    // In-tx read-your-own-writes: the source item is staged as tombstoned,
    // so a second move attempt from the same source key must see it gone.
    let err = server
        .query_text("SELECT TRANSFER_ITEM('items', 'items', 'sword', 'ownerA', 'ownerC')")
        .await
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("not found")
            || err.to_string().contains("22023")
            || err.to_string().contains("NOT_FOUND"),
        "second move from the now-tombstoned source must fail NotFound, got: {err}"
    );

    server.exec("ROLLBACK").await.unwrap();

    // ROLLBACK must discard both the tombstone and the staged dest put: the
    // original owner can move the item again.
    let after = server
        .query_text("SELECT TRANSFER_ITEM('items', 'items', 'sword', 'ownerA', 'ownerB')")
        .await
        .unwrap();
    let v_after = json_of(&after);
    assert_eq!(v_after["item_key"], "ownerA:sword");
}
