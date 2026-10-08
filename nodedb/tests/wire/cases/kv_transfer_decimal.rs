// SPDX-License-Identifier: BUSL-1.1

//! `TRANSFER` on a KV `DECIMAL(p,s)` field moves the balance by exact
//! decimal arithmetic. An INT or DECIMAL amount is accepted. Each balance
//! rounds to the declared scale, half away from zero. A balance past the
//! declared precision is refused with SQLSTATE 22003, and neither row moves.
//! The balance check compares exact decimals.

use crate::harness::TestServer;

const OUT_OF_RANGE: &str = "SQLSTATE 22003";

/// The `balance` of row `key`.
async fn balance(srv: &TestServer, key: &str) -> String {
    let rows = srv
        .query_rows(&format!("SELECT balance FROM td_acct WHERE key = '{key}'"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "{key}: {rows:?}");
    rows[0][0].clone()
}

async fn transfer(srv: &TestServer, from: &str, to: &str, amount: &str) -> Result<(), String> {
    srv.exec(&format!(
        "SELECT TRANSFER('td_acct', '{from}', '{to}', 'balance', {amount})"
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_transfer_moves_a_declared_decimal_exactly() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION td_acct (key TEXT PRIMARY KEY, balance DECIMAL(10,2)) \
         WITH (engine='kv')",
    )
    .await
    .unwrap();
    for (key, value) in [("a", "100.00"), ("b", "5.25"), ("c", "99999999.99")] {
        srv.exec(&format!(
            "INSERT INTO td_acct (key, balance) VALUES ('{key}', {value})"
        ))
        .await
        .unwrap();
    }

    // A DECIMAL amount moves both balances by exactly that amount.
    transfer(&srv, "a", "b", "30.10").await.unwrap();
    assert_eq!(balance(&srv, "a").await, "69.90");
    assert_eq!(balance(&srv, "b").await, "35.35");

    // Each balance rounds to scale 2, half away from zero:
    // 69.90 - 0.005 = 69.895 -> 69.90, and 35.35 + 0.005 = 35.355 -> 35.36.
    transfer(&srv, "a", "b", "0.005").await.unwrap();
    assert_eq!(balance(&srv, "a").await, "69.90");
    assert_eq!(balance(&srv, "b").await, "35.36");

    // An INT amount moves a DECIMAL balance.
    transfer(&srv, "a", "b", "1").await.unwrap();
    assert_eq!(balance(&srv, "a").await, "68.90");
    assert_eq!(balance(&srv, "b").await, "36.36");

    // A credit past DECIMAL(10,2) is refused, and neither row moves.
    srv.expect_error(
        "SELECT TRANSFER('td_acct', 'a', 'c', 'balance', 1)",
        OUT_OF_RANGE,
    )
    .await;
    assert_eq!(balance(&srv, "a").await, "68.90");
    assert_eq!(balance(&srv, "c").await, "99999999.99");

    // The balance check is exact: 68.90 does not cover 68.91.
    srv.expect_error(
        "SELECT TRANSFER('td_acct', 'a', 'b', 'balance', 68.91)",
        "source has 68.90, need 68.91",
    )
    .await;
    assert_eq!(balance(&srv, "a").await, "68.90");
    assert_eq!(balance(&srv, "b").await, "36.36");

    // The whole balance moves when the amount equals it.
    transfer(&srv, "a", "b", "68.90").await.unwrap();
    assert_eq!(balance(&srv, "a").await, "0.00");
    assert_eq!(balance(&srv, "b").await, "105.26");
}
