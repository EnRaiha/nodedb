// SPDX-License-Identifier: BUSL-1.1

//! A predicate `UPDATE` or `DELETE` inside a transaction block, on the source
//! of a materialized sum whose target lives on another vShard.
//!
//! The block's COMMIT spans the source and the target vShard, so it runs as a
//! Calvin transaction. Each predicate write carries the rows its statement
//! read, and the sum targets those rows resolve to. COMMIT applies the source
//! rows and the balance together.

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// `UPDATE ... WHERE` on a key moves the balance by the amount's difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_update_by_key_moves_a_cross_shard_sum() {
    let server = sum_fixture().await;
    exec(
        &server,
        "BEGIN; UPDATE ptv_post SET amount = '20' WHERE id = 'e0'; COMMIT",
    )
    .await;
    assert_eq!(balance(&server, "acc").await, "25", "e0 at 20, e1 at 5");
}

/// `UPDATE ... WHERE` on a non-key column moves the balance of every row it
/// matches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_update_by_predicate_moves_a_cross_shard_sum() {
    let server = sum_fixture().await;
    exec(
        &server,
        "BEGIN; UPDATE ptv_post SET amount = '1' WHERE account_id = 'acc'; COMMIT",
    )
    .await;
    assert_eq!(balance(&server, "acc").await, "2", "e0 and e1 at 1");
}

/// `DELETE ... WHERE` takes the deleted posting off the balance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_delete_takes_its_posting_off_a_cross_shard_sum() {
    let server = sum_fixture().await;
    exec(
        &server,
        "BEGIN; DELETE FROM ptv_post WHERE id = 'e1'; COMMIT",
    )
    .await;
    assert_eq!(balance(&server, "acc").await, "10", "e1 deleted");
}

/// A posting the block inserts and then moves by predicate lands on the
/// balance once, at its final amount.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_update_matches_the_blocks_own_insert() {
    let server = sum_fixture().await;
    exec(
        &server,
        "BEGIN; \
         INSERT INTO ptv_post (id, account_id, amount) VALUES ('e2', 'acc2', '4'); \
         UPDATE ptv_post SET amount = '9' WHERE account_id = 'acc2'; \
         COMMIT",
    )
    .await;
    assert_eq!(
        balance(&server, "acc2").await,
        "9",
        "e2 inserted, then at 9"
    );
    assert_eq!(balance(&server, "acc").await, "15", "acc untouched");
}
