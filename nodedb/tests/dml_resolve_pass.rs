// SPDX-License-Identifier: BUSL-1.1

//! The read-only RESOLVE pass of `MERGE` and `UPDATE ... FROM` must reach the
//! Data Plane as a read. Dispatched as `DocumentOp::ResolveWrite`, which
//! carries no RLS write-check slot; hand-stamping a check on a bare
//! `Merge`/`UpdateFromJoin` instead fails the un-injected-write guard.
//! Three entry points are asserted below: both statements' in-transaction
//! expanders, and the autocommit `UPDATE ... FROM` orchestrator.

mod common;

use common::pgwire_harness::TestServer;

use nodedb::types::{DatabaseId, VShardId};

/// Binding target (holds the balance) and binding source (drives it) for
/// [`autocommit_update_from_join_on_a_sum_source_folds_the_balance`]. The names
/// are chosen for their HASHES: they collide on one vShard.
const SUM_TARGET: &str = "atm_accounts";
const SUM_SOURCE: &str = "atm_entries";
/// Join source of the `UPDATE ... FROM`. It is read and shipped, never written,
/// so its own vShard is free.
const JOIN_SOURCE: &str = "atm_adjust";

/// The premise the autocommit sum test rests on: one core owns both bound
/// collections, so the balance rides the source write's own transaction —
/// splitting them needs the Calvin sequencer this harness has no deployment for.
#[test]
fn materialized_sum_source_and_target_are_co_resident() {
    assert_eq!(
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, SUM_SOURCE),
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, SUM_TARGET),
        "the autocommit sum test must exercise the CO-RESIDENT path; \
         rename the collections until the two hashes agree again"
    );
}

/// `BEGIN; MERGE; COMMIT` applies the merge — the statement-time RESOLVE pass
/// classified it instead of being refused at the Data-Plane boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_transaction_merge_resolves_and_applies() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION rp_merge_target (\
                id TEXT PRIMARY KEY, name TEXT, score INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION rp_merge_source (\
                id TEXT PRIMARY KEY, name TEXT, score INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO rp_merge_target (id, name, score) VALUES ('a', 'alpha', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rp_merge_source (id, name, score) VALUES ('a', 'ALPHA', 99)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rp_merge_source (id, name, score) VALUES ('b', 'BETA', 7)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "MERGE INTO rp_merge_target t \
             USING rp_merge_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score \
             WHEN NOT MATCHED THEN INSERT (id, name, score) \
                 VALUES (s.id, s.name, s.score)",
        )
        .await
        .expect("an in-transaction MERGE must resolve on the Control Plane");
    server.exec("COMMIT").await.unwrap();

    let rows = server
        .query_rows("SELECT id, name, score FROM rp_merge_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["a".to_string(), "ALPHA".to_string(), "99".to_string()],
            vec!["b".to_string(), "BETA".to_string(), "7".to_string()],
        ],
        "both the MATCHED update and the NOT-MATCHED insert must land; got {rows:?}"
    );
}

/// `BEGIN; UPDATE ... FROM; COMMIT` applies the update — same RESOLVE pass,
/// reached through the `UPDATE ... FROM` expander.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_transaction_update_from_join_resolves_and_applies() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION rp_ufj_target (\
                id TEXT PRIMARY KEY, sku TEXT, price INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec(
            "CREATE COLLECTION rp_ufj_source (\
                id TEXT PRIMARY KEY, sku TEXT, new_price INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO rp_ufj_target (id, sku, price) VALUES ('t1', 'k1', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rp_ufj_target (id, sku, price) VALUES ('t2', 'k2', 20)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rp_ufj_source (id, sku, new_price) VALUES ('s1', 'k1', 111)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec(
            "UPDATE rp_ufj_target SET price = s.new_price \
             FROM rp_ufj_source s WHERE rp_ufj_target.sku = s.sku",
        )
        .await
        .expect("an in-transaction UPDATE ... FROM must resolve on the Control Plane");
    server.exec("COMMIT").await.unwrap();

    let rows = server
        .query_rows("SELECT id, price FROM rp_ufj_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            vec!["t1".to_string(), "111".to_string()],
            vec!["t2".to_string(), "20".to_string()],
        ],
        "only the joined row must be rewritten; got {rows:?}"
    );
}

/// An autocommit `UPDATE ... FROM` whose target drives a materialized-sum
/// binding resolves that binding's targets through the same read-only pass
/// before writing, then folds the balance by the difference — the
/// orchestrator's own resolve dispatch, which no other test reaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_update_from_join_on_a_sum_source_folds_the_balance() {
    let server = TestServer::start().await;
    // `SUM_TARGET` holds the running total; `SUM_SOURCE` drives it and is the
    // collection the UPDATE ... FROM rewrites.
    server
        .exec(&format!(
            "CREATE COLLECTION {SUM_TARGET} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE COLLECTION {SUM_SOURCE} (\
                id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "ALTER COLLECTION {SUM_TARGET} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {SUM_SOURCE} \
             ON {SUM_SOURCE}.account_id = {SUM_TARGET}.id \
             VALUE {SUM_SOURCE}.amount"
        ))
        .await
        .unwrap();
    // The join source of the UPDATE ... FROM: it carries the new amounts.
    server
        .exec(&format!(
            "CREATE COLLECTION {JOIN_SOURCE} (\
                id TEXT PRIMARY KEY, entry_id TEXT, new_amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();

    server
        .exec(&format!(
            "INSERT INTO {SUM_TARGET} (id, owner, balance) VALUES ('acc-1', 'alice', '0')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {SUM_SOURCE} (id, account_id, amount) VALUES ('e1', 'acc-1', '25')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {JOIN_SOURCE} (id, entry_id, new_amount) VALUES ('a1', 'e1', '40')"
        ))
        .await
        .unwrap();

    let before = server
        .query_text(&format!(
            "SELECT balance FROM {SUM_TARGET} WHERE id = 'acc-1'"
        ))
        .await
        .unwrap();
    assert_eq!(before, vec!["25".to_string()]);

    server
        .exec(&format!(
            "UPDATE {SUM_SOURCE} SET amount = a.new_amount \
             FROM {JOIN_SOURCE} a WHERE {SUM_SOURCE}.id = a.entry_id"
        ))
        .await
        .expect("an autocommit UPDATE ... FROM on a sum source must resolve its targets");

    let after = server
        .query_text(&format!(
            "SELECT balance FROM {SUM_TARGET} WHERE id = 'acc-1'"
        ))
        .await
        .unwrap();
    assert_eq!(
        after,
        vec!["40".to_string()],
        "the balance must move by the difference (25 -> 40); got {after:?}"
    );
}
