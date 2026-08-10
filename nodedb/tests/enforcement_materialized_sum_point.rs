// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for MATERIALIZED SUM under the POINT write shapes, on a
//! single node whose source and target collections are CO-RESIDENT.
//!
//! A `WHERE id = '<pk>'` update or delete is the most ordinary statement there
//! is, and it lowers to `DocumentOp::PointUpdate` / `PointDelete` — plans that
//! carry no row body at all. The existing coverage exercises the BULK shapes,
//! which name their rows by predicate and are resolved from a reconnaissance
//! scan of that predicate; nothing exercised the point shapes end to end, so a
//! resolution that never covered them went unnoticed.
//!
//! `INSERT ... ON CONFLICT DO UPDATE` onto an EXISTING row has the same shape
//! from the other side: it folds as an update of the stored row by the submitted
//! body, so rewriting the join column moves value between two targets and only
//! one of them is named by anything the plan carries.
//!
//! The co-residency premise is asserted FIRST. Without that assertion a change
//! that moved the two collections onto different vShards would leave every
//! balance assertion below passing while testing a different path.

mod common;

use common::pgwire_harness::TestServer;

use nodedb::types::{DatabaseId, VShardId};

/// Target and source. The names are chosen for their HASHES: they collide on
/// one vShard, which is what makes every test below the co-resident path. See
/// [`materialized_sum_source_and_target_are_co_resident`].
const TARGET: &str = "uo_accounts";
const SOURCE: &str = "uo_entries";

/// The premise the whole file rests on: one core owns both collections, so a
/// balance rides the source write's own transaction rather than travelling on
/// an appended task.
#[test]
fn materialized_sum_source_and_target_are_co_resident() {
    assert_eq!(
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, SOURCE),
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, TARGET),
        "the point-shape tests in this file must exercise the CO-RESIDENT path; \
         rename the collections until the two hashes agree again"
    );
}

/// Create both collections, declare the binding, and seed one target row with a
/// zero balance.
async fn start_with_binding() -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "CREATE COLLECTION {TARGET} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the target collection");
    server
        .exec(&format!(
            "CREATE COLLECTION {SOURCE} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the source collection");
    server
        .exec(&format!(
            "ALTER COLLECTION {TARGET} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {SOURCE} \
             ON {SOURCE}.account_id = {TARGET}.id VALUE {SOURCE}.amount"
        ))
        .await
        .expect("declare the materialized sum");
    server
}

/// Add one target row starting from a zero balance.
async fn seed_account(server: &TestServer, id: &str) {
    server
        .exec(&format!(
            "INSERT INTO {TARGET} (id, owner, balance) VALUES ('{id}', 'alice', '0')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed account {id}: {e}"));
}

/// Add one source row, which credits its account.
async fn insert_entry(server: &TestServer, id: &str, account: &str, amount: &str) {
    server
        .exec(&format!(
            "INSERT INTO {SOURCE} (id, account_id, amount) \
             VALUES ('{id}', '{account}', '{amount}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert entry {id}: {e}"));
}

/// The stored balance of one target row.
async fn balance(server: &TestServer, id: &str) -> String {
    let rows = server
        .query_text(&format!("SELECT balance FROM {TARGET} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("read balance of {id}: {e}"));
    rows.into_iter()
        .next()
        .unwrap_or_else(|| panic!("target row {id} must exist"))
}

/// A binding declared AFTER its source collection exists must still fold.
///
/// The binding is stored on the TARGET, but the Data-Plane config that decides
/// whether a write folds it is derived for the SOURCE — from a catalog scan for
/// bindings naming that collection as their source. Propagating only the target
/// after the `ALTER` left the source asserting it drove nothing, so a plain
/// INSERT folded nothing and the total silently stayed put.
///
/// The narrowest statement of that defect, kept separate from the point shapes
/// because it has nothing to do with them: it is an INSERT, the shape that
/// always carried its own join value. Restarting the server would have healed
/// it — the boot seed re-derives every collection's config from the whole
/// catalog — so this must stay a first-write assertion, with no restart between
/// the `ALTER` and the `INSERT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_sum_binding_declared_after_the_source_folds_on_the_next_write() {
    let server = start_with_binding().await;
    seed_account(&server, "acc-1").await;

    insert_entry(&server, "e1", "acc-1", "25").await;

    assert_eq!(
        balance(&server, "acc-1").await,
        "25",
        "the very first write after the binding was declared must credit the target"
    );
}

/// A point UPDATE contributes the DIFFERENCE between the row's old and new
/// amount — and it must be applied at all.
///
/// The plan for `UPDATE ... WHERE id = '<pk>'` carries field assignments, not a
/// row, so the join key it needs is readable only from the stored row. A
/// resolution that skipped this shape left the statement with no target to
/// address and failed the most ordinary write against a source collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_sum_point_update_moves_the_balance_by_the_difference() {
    let server = start_with_binding().await;
    seed_account(&server, "acc-1").await;
    insert_entry(&server, "e1", "acc-1", "25").await;
    assert_eq!(balance(&server, "acc-1").await, "25");

    server
        .exec(&format!(
            "UPDATE {SOURCE} SET amount = '40' WHERE id = 'e1'"
        ))
        .await
        .expect("a point UPDATE on a materialized-sum source must be accepted");

    assert_eq!(
        balance(&server, "acc-1").await,
        "40",
        "the balance must move by the difference (25 → 40), not by the whole new amount"
    );
}

/// A point DELETE takes the removed row's contribution back off the total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_sum_point_delete_takes_the_rows_contribution_off_the_balance() {
    let server = start_with_binding().await;
    seed_account(&server, "acc-1").await;
    insert_entry(&server, "e1", "acc-1", "25").await;
    insert_entry(&server, "e2", "acc-1", "75").await;
    assert_eq!(balance(&server, "acc-1").await, "100");

    server
        .exec(&format!("DELETE FROM {SOURCE} WHERE id = 'e2'"))
        .await
        .expect("a point DELETE on a materialized-sum source must be accepted");

    assert_eq!(
        balance(&server, "acc-1").await,
        "25",
        "the deleted row's contribution must come off the total"
    );
    assert_eq!(
        server
            .query_text(&format!("SELECT id FROM {SOURCE} ORDER BY id"))
            .await
            .expect("read back the source rows"),
        vec!["e1".to_string()],
        "the source row must actually be gone"
    );
}

/// A point UPDATE that rewrites the JOIN KEY moves the row's value between two
/// targets, so BOTH must be resolved.
///
/// Resolving only one side leaves the other's total wrong by the row's whole
/// value — and neither side is named by the plan: the target the row leaves
/// lives in the stored row, the target it joins in the assignment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_sum_point_update_changing_the_join_key_moves_value_between_targets() {
    let server = start_with_binding().await;
    seed_account(&server, "acc-1").await;
    seed_account(&server, "acc-2").await;
    insert_entry(&server, "e1", "acc-1", "30").await;
    assert_eq!(balance(&server, "acc-1").await, "30");
    assert_eq!(balance(&server, "acc-2").await, "0");

    server
        .exec(&format!(
            "UPDATE {SOURCE} SET account_id = 'acc-2' WHERE id = 'e1'"
        ))
        .await
        .expect("a point UPDATE that rewrites the join key must be accepted");

    assert_eq!(
        balance(&server, "acc-1").await,
        "0",
        "the target the row LEFT must lose the row's whole value"
    );
    assert_eq!(
        balance(&server, "acc-2").await,
        "30",
        "the target the row JOINED must gain it"
    );
}

/// An UPSERT onto an EXISTING row is an update of the stored row, so rewriting
/// the join key on the conflict branch moves value between two targets too.
///
/// The plan carries the submitted body, which names only the target the row
/// joins. The one it abandons is named by the stored row and by nothing else —
/// the same gap as the point update, reached through a different statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_sum_upsert_changing_the_join_key_debits_the_target_it_abandons() {
    let server = start_with_binding().await;
    seed_account(&server, "acc-1").await;
    seed_account(&server, "acc-2").await;
    insert_entry(&server, "e1", "acc-1", "30").await;
    assert_eq!(balance(&server, "acc-1").await, "30");

    server
        .exec(&format!(
            "INSERT INTO {SOURCE} (id, account_id, amount) VALUES ('e1', 'acc-2', '30') \
             ON CONFLICT (id) DO UPDATE SET account_id = 'acc-2'"
        ))
        .await
        .expect("an upsert onto an existing source row must be accepted");

    assert_eq!(
        balance(&server, "acc-1").await,
        "0",
        "the target the upserted row LEFT must lose its contribution"
    );
    assert_eq!(
        balance(&server, "acc-2").await,
        "30",
        "the target it JOINED must gain it"
    );
}

/// An UPDATE naming a join value with no target row still fails the statement,
/// on the Control Plane, before anything is written.
///
/// Resolving the point shapes must not turn an unresolvable target into a
/// silently-skipped row: a skipped row is a stored total that disagrees with the
/// `SUM(...)` over the source rows forever after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_sum_point_update_onto_a_missing_target_is_refused() {
    let server = start_with_binding().await;
    seed_account(&server, "acc-1").await;
    insert_entry(&server, "e1", "acc-1", "30").await;

    let refusal = server
        .exec(&format!(
            "UPDATE {SOURCE} SET account_id = 'acc-missing' WHERE id = 'e1'"
        ))
        .await
        .expect_err("an update onto a target row that does not exist must be refused");
    assert!(
        refusal.to_lowercase().contains("acc-missing"),
        "the refusal must name the join value that could not be resolved: {refusal}"
    );

    assert_eq!(
        balance(&server, "acc-1").await,
        "30",
        "a refused statement must leave the total exactly where it was"
    );
}
