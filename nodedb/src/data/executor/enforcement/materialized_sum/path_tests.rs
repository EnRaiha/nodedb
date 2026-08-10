// SPDX-License-Identifier: BUSL-1.1

//! Every write path that folds materialized sums, driven end to end through its
//! own handler.
//!
//! `delta.rs` proves the arithmetic and `apply.rs` proves the write-back. What
//! neither can prove is that each PATH reaches them: a bulk update, a bulk
//! delete, a `TRUNCATE`, an `UPDATE ... FROM` and a batch insert each match
//! their rows differently, and a path that folds nothing leaves a stored total
//! that silently disagrees with the `SUM(...)` over the source rows. These tests
//! assert the total actually moved, per path.

use nodedb_physical::physical_plan::{MaterializedSumBinding, UpdateValue};
use nodedb_types::Surrogate;

use crate::bridge::envelope::{ErrorCode, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::funnel::run_write_enforcement;
use crate::data::executor::enforcement::images::{EnforcementCtx, RowImages};
use crate::data::executor::handlers::bulk_dml::{
    BulkDeleteParams, BulkUpdateParams, OllpPrediction,
};
use crate::data::executor::handlers::document::write::DocumentBatchInsertParams;
use crate::data::executor::handlers::update_from_join::UpdateFromJoinParams;
use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
use crate::types::{DatabaseId, TenantId};

const DB: u64 = 0;
const TID: u64 = 1;
/// The collection that DRIVES the binding — the one every path below writes.
const SOURCE: &str = "local_charges";
/// The collection whose `balance` column the binding maintains, sharing
/// `SOURCE`'s vShard.
///
/// Co-residency is not decoration here. Every test in this file drives the
/// INLINE fold and then reads the target row back out of the SOURCE core's own
/// document store — and each core opens its own store, so that read only sees
/// the write when one core owns both rows. The pair is asserted by
/// [`the_local_fixture_is_co_resident`] rather than assumed, so a change to the
/// collection hash reports itself by name instead of quietly turning this file
/// into a test of the deferred path.
const TARGET: &str = "local_balances";
/// A target that does NOT share `SOURCE`'s vShard, for the tests that pin the
/// deferred rule.
const REMOTE_TARGET: &str = "remote_balances";
/// A third collection, read-only, standing in as the FROM side of a joined
/// update.
const JOIN_SOURCE: &str = "local_rates";

const ACCOUNT_A: &str = "a1";
const ACCOUNT_B: &str = "a2";
const SURROGATE_A: Surrogate = Surrogate(4242);
const SURROGATE_B: Surrogate = Surrogate(4343);

fn binding_onto(target: &str) -> MaterializedSumBinding {
    MaterializedSumBinding {
        target_collection: target.to_string(),
        target_column: "balance".to_string(),
        join_column: "account_id".to_string(),
        value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
    }
}

fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
    (
        DatabaseId::DEFAULT,
        TenantId::new(TID),
        collection.to_string(),
    )
}

/// Register the three collections: the binding-driving source, its target, and
/// the read-only join side.
fn register_collections(core: &mut CoreLoop) {
    register_collections_onto(core, TARGET);
}

/// Register the three collections with the source's binding pointed at
/// `target`.
fn register_collections_onto(core: &mut CoreLoop, target: &str) {
    let mut source = CollectionConfig::new(SOURCE);
    source.enforcement.materialized_sum_sources = vec![binding_onto(target)];
    core.doc_configs.insert(config_key(SOURCE), source);
    core.doc_configs
        .insert(config_key(target), CollectionConfig::new(target));
    core.doc_configs
        .insert(config_key(JOIN_SOURCE), CollectionConfig::new(JOIN_SOURCE));
}

/// The premise every other test in this file rests on.
#[test]
fn the_local_fixture_is_co_resident() {
    assert!(
        crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, TARGET),
        "'{SOURCE}' and '{TARGET}' must share a vShard: the inline fold writes the target \
         inside the source's transaction, on the source's core, and each core opens its own \
         document store"
    );
    assert!(
        !crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, REMOTE_TARGET),
        "'{REMOTE_TARGET}' must NOT share '{SOURCE}'s vShard; it pins the deferred rule"
    );
}

fn seed_target(core: &mut CoreLoop, surrogate: Surrogate, id: &str, balance: &str) {
    seed_target_in(core, TARGET, surrogate, id, balance);
}

fn seed_target_in(
    core: &mut CoreLoop,
    collection: &str,
    surrogate: Surrogate,
    id: &str,
    balance: &str,
) {
    let row = serde_json::json!({"id": id, "balance": balance});
    core.sparse
        .put(
            DB,
            TID,
            collection,
            &surrogate_to_doc_id(surrogate),
            &doc_format::encode_to_msgpack(&row),
        )
        .expect("seed target row");
}

fn seed_source(core: &mut CoreLoop, surrogate: Surrogate, account: &str, amount: i64) {
    let row = serde_json::json!({"account_id": account, "amount": amount});
    core.sparse
        .put(
            DB,
            TID,
            SOURCE,
            &surrogate_to_doc_id(surrogate),
            &doc_format::encode_to_msgpack(&row),
        )
        .expect("seed source row");
}

fn balance_of(core: &CoreLoop, surrogate: Surrogate) -> String {
    balance_in(core, TARGET, surrogate)
}

fn balance_in(core: &CoreLoop, collection: &str, surrogate: Surrogate) -> String {
    let stored = core
        .sparse
        .get(DB, TID, collection, &surrogate_to_doc_id(surrogate))
        .expect("read target row")
        .expect("target row must still exist");
    doc_format::decode_document(&stored)
        .expect("target row must decode")
        .get("balance")
        .and_then(|v| v.as_str())
        .expect("target row must carry a balance")
        .to_string()
}

fn literal(value: serde_json::Value) -> UpdateValue {
    UpdateValue::Literal(nodedb_types::json_to_msgpack(&value).expect("encode literal"))
}

/// A bulk UPDATE contributes each matched row's DIFFERENCE, not its whole new
/// value: two rows moved from 30 and 20 to 50 add 20 + 30 to the total, never
/// 100.
#[test]
fn bulk_update_moves_the_total_by_the_difference() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
    seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
    seed_source(&mut core, Surrogate(2), ACCOUNT_A, 20);

    let updates = vec![("amount".to_string(), literal(serde_json::json!(50)))];
    let resolved = vec![(ACCOUNT_A.to_string(), SURROGATE_A)];
    let task = make_default_task();
    let response = core.execute_bulk_update(
        &task,
        TID,
        BulkUpdateParams {
            collection: SOURCE,
            filter_bytes: &[],
            updates: &updates,
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &resolved,
        },
    );

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(balance_of(&core, SURROGATE_A), "150");
}

/// A bulk DELETE takes every removed row's contribution back off the total.
#[test]
fn bulk_delete_subtracts_every_removed_rows_contribution() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
    seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
    seed_source(&mut core, Surrogate(2), ACCOUNT_A, 20);

    let resolved = vec![(ACCOUNT_A.to_string(), SURROGATE_A)];
    let task = make_default_task();
    let response = core.execute_bulk_delete(
        &task,
        TID,
        BulkDeleteParams {
            collection: SOURCE,
            filter_bytes: &[],
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &resolved,
            ollp: OllpPrediction {
                surrogates: None,
                edges: None,
            },
        },
    );

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(balance_of(&core, SURROGATE_A), "50");
}

/// `TRUNCATE` on the source zeroes EVERY target the collection's rows
/// contributed to — it must leave the totals exactly where N individual deletes
/// would.
#[test]
fn truncate_zeroes_every_target_balance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "30");
    seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");
    seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
    seed_source(&mut core, Surrogate(2), ACCOUNT_B, 50);

    let resolved = vec![
        (ACCOUNT_A.to_string(), SURROGATE_A),
        (ACCOUNT_B.to_string(), SURROGATE_B),
    ];
    let task = make_default_task();
    let response = core.execute_truncate(&task, TID, SOURCE, &resolved);

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(balance_of(&core, SURROGATE_A), "0");
    assert_eq!(
        balance_of(&core, SURROGATE_B),
        "0",
        "a target only the SECOND removed row contributed to must be zeroed too"
    );
}

/// `UPDATE ... FROM` folds the difference of every row the join matched.
#[test]
fn update_from_join_moves_the_total_by_the_difference() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
    // The written row carries the join column of the FROM side as well.
    let entry = serde_json::json!({"account_id": ACCOUNT_A, "amount": 30, "rate_id": "r1"});
    core.sparse
        .put(
            DB,
            TID,
            SOURCE,
            &surrogate_to_doc_id(Surrogate(1)),
            &doc_format::encode_to_msgpack(&entry),
        )
        .expect("seed written row");

    let rate = serde_json::json!({"rate_id": "r1", "amount": 80});
    let source_rows = vec![(
        surrogate_to_doc_id(Surrogate(9)),
        doc_format::encode_to_msgpack(&rate),
    )];

    let updates = vec![("amount".to_string(), literal(serde_json::json!(80)))];
    let resolved = vec![(ACCOUNT_A.to_string(), SURROGATE_A)];
    let task = make_default_task();
    let response = core.execute_update_from_join(
        &task,
        TID,
        UpdateFromJoinParams {
            target_collection: SOURCE,
            source_collection: JOIN_SOURCE,
            source_alias: "r",
            target_join_col: "rate_id",
            source_join_col: "rate_id",
            updates: &updates,
            target_filter_bytes: &[],
            returning: None,
            resolve_only: false,
            source_rows: Some(source_rows.as_slice()),
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &resolved,
        },
    );

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(balance_of(&core, SURROGATE_A), "150");
}

/// The batch insert an `INSERT ... SELECT` page ships credits its targets.
///
/// The orchestrator re-issues the copy through `dispatch_local`, which never
/// passes through the statement-level resolution pass — so a page shipping an
/// empty resolution would leave the total short of the rows it inserted.
#[test]
fn insert_select_page_credits_its_targets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");

    let documents = vec![
        (
            surrogate_to_doc_id(Surrogate(1)),
            doc_format::encode_to_msgpack(
                &serde_json::json!({"account_id": ACCOUNT_A, "amount": 25}),
            ),
        ),
        (
            surrogate_to_doc_id(Surrogate(2)),
            doc_format::encode_to_msgpack(
                &serde_json::json!({"account_id": ACCOUNT_A, "amount": 75}),
            ),
        ),
    ];
    let surrogates = vec![Surrogate(1), Surrogate(2)];
    let resolved = vec![(ACCOUNT_A.to_string(), SURROGATE_A)];
    let task = make_default_task();
    let response = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            tid: TID,
            collection: SOURCE,
            documents: &documents,
            surrogates: &surrogates,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &resolved,
            deferred_sum_targets: &[],
        },
    );

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(balance_of(&core, SURROGATE_A), "200");
}

/// A resolution that no longer covers the matched rows is REFUSED before
/// anything is written.
///
/// This is the drift the recon scan cannot rule out: a row that joined the match
/// set after the Control Plane resolved its targets. Folding it would fail
/// mid-statement with earlier rows already removed, leaving a stored total that
/// still counts rows the statement deleted. The leader answers
/// `OllpRetryRequired` instead, having removed nothing, and the coordinator
/// re-resolves.
///
/// CO-RESIDENT, and that is what makes an uncovered value a defect here: this
/// core applies the binding itself, so nothing ever removes a join value from
/// the resolution and a missing one can only mean the plan no longer covers the
/// rows. The cross-shard half of the rule, where a missing value is the
/// deliberate deferral signal, is
/// [`an_uncovered_cross_shard_join_value_is_deferred_rather_than_retried`].
#[test]
fn an_uncovered_join_value_retries_instead_of_writing_a_wrong_total() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
    seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");
    seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
    // The row the resolution below does not know about — it arrived after the
    // Control Plane scanned.
    seed_source(&mut core, Surrogate(2), ACCOUNT_B, 20);

    let resolved = vec![(ACCOUNT_A.to_string(), SURROGATE_A)];
    let task = make_default_task();
    let response = core.execute_bulk_delete(
        &task,
        TID,
        BulkDeleteParams {
            collection: SOURCE,
            filter_bytes: &[],
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &resolved,
            ollp: OllpPrediction {
                surrogates: None,
                edges: None,
            },
        },
    );

    assert_eq!(
        response.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired),
        "an uncovered join value must ask for a retry, not write a partial total"
    );
    assert_eq!(
        balance_of(&core, SURROGATE_A),
        "100",
        "the covered target must be untouched: the statement wrote nothing"
    );
    assert_eq!(balance_of(&core, SURROGATE_B), "50");
    assert!(
        core.sparse
            .get(DB, TID, SOURCE, &surrogate_to_doc_id(Surrogate(1)))
            .expect("read source row")
            .is_some(),
        "no source row may be removed on a refused statement"
    );
}

/// The MIRROR of the retry test, and the rule that keeps the guard honest: for a
/// CROSS-SHARD binding an uncovered join value is not drift at all.
///
/// The Control Plane settles such a binding at plan time and REMOVES its join
/// values from the resolution — that removal is how this core is told to stand
/// down. Demanding coverage for them anyway would report a divergence on every
/// single cross-shard predicate write, and the coordinator would re-recon,
/// resolve, remove them again and resubmit: a livelock, not a retry, on a
/// dataset nobody else is touching.
///
/// Residency is the discriminator, and it is total. A co-resident binding is
/// never omitted, so an uncovered value there is a genuine shortfall and
/// `an_uncovered_join_value_retries_instead_of_writing_a_wrong_total` still
/// demands the retry. A cross-shard binding is always omitted once settled, so
/// an uncovered value there is the deferral. Drift on the cross-shard side is
/// caught by the settlement's own read-set entry over the images the shipped
/// deltas were folded from, which the Calvin OCC check validates on this core
/// before any mutation — a different guard, not a missing one.
///
/// The balance assertion is what makes "stood down" mean something: the
/// statement must proceed AND leave the total alone, because the sibling task
/// owns it.
#[test]
fn an_uncovered_cross_shard_join_value_is_deferred_rather_than_retried() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections_onto(&mut core, REMOTE_TARGET);
    seed_target_in(&mut core, REMOTE_TARGET, SURROGATE_A, ACCOUNT_A, "100");
    seed_target_in(&mut core, REMOTE_TARGET, SURROGATE_B, ACCOUNT_B, "50");
    seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
    seed_source(&mut core, Surrogate(2), ACCOUNT_B, 20);

    // Empty, not partial: a settled cross-shard binding has every one of its
    // join values removed, so this is the ORDINARY shape of the plan, not a
    // damaged one.
    let task = make_default_task();
    let response = core.execute_bulk_delete(
        &task,
        TID,
        BulkDeleteParams {
            collection: SOURCE,
            filter_bytes: &[],
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &[],
            ollp: OllpPrediction {
                surrogates: None,
                edges: None,
            },
        },
    );

    assert_eq!(
        response.status,
        Status::Ok,
        "a settled cross-shard binding must not be reported as drift: {:?}",
        response.error_code
    );
    assert_ne!(
        response.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired),
        "demanding coverage for a deliberately-omitted join value livelocks the statement"
    );
    assert_eq!(
        balance_in(&core, REMOTE_TARGET, SURROGATE_A),
        "100",
        "the balance travels on its own task; this core must not move it"
    );
    assert_eq!(balance_in(&core, REMOTE_TARGET, SURROGATE_B), "50");
    assert!(
        core.sparse
            .get(DB, TID, SOURCE, &surrogate_to_doc_id(Surrogate(1)))
            .expect("read source row")
            .is_none(),
        "the statement itself must have run: the matched source rows are gone"
    );
}

/// A covered resolution is NOT treated as drift — the guard is coverage, so an
/// entry the statement turns out not to need costs one unused surrogate and
/// never a spurious retry.
#[test]
fn an_over_resolved_plan_is_not_a_divergence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
    seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");
    seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);

    let resolved = vec![
        (ACCOUNT_A.to_string(), SURROGATE_A),
        // Resolved, then the row that needed it was removed by someone else.
        (ACCOUNT_B.to_string(), SURROGATE_B),
    ];
    let task = make_default_task();
    let response = core.execute_bulk_delete(
        &task,
        TID,
        BulkDeleteParams {
            collection: SOURCE,
            filter_bytes: &[],
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &resolved,
            ollp: OllpPrediction {
                surrogates: None,
                edges: None,
            },
        },
    );

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(balance_of(&core, SURROGATE_A), "70");
    assert_eq!(
        balance_of(&core, SURROGATE_B),
        "50",
        "an unused resolution entry must move no total"
    );
}

/// The MERGE update arm that REWRITES the join key debits the target the row
/// leaves and credits the one it joins — one arm, two targets, opposite signs.
///
/// The insert and delete arms are covered in `apply.rs`; this is the arm whose
/// two-sided split is derived rather than carried, and accounting it as a single
/// positive contribution leaves the abandoned target permanently overstated.
#[test]
fn a_merge_update_arm_that_moves_the_join_key_touches_both_targets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    register_collections(&mut core);
    seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
    seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");

    let old_doc = serde_json::json!({"account_id": ACCOUNT_A, "amount": 30});
    let new_doc = serde_json::json!({"account_id": ACCOUNT_B, "amount": 40});
    let txn = core.sparse.begin_write().expect("begin write");
    run_write_enforcement(
        &mut core,
        &txn,
        EnforcementCtx {
            database_id: DB,
            tid: TID,
            collection: SOURCE,
            resolved_targets: &[
                (ACCOUNT_A.to_string(), SURROGATE_A),
                (ACCOUNT_B.to_string(), SURROGATE_B),
            ],
            deferred_sum_targets: &[],
            wal_lsn: None,
        },
        RowImages::Update {
            old_doc: &old_doc,
            new_doc: &new_doc,
        },
    )
    .expect("a join-key move must be applied to both targets");
    txn.commit().expect("commit");

    assert_eq!(
        balance_of(&core, SURROGATE_A),
        "70",
        "the target the row left loses its old value"
    );
    assert_eq!(
        balance_of(&core, SURROGATE_B),
        "90",
        "the target the row joined gains its new value"
    );
}
