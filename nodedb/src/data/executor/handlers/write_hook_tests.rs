// SPDX-License-Identifier: BUSL-1.1

//! Every point / upsert write path folds its images, and a collection that
//! declared its rows immutable refuses to mutate them.
//!
//! One module rather than one per handler, because the property under test is
//! the same property in each: the stored total after a write equals the total
//! the source rows imply. A handler that skipped the hook would report success
//! and leave that equality broken, which no test of the handler's own return
//! value can see.

use crate::bridge::envelope::Status;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::doc_format;
use crate::data::executor::handlers::document::write::DocumentBatchInsertParams;
use crate::data::executor::handlers::point::delete::PointDeleteExec;
use crate::data::executor::handlers::point::insert::PointInsertParams;
use crate::data::executor::handlers::point::put::PointPutExec;
use crate::data::executor::handlers::point::update::PointUpdateParams;
use crate::data::executor::handlers::upsert::UpsertParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::{
    DocumentOp, MaterializedSumBinding, PhysicalPlan, ResolvedSumTarget, UpdateValue,
};
use nodedb_types::Surrogate;

const DB: u64 = 0;
const TID: u64 = 1;
/// The collection that drives the binding.
const SOURCE: &str = "point_txns";
/// The collection whose `balance` the binding maintains, sharing `SOURCE`'s
/// vShard.
///
/// Every test here asserts the INLINE fold: the target row is seeded into, and
/// read back out of, the SOURCE core's own document store. Each core opens its
/// own store, so that assertion only means anything when one core owns both
/// rows — and a cross-shard binding is deliberately not applied inline at all.
/// [`the_fixture_is_co_resident`] asserts the pair rather than trusting the
/// collection hash to keep producing it.
const TARGET: &str = "point_holders";
const A1: &str = "a1";
const A2: &str = "a2";
const T1: Surrogate = Surrogate(4001);
const T2: Surrogate = Surrogate(4002);

/// The premise every test below rests on.
#[test]
fn the_fixture_is_co_resident() {
    assert!(
        crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, TARGET),
        "'{SOURCE}' and '{TARGET}' must share a vShard: a cross-shard binding's balance \
         travels on its own task and is never folded into the source write's transaction"
    );
}

/// `SUM(amount)` per `account_id`, materialized onto the target's `balance`.
fn binding() -> MaterializedSumBinding {
    MaterializedSumBinding {
        target_collection: TARGET.to_string(),
        target_column: "balance".to_string(),
        join_column: "account_id".to_string(),
        value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
    }
}

/// Both accounts, resolved the way the Control Plane resolves them at plan time
/// — each entry naming the TARGET collection its binding points at.
fn resolved() -> Vec<ResolvedSumTarget> {
    vec![
        ResolvedSumTarget::new(TARGET, A1, T1),
        ResolvedSumTarget::new(TARGET, A2, T2),
    ]
}

fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
    (
        DatabaseId::DEFAULT,
        TenantId::new(TID),
        collection.to_string(),
    )
}

/// A source collection bound to the sum, and two target rows starting at zero.
fn seeded_core(dir: &std::path::Path) -> CoreLoop {
    let (mut core, _req, _resp) = make_core_with_dir(dir);

    let mut source = CollectionConfig::new(SOURCE);
    source.enforcement.materialized_sum_sources = vec![binding()];
    core.doc_configs.insert(config_key(SOURCE), source);
    core.doc_configs
        .insert(config_key(TARGET), CollectionConfig::new(TARGET));

    for (id, surrogate) in [(A1, T1), (A2, T2)] {
        let seed = serde_json::json!({"id": id, "balance": "0"});
        core.sparse
            .put(
                DB,
                TID,
                TARGET,
                &surrogate_to_doc_id(surrogate),
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");
    }
    core
}

/// A source row body, in the MessagePack every handler receives.
fn entry(account: &str, amount: i64) -> Vec<u8> {
    doc_format::encode_to_msgpack(&serde_json::json!({
        "account_id": account,
        "amount": amount,
    }))
}

/// The balance the target row currently holds.
fn balance(core: &CoreLoop, surrogate: Surrogate) -> String {
    let stored = core
        .sparse
        .get(DB, TID, TARGET, &surrogate_to_doc_id(surrogate))
        .expect("read target")
        .expect("target row must exist");
    doc_format::decode_document(&stored)
        .expect("target row must decode")
        .get("balance")
        .and_then(|v| v.as_str())
        .expect("target row must carry a balance")
        .to_string()
}

fn insert(core: &mut CoreLoop, task: &ExecutionTask, surrogate: Surrogate, body: &[u8]) -> Status {
    let targets = resolved();
    let document_id = format!("e{}", surrogate.as_u32());
    core.execute_point_insert(PointInsertParams {
        task,
        tid: TID,
        collection: SOURCE,
        document_id: &document_id,
        surrogate,
        value: body,
        if_absent: false,
        returning: None,
        rls_filters: &[],
        resolved_sum_targets: &targets,
        deferred_sum_targets: &[],
    })
    .status
}

/// One PUT of `body` onto the same source row.
fn put(core: &mut CoreLoop, task: &ExecutionTask, body: &[u8]) -> Status {
    let targets = resolved();
    core.execute_point_put(
        task,
        PointPutExec {
            tid: TID,
            collection: SOURCE,
            document_id: "e1",
            surrogate: Surrogate(21),
            value: body,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &targets,
        },
    )
    .status
}

/// One UPSERT of `body` onto the same source row.
fn upsert(core: &mut CoreLoop, task: &ExecutionTask, body: &[u8]) -> Status {
    let targets = resolved();
    core.execute_upsert(
        task,
        UpsertParams {
            tid: TID,
            collection: SOURCE,
            document_id: "e61",
            surrogate: Surrogate(61),
            value: body,
            on_conflict_updates: &[],
            rls_write_check: &[],
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &targets,
        },
    )
    .status
}

#[test]
fn point_insert_credits_the_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();

    assert_eq!(
        insert(&mut core, &task, Surrogate(11), &entry(A1, 25)),
        Status::Ok
    );
    assert_eq!(
        insert(&mut core, &task, Surrogate(12), &entry(A1, 75)),
        Status::Ok
    );

    assert_eq!(balance(&core, T1), "100", "both inserts must be totalled");
    assert_eq!(
        balance(&core, T2),
        "0",
        "an untouched account must not move"
    );
}

#[test]
fn point_put_credits_an_insert_and_deltas_an_overwrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();

    assert_eq!(put(&mut core, &task, &entry(A1, 10)), Status::Ok);
    assert_eq!(put(&mut core, &task, &entry(A1, 30)), Status::Ok);

    assert_eq!(
        balance(&core, T1),
        "30",
        "an overwrite must delta the total, not add the new amount on top of \
         the old one"
    );
}

#[test]
fn point_delete_takes_the_row_back_off_the_total() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();
    let targets = resolved();

    assert_eq!(
        insert(&mut core, &task, Surrogate(31), &entry(A1, 40)),
        Status::Ok
    );
    assert_eq!(balance(&core, T1), "40");

    let resp = core.execute_point_delete(
        &task,
        PointDeleteExec {
            tid: TID,
            collection: SOURCE,
            document_id: "e31",
            surrogate: Surrogate(31),
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &targets,
        },
    );
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(
        balance(&core, T1),
        "0",
        "a deleted row's contribution must come back off the total"
    );
}

#[test]
fn point_update_moves_the_amount_when_the_join_key_changes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();
    let targets = resolved();

    assert_eq!(
        insert(&mut core, &task, Surrogate(41), &entry(A1, 60)),
        Status::Ok
    );
    assert_eq!(balance(&core, T1), "60");

    let moved = nodedb_types::json_to_msgpack(&serde_json::json!(A2)).expect("encode");
    let updates = vec![("account_id".to_string(), UpdateValue::Literal(moved))];
    let resp = core.execute_point_update(
        &task,
        PointUpdateParams {
            tid: TID,
            collection: SOURCE,
            document_id: "e41",
            surrogate: Surrogate(41),
            updates: &updates,
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &targets,
        },
    );
    assert_eq!(resp.status, Status::Ok);

    assert_eq!(
        balance(&core, T1),
        "0",
        "the account the row left must lose the amount"
    );
    assert_eq!(
        balance(&core, T2),
        "60",
        "the account the row joined must gain it"
    );
}

#[test]
fn point_update_deltas_the_amount_in_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();
    let targets = resolved();

    assert_eq!(
        insert(&mut core, &task, Surrogate(51), &entry(A1, 10)),
        Status::Ok
    );

    let raised = nodedb_types::json_to_msgpack(&serde_json::json!(35)).expect("encode");
    let updates = vec![("amount".to_string(), UpdateValue::Literal(raised))];
    let resp = core.execute_point_update(
        &task,
        PointUpdateParams {
            tid: TID,
            collection: SOURCE,
            document_id: "e51",
            surrogate: Surrogate(51),
            updates: &updates,
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &targets,
        },
    );
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(
        balance(&core, T1),
        "35",
        "the total must move by the difference, not by the new amount"
    );
}

#[test]
fn upsert_credits_on_insert_and_deltas_on_conflict() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();

    assert_eq!(upsert(&mut core, &task, &entry(A1, 20)), Status::Ok);
    assert_eq!(balance(&core, T1), "20", "the insert arm must credit");

    assert_eq!(upsert(&mut core, &task, &entry(A1, 50)), Status::Ok);
    assert_eq!(
        balance(&core, T1),
        "50",
        "the conflict arm must delta the total against the pre-merge row"
    );
}

#[test]
fn batch_insert_credits_every_row_of_the_page() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();
    let targets = resolved();

    let documents = vec![
        ("e71".to_string(), entry(A1, 5)),
        ("e72".to_string(), entry(A2, 7)),
        ("e73".to_string(), entry(A1, 11)),
    ];
    let surrogates = vec![Surrogate(71), Surrogate(72), Surrogate(73)];
    let resp = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            tid: TID,
            collection: SOURCE,
            documents: &documents,
            surrogates: &surrogates,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &targets,
            deferred_sum_targets: &[],
        },
    );
    assert_eq!(resp.status, Status::Ok);

    assert_eq!(balance(&core, T1), "16", "5 + 11 land on the first account");
    assert_eq!(balance(&core, T2), "7", "7 lands on the second");
}

#[test]
fn a_transactional_delete_takes_the_row_back_off_the_total() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = seeded_core(dir.path());
    let task = make_default_task();

    assert_eq!(
        insert(&mut core, &task, Surrogate(81), &entry(A1, 90)),
        Status::Ok
    );
    assert_eq!(balance(&core, T1), "90");

    let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: SOURCE.to_string(),
        document_id: "e81".to_string(),
        surrogate: Surrogate(81),
        pk_bytes: b"e81".to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: Vec::new(),
        resolved_sum_targets: resolved(),
    });
    let resp = core.execute_transaction_batch(&task, TID, &[plan], &[], None);
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(
        balance(&core, T1),
        "0",
        "a delete inside a transaction must debit the target too"
    );
}

/// A hash-chained collection, exactly as DDL builds one: `HASH_CHAIN` implies
/// `APPEND_ONLY`.
fn chained_core(dir: &std::path::Path) -> CoreLoop {
    let (mut core, _req, _resp) = make_core_with_dir(dir);
    let mut config = CollectionConfig::new(SOURCE);
    config.enforcement.append_only = true;
    config.enforcement.hash_chain = true;
    core.doc_configs.insert(config_key(SOURCE), config);
    core
}

fn insert_chained(core: &mut CoreLoop, task: &ExecutionTask) -> Status {
    core.execute_point_insert(PointInsertParams {
        task,
        tid: TID,
        collection: SOURCE,
        document_id: "e1",
        surrogate: Surrogate(91),
        value: &entry(A1, 10),
        if_absent: false,
        returning: None,
        rls_filters: &[],
        resolved_sum_targets: &[],
        deferred_sum_targets: &[],
    })
    .status
}

/// The chain must actually be built by the autocommit INSERT path, or the
/// refusal tests below would pass over rows that carry no link at all.
#[test]
fn a_point_insert_links_the_row_into_the_chain() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = chained_core(dir.path());
    let task = make_default_task();

    assert_eq!(insert_chained(&mut core, &task), Status::Ok);

    let stored = core
        .sparse
        .get(DB, TID, SOURCE, &surrogate_to_doc_id(Surrogate(91)))
        .expect("read back")
        .expect("row must exist");
    let doc = doc_format::decode_document(&stored).expect("decode");
    assert!(
        doc.get("_chain_hash").and_then(|v| v.as_str()).is_some(),
        "an autocommit INSERT into a hash-chained collection must store its link"
    );
    assert!(
        core.sparse
            .get_chain_head(DB, TID, SOURCE)
            .expect("read head")
            .is_some(),
        "and must persist the advanced head"
    );
}

/// Removing a link makes `verify_chain` report the row AFTER it as broken, so
/// the delete is refused rather than allowed to accuse an untampered row.
#[test]
fn a_delete_on_a_hash_chained_collection_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = chained_core(dir.path());
    let task = make_default_task();
    assert_eq!(insert_chained(&mut core, &task), Status::Ok);

    let resp = core.execute_point_delete(
        &task,
        PointDeleteExec {
            tid: TID,
            collection: SOURCE,
            document_id: "e1",
            surrogate: Surrogate(91),
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(resp.status, Status::Error);
    assert!(
        core.sparse
            .get(DB, TID, SOURCE, &surrogate_to_doc_id(Surrogate(91)))
            .expect("read back")
            .is_some(),
        "a refused delete must leave the chained row in place"
    );
}

/// Rewriting a link has the same effect as removing one.
#[test]
fn an_update_on_a_hash_chained_collection_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = chained_core(dir.path());
    let task = make_default_task();
    assert_eq!(insert_chained(&mut core, &task), Status::Ok);

    let raised = nodedb_types::json_to_msgpack(&serde_json::json!(999)).expect("encode");
    let updates = vec![("amount".to_string(), UpdateValue::Literal(raised))];
    let resp = core.execute_point_update(
        &task,
        PointUpdateParams {
            tid: TID,
            collection: SOURCE,
            document_id: "e1",
            surrogate: Surrogate(91),
            updates: &updates,
            returning: None,
            rls_filters: &[],
            rls_write_check: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(resp.status, Status::Error);

    let stored = core
        .sparse
        .get(DB, TID, SOURCE, &surrogate_to_doc_id(Surrogate(91)))
        .expect("read back")
        .expect("row must exist");
    let doc = doc_format::decode_document(&stored).expect("decode");
    assert_eq!(
        doc.get("amount").and_then(|v| v.as_i64()),
        Some(10),
        "a refused update must leave the chained row byte-identical"
    );
}

/// An UPSERT that lands on an existing chained row is an update, and is refused
/// on the same terms — the arm that used to write with a bare `sparse.put` ran
/// no admission at all.
#[test]
fn an_upsert_onto_an_existing_hash_chained_row_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = chained_core(dir.path());
    let task = make_default_task();
    assert_eq!(insert_chained(&mut core, &task), Status::Ok);

    let resp = core.execute_upsert(
        &task,
        UpsertParams {
            tid: TID,
            collection: SOURCE,
            document_id: "e1",
            surrogate: Surrogate(91),
            value: &entry(A1, 999),
            on_conflict_updates: &[],
            rls_write_check: &[],
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(resp.status, Status::Error);
}
