// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for Calvin dispatch classification and routing.

use super::*;
use crate::Error;
use crate::control::planner::calvin::cross_shard_mode::CrossShardTxnMode;
use crate::control::planner::calvin::types::{DispatchClass, DispatchOutcome};
use crate::control::server::shared::session::TransactionState;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::{ColumnarOp, CrdtOp, DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

fn doc_insert_task(vshard: u32) -> PhysicalTask {
    PhysicalTask {
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: format!("col_{vshard}"),
            document_id: "id1".to_owned(),
            surrogate: nodedb_types::Surrogate::new(1),
            value: vec![],
            if_absent: false,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

fn scan_task(vshard: u32) -> PhysicalTask {
    PhysicalTask {
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Document(DocumentOp::Scan {
            collection: format!("col_{vshard}"),
            filters: vec![],
            limit: 0,
            offset: 0,
            sort_keys: vec![],
            distinct: false,
            projection: vec![],
            computed_columns: vec![],
            window_functions: vec![],
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

fn bulk_update_task(vshard: u32) -> PhysicalTask {
    PhysicalTask {
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: format!("col_{vshard}"),
            filters: vec![],
            updates: vec![],
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }
}

#[test]
fn is_write_plan_classifies_correctly() {
    let write = doc_insert_task(0).plan;
    let read = scan_task(0).plan;
    assert!(is_write_plan(&write));
    assert!(!is_write_plan(&read));
}

#[test]
fn is_write_plan_classifies_crdt_list_ops() {
    let list_ops = [
        (
            "ListInsert",
            PhysicalPlan::Crdt(CrdtOp::ListInsert {
                collection: "docs".to_owned(),
                document_id: "id1".to_owned(),
                list_path: "blocks".to_owned(),
                index: 0,
                fields_json: "{}".to_owned(),
                surrogate: nodedb_types::Surrogate::new(1),
            }),
        ),
        (
            "ListDelete",
            PhysicalPlan::Crdt(CrdtOp::ListDelete {
                collection: "docs".to_owned(),
                document_id: "id1".to_owned(),
                list_path: "blocks".to_owned(),
                index: 0,
                surrogate: nodedb_types::Surrogate::new(1),
            }),
        ),
        (
            "ListMove",
            PhysicalPlan::Crdt(CrdtOp::ListMove {
                collection: "docs".to_owned(),
                document_id: "id1".to_owned(),
                list_path: "blocks".to_owned(),
                from_index: 0,
                to_index: 1,
                surrogate: nodedb_types::Surrogate::new(1),
            }),
        ),
    ];
    for (name, plan) in &list_ops {
        assert!(is_write_plan(plan), "{name} should classify as a write");
    }
}

#[test]
fn is_write_plan_classifies_columnar_update_and_delete() {
    let update = PhysicalPlan::Columnar(ColumnarOp::Update {
        collection: "metrics".to_owned(),
        filters: vec![],
        updates: vec![],
    });
    let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
        collection: "metrics".to_owned(),
        filters: vec![],
    });
    assert!(
        is_write_plan(&update),
        "ColumnarOp::Update should be a write"
    );
    assert!(
        is_write_plan(&delete),
        "ColumnarOp::Delete should be a write"
    );
}

#[test]
fn is_dependent_predicate_bulk_update() {
    let task = bulk_update_task(0);
    assert!(is_dependent_predicate(&task.plan));
}

#[test]
fn is_dependent_predicate_point_insert_is_false() {
    let task = doc_insert_task(0);
    assert!(!is_dependent_predicate(&task.plan));
}

#[test]
fn classify_dispatch_single_shard() {
    let tasks = vec![doc_insert_task(5), doc_insert_task(5)];
    let class = classify_dispatch(&tasks);
    assert!(matches!(
        class,
        DispatchClass::SingleShard { vshard } if vshard.as_u32() == 5
    ));
}

#[test]
fn classify_dispatch_multi_shard_returns_btreeset() {
    let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
    let class = classify_dispatch(&tasks);
    match class {
        DispatchClass::MultiShard { vshards } => {
            let v: Vec<u32> = vshards.into_iter().collect();
            assert_eq!(v, vec![3, 7]);
        }
        _ => panic!("expected MultiShard"),
    }
}

#[test]
fn classify_dispatch_zero_writes_is_single_shard() {
    let tasks = vec![scan_task(3), scan_task(7)];
    let class = classify_dispatch(&tasks);
    assert!(matches!(class, DispatchClass::SingleShard { .. }));
}

#[test]
fn predicate_class_byte_stable_across_runs() {
    let h1 = predicate_class("WHERE balance > 1000", "accounts");
    let h2 = predicate_class("WHERE balance > 1000", "accounts");
    assert_eq!(h1, h2);
}

#[test]
fn predicate_class_normalizes_bound_parameters() {
    let h1 = predicate_class("WHERE balance > 1000", "accounts");
    let h2 = predicate_class("WHERE balance > 9999", "accounts");
    assert_eq!(
        h1, h2,
        "different numeric literals should normalize to the same predicate class"
    );
}

#[test]
fn dispatch_inblock_multi_shard_rejects() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let result = dispatch_calvin_or_fast(
            &tasks,
            CrossShardTxnMode::Strict,
            TransactionState::InBlock,
            None,
            None,
            TenantId::new(1),
        )
        .await;
        assert!(
            matches!(result, Err(Error::CrossShardInExplicitTransaction)),
            "expected CrossShardInExplicitTransaction, got {result:?}"
        );
    });
}

#[test]
fn dispatch_no_inbox_returns_sequencer_unavailable() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let result = dispatch_calvin_or_fast(
            &tasks,
            CrossShardTxnMode::Strict,
            TransactionState::Idle,
            None,
            None,
            TenantId::new(1),
        )
        .await;
        assert!(
            matches!(result, Err(Error::SequencerUnavailable)),
            "expected SequencerUnavailable, got {result:?}"
        );
    });
}

#[test]
fn dispatch_best_effort_skips_inbox() {
    use nodedb_cluster::calvin::sequencer::config::SequencerConfig;
    use nodedb_cluster::calvin::sequencer::inbox::new_inbox;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (inbox, mut rx) = new_inbox(16, &SequencerConfig::default());
        let tasks = vec![doc_insert_task(3), doc_insert_task(7)];
        let result = dispatch_calvin_or_fast(
            &tasks,
            CrossShardTxnMode::BestEffortNonAtomic,
            TransactionState::Idle,
            Some(&inbox),
            None,
            TenantId::new(1),
        )
        .await;
        assert!(
            matches!(result, Ok(DispatchOutcome::BestEffortNonAtomic)),
            "expected BestEffortNonAtomic, got {result:?}"
        );
        let mut out = Vec::new();
        let drained = rx.drain_into_capped(&mut out, 10, usize::MAX);
        assert_eq!(drained, 0, "inbox should not have been called");
    });
}
