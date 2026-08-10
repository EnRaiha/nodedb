// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for the newly-widened write variants in `is_write_plan`.
//!
//! Each test below corresponds to a variant that `plan_vshard`
//! (`control/cluster/calvin/scheduler/driver/core/routing.rs`) confirms is
//! `Vshards`-routable, and that `required_permission`
//! (`control/security/identity/plan_permission.rs`) confirms is
//! `Permission::Write` — the two pieces of evidence this widening rests on.

use super::*;
use nodedb_physical::physical_plan::{BatchEdge, CrdtOp, DocumentOp, GraphOp, KvOp, VectorOp};
use nodedb_types::{PayloadIndexKind, Surrogate, VectorQuantization, VectorStorageDtype};

// ── CrdtOp ──────────────────────────────────────────────────────────────────

#[test]
fn is_write_plan_true_for_crdt_apply() {
    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: "docs".to_owned(),
        document_id: "id1".to_owned(),
        delta: Vec::new(),
        peer_id: 0,
        mutation_id: 0,
        surrogate: Surrogate::ZERO,
        provenance: None,
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });
    assert!(is_write_plan(&plan), "CrdtOp::Apply must be a write");
}

#[test]
fn is_write_plan_true_for_crdt_set_constraints() {
    let plan = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
        collection: "docs".to_owned(),
        constraint_version: 1,
        constraints: Vec::new(),
    });
    assert!(
        is_write_plan(&plan),
        "CrdtOp::SetConstraints must be a write"
    );
}

#[test]
fn is_write_plan_true_for_crdt_drop_constraints() {
    let plan = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
        collection: "docs".to_owned(),
        constraint_version: 1,
    });
    assert!(
        is_write_plan(&plan),
        "CrdtOp::DropConstraints must be a write"
    );
}

#[test]
fn is_write_plan_true_for_crdt_restore_to_version() {
    let plan = PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
        collection: "docs".to_owned(),
        document_id: "id1".to_owned(),
        target_version_json: "{}".to_owned(),
        surrogate: Surrogate::new(1),
    });
    assert!(
        is_write_plan(&plan),
        "CrdtOp::RestoreToVersion must be a write"
    );
}

#[test]
fn is_write_plan_true_for_crdt_import_snapshot() {
    let plan = PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
        tenant_id: 1,
        collection: "docs".to_owned(),
        bytes: Vec::new(),
    });
    assert!(
        is_write_plan(&plan),
        "CrdtOp::ImportSnapshot must be a write"
    );
}

// ── DocumentOp ────────────────────────────────────────────────────────────

#[test]
fn is_write_plan_true_for_document_truncate() {
    let plan = PhysicalPlan::Document(DocumentOp::Truncate {
        collection: "docs".to_owned(),
        restart_identity: false,
        resolved_sum_targets: Vec::new(),
    });
    assert!(is_write_plan(&plan), "DocumentOp::Truncate must be a write");
}

// ── KvOp ─────────────────────────────────────────────────────────────────

#[test]
fn is_write_plan_true_for_kv_truncate() {
    let plan = PhysicalPlan::Kv(KvOp::Truncate {
        collection: "cache".to_owned(),
    });
    assert!(is_write_plan(&plan), "KvOp::Truncate must be a write");
}

#[test]
fn is_write_plan_true_for_kv_expire() {
    let plan = PhysicalPlan::Kv(KvOp::Expire {
        collection: "cache".to_owned(),
        key: b"k".to_vec(),
        ttl_ms: 1000,
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::Expire must be a write");
}

#[test]
fn is_write_plan_true_for_kv_persist() {
    let plan = PhysicalPlan::Kv(KvOp::Persist {
        collection: "cache".to_owned(),
        key: b"k".to_vec(),
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::Persist must be a write");
}

#[test]
fn is_write_plan_true_for_kv_field_set() {
    let plan = PhysicalPlan::Kv(KvOp::FieldSet {
        collection: "cache".to_owned(),
        key: b"k".to_vec(),
        updates: vec![("field".to_owned(), b"v".to_vec())],
        surrogate: Surrogate::new(1),
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::FieldSet must be a write");
}

#[test]
fn is_write_plan_true_for_kv_incr() {
    let plan = PhysicalPlan::Kv(KvOp::Incr {
        collection: "cache".to_owned(),
        key: b"k".to_vec(),
        delta: 1,
        ttl_ms: 0,
        surrogate: Surrogate::new(1),
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::Incr must be a write");
}

#[test]
fn is_write_plan_true_for_kv_incr_float() {
    let plan = PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: "cache".to_owned(),
        key: b"k".to_vec(),
        delta: 1.5,
        surrogate: Surrogate::new(1),
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::IncrFloat must be a write");
}

#[test]
fn is_write_plan_true_for_kv_cas() {
    let plan = PhysicalPlan::Kv(KvOp::Cas {
        collection: "cache".to_owned(),
        key: b"k".to_vec(),
        expected: b"old".to_vec(),
        new_value: b"new".to_vec(),
        surrogate: Surrogate::new(1),
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::Cas must be a write");
}

#[test]
fn is_write_plan_true_for_kv_get_set() {
    let plan = PhysicalPlan::Kv(KvOp::GetSet {
        collection: "cache".to_owned(),
        key: b"k".to_vec(),
        new_value: b"new".to_vec(),
        surrogate: Surrogate::new(1),
        rls_filters: Vec::new(),
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::GetSet must be a write");
}

#[test]
fn is_write_plan_true_for_kv_transfer() {
    let plan = PhysicalPlan::Kv(KvOp::Transfer {
        collection: "accounts".to_owned(),
        source_key: b"a".to_vec(),
        dest_key: b"b".to_vec(),
        field: "balance".to_owned(),
        amount: 10.0,
        debit_surrogate: Surrogate::new(1),
        credit_surrogate: Surrogate::new(2),
        rls_write_check: Vec::new(),
    });
    assert!(is_write_plan(&plan), "KvOp::Transfer must be a write");
}

// ── VectorOp ─────────────────────────────────────────────────────────────

#[test]
fn is_write_plan_true_for_vector_multi_vector_delete() {
    let plan = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
        collection: "vecs".to_owned(),
        field_name: "colbert".to_owned(),
        document_surrogate: Surrogate::new(2),
    });
    assert!(
        is_write_plan(&plan),
        "VectorOp::MultiVectorDelete must be a write"
    );
}

#[test]
fn is_write_plan_true_for_vector_direct_upsert() {
    let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
        collection: "vecs".to_owned(),
        field: "emb".to_owned(),
        surrogate: Surrogate::new(3),
        vector: vec![0.5, 0.6],
        payload: vec![1, 2, 3],
        quantization: VectorQuantization::None,
        storage_dtype: VectorStorageDtype::F32,
        payload_indexes: vec![("tenant_id".to_owned(), PayloadIndexKind::Equality)],
        returning: None,
        rls_filters: Vec::new(),
    });
    assert!(
        is_write_plan(&plan),
        "VectorOp::DirectUpsert must be a write"
    );
}

// ── GraphOp ──────────────────────────────────────────────────────────────

#[test]
fn is_write_plan_true_for_graph_edge_put_batch() {
    let edge = BatchEdge {
        collection: "follows".to_owned(),
        src_id: "a".to_owned(),
        label: "knows".to_owned(),
        dst_id: "b".to_owned(),
        src_surrogate: Surrogate::new(1),
        dst_surrogate: Surrogate::new(2),
    };
    let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: vec![edge] });
    assert!(
        is_write_plan(&plan),
        "GraphOp::EdgePutBatch must be a write"
    );
}

#[test]
fn is_write_plan_true_for_graph_edge_delete_batch() {
    let edge = BatchEdge {
        collection: "follows".to_owned(),
        src_id: "a".to_owned(),
        label: "knows".to_owned(),
        dst_id: "b".to_owned(),
        src_surrogate: Surrogate::new(1),
        dst_surrogate: Surrogate::new(2),
    };
    let plan = PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges: vec![edge] });
    assert!(
        is_write_plan(&plan),
        "GraphOp::EdgeDeleteBatch must be a write"
    );
}

#[test]
fn is_write_plan_true_for_graph_set_node_labels() {
    let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
        node_id: "n1".to_owned(),
        labels: vec!["Person".to_owned()],
    });
    assert!(
        is_write_plan(&plan),
        "GraphOp::SetNodeLabels must be a write"
    );
}

#[test]
fn is_write_plan_true_for_graph_remove_node_labels() {
    let plan = PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
        node_id: "n1".to_owned(),
        labels: vec!["Person".to_owned()],
    });
    assert!(
        is_write_plan(&plan),
        "GraphOp::RemoveNodeLabels must be a write"
    );
}

// ── is_derived_side_effect ──────────────────────────────────────────────────

fn balance_delta_plan() -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
        collection: "accounts".to_owned(),
        document_id: "0000010f".to_owned(),
        surrogate: Surrogate::new(271),
        column: "balance".to_owned(),
        delta: "25".to_owned(),
        join_column: "account_id".to_owned(),
        join_value: "acc-1".to_owned(),
    })
}

fn point_insert_plan() -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: "entries".to_owned(),
        document_id: "e1".to_owned(),
        value: Vec::new(),
        if_absent: false,
        surrogate: Surrogate::new(11),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

/// A balance write is a DERIVED side effect, exactly as an implicit graph edge
/// is.
///
/// Both are appended by the Control Plane alongside a statement they do not
/// appear in, and neither may own that statement's applied response: the
/// `CommandComplete` tag is shaped from ONE deposited response, so a derived
/// participant winning the deposit hands the user's `INSERT` a count that
/// belongs to a row the statement never named.
#[test]
fn a_balance_write_is_a_derived_side_effect() {
    assert!(is_derived_side_effect(&balance_delta_plan()));
}

/// The user's own write is not, so it remains the participant that deposits.
/// Without this the fix would leave every cross-shard statement with no applied
/// response at all.
#[test]
fn the_users_own_write_is_not_a_derived_side_effect() {
    assert!(!is_derived_side_effect(&point_insert_plan()));
}

/// Derived does NOT mean "not a write": both still enter Calvin's write-key
/// set, which is what makes the pair multi-shard and commit atomically. The two
/// classifications answer different questions and must not be collapsed.
#[test]
fn a_derived_side_effect_is_still_a_calvin_write() {
    assert!(is_write_plan(&balance_delta_plan()));
    assert!(is_write_plan(&point_insert_plan()));
}
