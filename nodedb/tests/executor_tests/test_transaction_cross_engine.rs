// SPDX-License-Identifier: BUSL-1.1

//! Transaction batches spanning more than one engine.
//!
//! A graph edge or a vector node written inside a batch must commit and
//! roll back with the document writes beside it — a surviving edge or
//! HNSW node after a failed batch is state no read path can account for.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, MetaOp, PhysicalPlan, VectorOp};

use crate::helpers::*;

#[test]
fn transaction_edge_put_committed() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert source and destination nodes.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "nodes".into(),
            document_id: "alice".into(),
            value: b"{\"name\":\"alice\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "nodes".into(),
            document_id: "bob".into(),
            value: b"{\"name\":\"bob\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Transaction: insert doc + edge.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            txn_id: None,
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "nodes".into(),
                    document_id: "carol".into(),
                    value: b"{\"name\":\"carol\"}".to_vec(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
                PhysicalPlan::Graph(GraphOp::EdgePut {
                    collection: "col".into(),
                    src_id: "alice".into(),
                    label: "KNOWS".into(),
                    dst_id: "bob".into(),
                    properties: Vec::new(),
                    src_surrogate: nodedb_types::Surrogate::ZERO,
                    dst_surrogate: nodedb_types::Surrogate::ZERO,
                }),
            ],
        }),
    );
    assert_eq!(resp.status, Status::Ok);

    // Verify edge exists via Neighbors.
    let n = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "alice".into(),
            edge_label: Some("KNOWS".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(n.status, Status::Ok);
    assert!(!n.payload.is_empty());
}

#[test]
fn transaction_edge_put_rolled_back_on_failure() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert nodes.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "nodes".into(),
            document_id: "alice".into(),
            value: b"{\"name\":\"alice\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "nodes".into(),
            document_id: "bob".into(),
            value: b"{\"name\":\"bob\"}".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Set up vector index with dim=3.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection: "emb".into(),
            field_name: String::new(),
            dim: 3,
            m: 16,
            ef_construction: 200,
            metric: "cosine".into(),
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: "emb".into(),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        }),
    );

    // Transaction: edge put + vector with wrong dimension (triggers rollback).
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            txn_id: None,
            plans: vec![
                PhysicalPlan::Graph(GraphOp::EdgePut {
                    collection: "col".into(),
                    src_id: "alice".into(),
                    label: "KNOWS".into(),
                    dst_id: "bob".into(),
                    properties: Vec::new(),
                    src_surrogate: nodedb_types::Surrogate::ZERO,
                    dst_surrogate: nodedb_types::Surrogate::ZERO,
                }),
                // Dimension mismatch: index is dim=3 but vector has 2 elements.
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: "emb".into(),
                    vector: vec![1.0, 2.0],
                    dim: 3,
                    field_name: String::new(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: None,
                    provenance: None,
                }),
            ],
        }),
    );
    assert_eq!(resp.status, Status::Error);

    // Verify edge was rolled back: neighbors should be empty.
    let n = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "alice".into(),
            edge_label: Some("KNOWS".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(n.status, Status::Ok);
    // Payload should be empty array (no neighbors).
    let payload = &*n.payload;
    // Deserialize: either empty msgpack array or empty JSON array.
    // Empty result = msgpack empty array [0x90] or very short payload.
    assert!(
        payload.len() <= 3,
        "edge should have been rolled back, but payload len: {}",
        payload.len()
    );
}

#[test]
fn transaction_mixed_doc_edge_vector_rollback() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-insert nodes.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "nodes".into(),
            document_id: "n1".into(),
            value: b"original_n1".to_vec(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"n1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "nodes".into(),
            document_id: "n2".into(),
            value: b"original_n2".to_vec(),
            surrogate: nodedb_types::Surrogate::new(2),
            pk_bytes: b"n2".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );

    // Set up vector index.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection: "vec".into(),
            field_name: String::new(),
            dim: 3,
            m: 16,
            ef_construction: 200,
            metric: "cosine".into(),
            index_type: String::new(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }),
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: "vec".into(),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        }),
    );

    // Transaction: doc update + edge put + vector insert (wrong dim) — all should rollback.
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            txn_id: None,
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "nodes".into(),
                    document_id: "n1".into(),
                    value: b"modified_n1".to_vec(),
                    surrogate: nodedb_types::Surrogate::new(1),
                    pk_bytes: b"n1".to_vec(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
                PhysicalPlan::Graph(GraphOp::EdgePut {
                    collection: "col".into(),
                    src_id: "n1".into(),
                    label: "LINKED".into(),
                    dst_id: "n2".into(),
                    properties: Vec::new(),
                    src_surrogate: nodedb_types::Surrogate::ZERO,
                    dst_surrogate: nodedb_types::Surrogate::ZERO,
                }),
                // Fail: dim mismatch.
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: "vec".into(),
                    vector: vec![1.0],
                    dim: 3,
                    field_name: String::new(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: None,
                    provenance: None,
                }),
            ],
        }),
    );
    assert_eq!(resp.status, Status::Error);

    // Document should be rolled back to original.
    let r = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "nodes".into(),
            document_id: "n1".into(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: b"n1".to_vec(),
        }),
    );
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original_n1");

    // Edge should be rolled back (no neighbors).
    let n = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "n1".into(),
            edge_label: Some("LINKED".into()),
            direction: nodedb::engine::graph::edge_store::Direction::Out,
            rls_filters: Vec::new(),
            collection: None,
        }),
    );
    assert_eq!(n.status, Status::Ok);
    // Empty result = msgpack empty array [0x90] or very short payload.
    assert!(n.payload.len() <= 3, "edge should have been rolled back");
}
