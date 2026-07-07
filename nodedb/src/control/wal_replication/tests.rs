// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, CrdtOp, DocumentOp, GraphOp, SpatialOp, TextOp, TimeseriesOp,
    VectorOp,
};
use nodedb_types::geometry::Geometry;
use nodedb_types::sync::wire::SyncProvenance;

#[test]
fn replicated_entry_roundtrip() {
    let entry = ReplicatedEntry::new(
        1,
        0,
        42,
        ReplicatedWrite::PointPut {
            collection: "users".into(),
            document_id: "u1".into(),
            value: b"alice".to_vec(),
            surrogate: 1,
        },
    );
    let original_key = entry.idempotency_key;
    assert_ne!(original_key, 0, "idempotency_key must be non-zero");

    let bytes = entry.to_bytes();
    let decoded = ReplicatedEntry::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.tenant_id, 1);
    assert_eq!(decoded.vshard_id, 42);
    assert_eq!(
        decoded.idempotency_key, original_key,
        "idempotency_key roundtrip"
    );
    match decoded.write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
            surrogate,
        } => {
            assert_eq!(collection, "users");
            assert_eq!(document_id, "u1");
            assert_eq!(value, b"alice");
            assert_eq!(surrogate, 1);
        }
        other => panic!("expected PointPut, got {other:?}"),
    }
}

#[test]
fn constraint_change_roundtrip() {
    let entry = ReplicatedEntry::new(
        7,
        0,
        3,
        ReplicatedWrite::ConstraintChange {
            collection: "orders".into(),
            op: ConstraintChangeOp::Set,
            constraint_version: 9,
            constraints: vec![vec![1, 2, 3], vec![4, 5, 6]],
        },
    );
    let original_key = entry.idempotency_key;

    let bytes = entry.to_bytes();
    let decoded = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    assert_eq!(decoded.tenant_id, 7);
    assert_eq!(decoded.vshard_id, 3);
    assert_eq!(decoded.idempotency_key, original_key);
    match decoded.write {
        ReplicatedWrite::ConstraintChange {
            collection,
            op,
            constraint_version,
            constraints,
        } => {
            assert_eq!(collection, "orders");
            assert_eq!(op, ConstraintChangeOp::Set);
            assert_eq!(constraint_version, 9);
            assert_eq!(constraints, vec![vec![1u8, 2, 3], vec![4u8, 5, 6]]);
        }
        other => panic!("expected ConstraintChange, got {other:?}"),
    }
}

#[test]
fn constraint_change_encoding_is_deterministic() {
    let write = ReplicatedWrite::ConstraintChange {
        collection: "orders".into(),
        op: ConstraintChangeOp::Drop,
        constraint_version: 4,
        constraints: vec![vec![1, 2, 3], vec![4, 5, 6]],
    };
    let a = zerompk::to_msgpack_vec(&write).expect("encode a failed");
    let b = zerompk::to_msgpack_vec(&write).expect("encode b failed");
    assert_eq!(
        a, b,
        "encoding the same ConstraintChange must be byte-identical"
    );
}

#[test]
fn all_write_variants_serialize() {
    let writes = vec![
        ReplicatedWrite::PointPut {
            collection: "c".into(),
            document_id: "d".into(),
            value: vec![1, 2, 3],
            surrogate: 1,
        },
        ReplicatedWrite::PointDelete {
            collection: "c".into(),
            document_id: "d".into(),
            surrogate: 1,
        },
        ReplicatedWrite::VectorInsert {
            collection: "v".into(),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: "embedding".into(),
            surrogate: 7,
            pk_bytes: Some(b"doc-1".to_vec()),
            provenance: None,
        },
        ReplicatedWrite::CrdtApply {
            collection: "c".into(),
            document_id: "d".into(),
            delta: vec![0xAB],
            peer_id: 7,
            provenance: None,
            constraint_version_required: 0,
        },
        ReplicatedWrite::EdgePut {
            collection: "col".into(),
            src_id: "a".into(),
            label: "knows".into(),
            dst_id: "b".into(),
            properties: vec![],
            src_surrogate: 10,
            dst_surrogate: 20,
        },
        ReplicatedWrite::EdgeDelete {
            collection: "col".into(),
            src_id: "a".into(),
            label: "knows".into(),
            dst_id: "b".into(),
            src_surrogate: 10,
            dst_surrogate: 20,
        },
        ReplicatedWrite::ArrayOp {
            array: "genome".into(),
            op_bytes: vec![0xde, 0xad],
            schema_hlc_bytes: [0u8; 18],
            provenance: None,
        },
        ReplicatedWrite::ArraySchema {
            array: "genome".into(),
            snapshot_payload: vec![0xbe, 0xef],
            schema_hlc_bytes: [1u8; 18],
        },
        ReplicatedWrite::ConstraintChange {
            collection: "orders".into(),
            op: ConstraintChangeOp::Set,
            constraint_version: 1,
            constraints: vec![vec![1, 2, 3]],
        },
    ];

    for write in writes {
        let entry = ReplicatedEntry::new(1, 0, 0, write);
        let bytes = entry.to_bytes();
        let decoded = ReplicatedEntry::from_bytes(&bytes);
        assert!(decoded.is_some(), "failed to roundtrip: {entry:?}");
    }
}

#[test]
fn propose_tracker_register_and_complete() {
    let tracker = ProposeTracker::new();
    let mut rx = tracker.register(1, 5, 0xdead_beef);

    assert!(tracker.complete(1, 5, 0xdead_beef, Ok(b"result".to_vec())));

    let result = rx.try_recv().unwrap();
    assert_eq!(result.unwrap(), b"result");
}

#[test]
fn propose_tracker_no_waiter_returns_false() {
    let tracker = ProposeTracker::new();
    assert!(!tracker.complete(1, 99, 0, Ok(vec![])));
}

#[test]
fn propose_tracker_key_mismatch_surfaces_retryable_leader_change() {
    let tracker = ProposeTracker::new();
    let mut rx = tracker.register(1, 5, 0xaaaa);

    // A different proposer's entry (different idempotency_key)
    // committed at the same (group_id, log_index). The waiter must
    // see RetryableLeaderChange, not the success result that belongs
    // to a different proposal.
    assert!(tracker.complete(1, 5, 0xbbbb, Ok(b"other-proposers-payload".to_vec())));

    let result = rx.try_recv().unwrap();
    match result {
        Err(crate::Error::RetryableLeaderChange {
            group_id,
            log_index,
        }) => {
            assert_eq!(group_id, 1);
            assert_eq!(log_index, 5);
        }
        other => panic!("expected RetryableLeaderChange, got {other:?}"),
    }
}

#[test]
fn propose_tracker_zero_applied_key_passes_through_explicit_error() {
    // Empty raft entries (leader-change no-ops) carry no idempotency
    // key. The applier passes `applied_key = 0` together with an
    // explicit `RetryableLeaderChange` result; the tracker must
    // forward that result rather than treating the zero key as a
    // mismatch (which would produce the same error but mask the
    // distinction in logs).
    let tracker = ProposeTracker::new();
    let mut rx = tracker.register(1, 5, 0xaaaa);
    assert!(tracker.complete(
        1,
        5,
        0,
        Err(crate::Error::RetryableLeaderChange {
            group_id: 1,
            log_index: 5,
        }),
    ));
    let result = rx.try_recv().unwrap();
    assert!(matches!(
        result,
        Err(crate::Error::RetryableLeaderChange { .. })
    ));
}

#[test]
fn to_replicated_entry_writes_only() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "c".into(),
        document_id: "d".into(),
        value: vec![],
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    });
    assert!(to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan).is_some());

    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: "c".into(),
        document_id: "d".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    assert!(to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan).is_none());
}

#[test]
fn vector_insert_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 42,
        epoch: 7,
        stream_id: 3,
        seq: 100,
    };

    // With provenance.
    let plan = PhysicalPlan::Vector(VectorOp::Insert {
        collection: "vecs".into(),
        vector: vec![0.1, 0.2, 0.3],
        dim: 3,
        field_name: "emb".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: None,
        provenance: Some(prov.clone()),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("VectorInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    let decoded_plan = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    let (_, _, decoded_plan) = decoded_plan;
    match decoded_plan {
        PhysicalPlan::Vector(VectorOp::Insert { provenance, .. }) => {
            assert_eq!(
                provenance,
                Some(prov.clone()),
                "VectorInsert provenance should round-trip"
            );
        }
        other => panic!("expected VectorInsert, got {other:?}"),
    }
    drop(decoded_entry);

    // Without provenance.
    let plan_none = PhysicalPlan::Vector(VectorOp::Insert {
        collection: "vecs".into(),
        vector: vec![0.1, 0.2, 0.3],
        dim: 3,
        field_name: "emb".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: None,
        provenance: None,
    });
    let entry_none = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan_none)
        .expect("VectorInsert(no provenance) should produce a ReplicatedEntry");
    let bytes_none = entry_none.to_bytes();
    let (_, _, decoded_none) = decode::from_replicated_entry(&bytes_none, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_none {
        PhysicalPlan::Vector(VectorOp::Insert { provenance, .. }) => {
            assert_eq!(
                provenance, None,
                "None provenance should round-trip as None"
            );
        }
        other => panic!("expected VectorInsert, got {other:?}"),
    }
}

#[test]
fn crdt_apply_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 99,
        epoch: 2,
        stream_id: 1,
        seq: 55,
    };

    // With provenance.
    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: "docs".into(),
        document_id: "doc-1".into(),
        delta: vec![0xDE, 0xAD],
        peer_id: 7,
        mutation_id: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
        provenance: Some(prov.clone()),
        constraint_version_required: 42,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("CrdtApply should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::Apply {
            provenance,
            constraint_version_required,
            ..
        }) => {
            assert_eq!(
                provenance,
                Some(prov.clone()),
                "CrdtApply provenance should round-trip"
            );
            assert_eq!(
                constraint_version_required, 42,
                "CrdtApply constraint_version_required should round-trip"
            );
        }
        other => panic!("expected CrdtApply, got {other:?}"),
    }

    // Without provenance.
    let plan_none = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: "docs".into(),
        document_id: "doc-1".into(),
        delta: vec![0xDE, 0xAD],
        peer_id: 7,
        mutation_id: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
        provenance: None,
        constraint_version_required: 0,
    });
    let entry_none = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan_none)
        .expect("CrdtApply(no provenance) should produce a ReplicatedEntry");
    let bytes_none = entry_none.to_bytes();
    let (_, _, decoded_none) = decode::from_replicated_entry(&bytes_none, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_none {
        PhysicalPlan::Crdt(CrdtOp::Apply { provenance, .. }) => {
            assert_eq!(
                provenance, None,
                "None provenance should round-trip as None"
            );
        }
        other => panic!("expected CrdtApply, got {other:?}"),
    }
}

#[test]
fn columnar_ingest_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 11,
        epoch: 3,
        stream_id: 2,
        seq: 77,
    };

    let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: "metrics".into(),
        payload: b"[{}]".to_vec(),
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: vec![
            nodedb_types::Surrogate::new(42),
            nodedb_types::Surrogate::new(43),
        ],
        schema_bytes: Vec::new(),
        provenance: Some(prov.clone()),
        wal_lsn: None,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("ColumnarIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    match &decoded_entry.write {
        ReplicatedWrite::ColumnarIngest { surrogates, .. } => {
            assert_eq!(surrogates, &vec![42u32, 43u32], "surrogates must roundtrip");
        }
        other => panic!("expected ColumnarIngest, got {other:?}"),
    }
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            surrogates,
            provenance,
            wal_lsn,
            intent,
            on_conflict_updates,
            ..
        }) => {
            assert_eq!(
                surrogates,
                vec![
                    nodedb_types::Surrogate::new(42),
                    nodedb_types::Surrogate::new(43)
                ]
            );
            assert_eq!(provenance, Some(prov));
            assert_eq!(wal_lsn, None, "wal_lsn must be None on decode");
            assert_eq!(intent, ColumnarInsertIntent::Insert);
            assert!(on_conflict_updates.is_empty());
        }
        other => panic!("expected Columnar(Insert), got {other:?}"),
    }
}

#[test]
fn timeseries_ingest_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 5,
        epoch: 1,
        stream_id: 0,
        seq: 200,
    };

    let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: "temps".into(),
        payload: b"data".to_vec(),
        format: "ilp".into(),
        wal_lsn: None,
        surrogates: vec![nodedb_types::Surrogate::new(99)],
        provenance: Some(prov.clone()),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("TimeseriesIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            surrogates,
            provenance,
            wal_lsn,
            ..
        }) => {
            assert_eq!(surrogates, vec![nodedb_types::Surrogate::new(99)]);
            assert_eq!(provenance, Some(prov));
            assert_eq!(wal_lsn, None, "wal_lsn must be None on decode");
        }
        other => panic!("expected Timeseries(Ingest), got {other:?}"),
    }
}

#[test]
fn fts_index_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 7,
        epoch: 4,
        stream_id: 1,
        seq: 33,
    };

    let plan = PhysicalPlan::Text(TextOp::FtsIndexDoc {
        collection: "articles".into(),
        surrogate: nodedb_types::Surrogate::new(500),
        text: "hello world".into(),
        provenance: Some(prov.clone()),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("FtsIndex should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Text(TextOp::FtsIndexDoc {
            surrogate,
            provenance,
            ..
        }) => {
            assert_eq!(surrogate, nodedb_types::Surrogate::new(500));
            assert_eq!(provenance, Some(prov));
        }
        other => panic!("expected Text(FtsIndexDoc), got {other:?}"),
    }
}

#[test]
fn fts_delete_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 8,
        epoch: 2,
        stream_id: 0,
        seq: 10,
    };

    let plan = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
        collection: "articles".into(),
        surrogate: nodedb_types::Surrogate::new(501),
        provenance: Some(prov.clone()),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("FtsDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            surrogate,
            provenance,
            ..
        }) => {
            assert_eq!(surrogate, nodedb_types::Surrogate::new(501));
            assert_eq!(provenance, Some(prov));
        }
        other => panic!("expected Text(FtsDeleteDoc), got {other:?}"),
    }
}

#[test]
fn spatial_insert_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 3,
        epoch: 9,
        stream_id: 2,
        seq: 44,
    };
    let geometry = Geometry::Point {
        coordinates: [-73.985, 40.758],
    };

    let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
        collection: "places".into(),
        field: "location".into(),
        surrogate: nodedb_types::Surrogate::new(700),
        geometry: geometry.clone(),
        provenance: Some(prov.clone()),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("SpatialInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Spatial(SpatialOp::Insert {
            surrogate,
            geometry: decoded_geom,
            provenance,
            ..
        }) => {
            assert_eq!(surrogate, nodedb_types::Surrogate::new(700));
            assert_eq!(decoded_geom, geometry);
            assert_eq!(provenance, Some(prov));
        }
        other => panic!("expected Spatial(Insert), got {other:?}"),
    }
}

#[test]
fn spatial_delete_provenance_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 2,
        epoch: 1,
        stream_id: 0,
        seq: 5,
    };

    let plan = PhysicalPlan::Spatial(SpatialOp::Delete {
        collection: "places".into(),
        field: "location".into(),
        surrogate: nodedb_types::Surrogate::new(701),
        provenance: Some(prov.clone()),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("SpatialDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Spatial(SpatialOp::Delete {
            surrogate,
            provenance,
            ..
        }) => {
            assert_eq!(surrogate, nodedb_types::Surrogate::new(701));
            assert_eq!(provenance, Some(prov));
        }
        other => panic!("expected Spatial(Delete), got {other:?}"),
    }
}

#[test]
fn edge_put_surrogates_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: "graph".into(),
        src_id: "alice".into(),
        label: "knows".into(),
        dst_id: "bob".into(),
        properties: vec![],
        src_surrogate: nodedb_types::Surrogate::new(11),
        dst_surrogate: nodedb_types::Surrogate::new(22),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("EdgePut should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();

    // Verify wire representation carries the raw u32 values.
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    match &decoded_entry.write {
        ReplicatedWrite::EdgePut {
            src_surrogate,
            dst_surrogate,
            ..
        } => {
            assert_eq!(
                *src_surrogate, 11u32,
                "src_surrogate must roundtrip on wire"
            );
            assert_eq!(
                *dst_surrogate, 22u32,
                "dst_surrogate must roundtrip on wire"
            );
        }
        other => panic!("expected EdgePut, got {other:?}"),
    }

    // Verify the decoded PhysicalPlan uses the carried (authoritative) surrogates.
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Graph(GraphOp::EdgePut {
            src_surrogate,
            dst_surrogate,
            ..
        }) => {
            assert_eq!(
                src_surrogate,
                nodedb_types::Surrogate::new(11),
                "src_surrogate must survive encode→decode"
            );
            assert_eq!(
                dst_surrogate,
                nodedb_types::Surrogate::new(22),
                "dst_surrogate must survive encode→decode"
            );
        }
        other => panic!("expected Graph(EdgePut), got {other:?}"),
    }
}

#[test]
fn constraint_change_set_decodes_to_set_constraints() {
    // Decode layer keeps constraint blobs opaque, so raw bytes are sufficient
    // and avoid coupling this test to the constraint wire layout.
    let entry = ReplicatedEntry::new(
        1,
        0,
        0,
        ReplicatedWrite::ConstraintChange {
            collection: "users".into(),
            op: ConstraintChangeOp::Set,
            constraint_version: 12,
            constraints: vec![vec![1, 2, 3]],
        },
    );
    let bytes = entry.to_bytes();
    let (_, _, plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("ConstraintChange(Set) must decode to a plan");
    match plan {
        PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection,
            constraint_version,
            constraints,
        }) => {
            assert_eq!(collection, "users");
            assert_eq!(constraint_version, 12);
            assert_eq!(constraints.len(), 1);
            assert_eq!(constraints[0], vec![1, 2, 3]);
        }
        other => panic!("expected Crdt(SetConstraints), got {other:?}"),
    }
}

#[test]
fn constraint_change_drop_decodes_to_drop_constraints() {
    let entry = ReplicatedEntry::new(
        1,
        0,
        0,
        ReplicatedWrite::ConstraintChange {
            collection: "users".into(),
            op: ConstraintChangeOp::Drop,
            constraint_version: 8,
            constraints: Vec::new(),
        },
    );
    let bytes = entry.to_bytes();
    let (_, _, plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("ConstraintChange(Drop) must decode to a plan");
    match plan {
        PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection,
            constraint_version,
        }) => {
            assert_eq!(collection, "users");
            assert_eq!(constraint_version, 8);
        }
        other => panic!("expected Crdt(DropConstraints), got {other:?}"),
    }
}

#[test]
fn non_default_database_id_roundtrips_through_encode_decode() {
    let tenant = TenantId::new(1);
    let database = DatabaseId::new(1024);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "c".into(),
        document_id: "d".into(),
        value: vec![1, 2, 3],
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    });

    let entry = to_replicated_entry(tenant, database, vshard, &plan)
        .expect("PointPut should produce a ReplicatedEntry");
    assert_eq!(entry.database_id, database.as_u64());

    let bytes = entry.to_bytes();
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    assert_eq!(
        decoded_entry.database_id,
        database.as_u64(),
        "database_id must survive the byte round-trip"
    );

    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) => {
            assert_eq!(collection, "c");
        }
        other => panic!("expected Document(PointPut), got {other:?}"),
    }
}

#[test]
fn pre_database_id_entry_decodes_to_default_database() {
    // Simulates a Raft log entry proposed by a not-yet-upgraded leader: the
    // pre-`database_id` 4-field shape. `ReplicatedEntry::from_bytes` must
    // recognize the resulting `ArrayLengthMismatch` and fall back to
    // `LegacyReplicatedEntry`, defaulting `database_id` to `0`
    // (`DatabaseId::DEFAULT`), rather than failing to decode.
    let legacy = super::legacy_entry::LegacyReplicatedEntry {
        tenant_id: 1,
        vshard_id: 0,
        idempotency_key: 0xabcd,
        write: ReplicatedWrite::PointPut {
            collection: "c".into(),
            document_id: "d".into(),
            value: vec![9, 9, 9],
            surrogate: 1,
        },
    };
    let bytes = zerompk::to_msgpack_vec(&legacy).expect("legacy entry encode failed");

    let decoded = ReplicatedEntry::from_bytes(&bytes).expect("legacy entry must decode");
    assert_eq!(decoded.tenant_id, 1);
    assert_eq!(decoded.vshard_id, 0);
    assert_eq!(decoded.idempotency_key, 0xabcd);
    assert_eq!(
        decoded.database_id, 0,
        "old-leader entries lacking database_id must decode to DatabaseId::DEFAULT (0)"
    );

    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) => {
            assert_eq!(collection, "c");
        }
        other => panic!("expected Document(PointPut), got {other:?}"),
    }
}
