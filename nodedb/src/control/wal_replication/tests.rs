// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
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
    ];

    for write in writes {
        let entry = ReplicatedEntry::new(1, 0, write);
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
    assert!(to_replicated_entry(tenant, vshard, &plan).is_some());

    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: "c".into(),
        document_id: "d".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    assert!(to_replicated_entry(tenant, vshard, &plan).is_none());
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
    let entry_none = to_replicated_entry(tenant, vshard, &plan_none)
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
    });
    let entry = to_replicated_entry(tenant, vshard, &plan)
        .expect("CrdtApply should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::Apply { provenance, .. }) => {
            assert_eq!(
                provenance,
                Some(prov.clone()),
                "CrdtApply provenance should round-trip"
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
    });
    let entry_none = to_replicated_entry(tenant, vshard, &plan_none)
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
    let entry = to_replicated_entry(tenant, vshard, &plan)
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
