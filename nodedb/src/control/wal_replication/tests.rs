// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, CrdtOp, DocumentOp, GraphOp, ResolvedSumTarget,
    ReturningColumns, ReturningItem, ReturningSpec, SpatialOp, TextOp, TimeseriesOp, UpdateValue,
    VectorOp,
};
use nodedb_types::geometry::Geometry;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{PayloadIndexKind, Surrogate, VectorQuantization, VectorStorageDtype};

use super::test_support::to_replicated_entry;

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
            resolved_sum_targets: Vec::new(),
            resolved_sum_target_bindings: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
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
            resolved_sum_targets,
            resolved_sum_target_bindings,
            returning,
            rls_filters,
        } => {
            assert_eq!(collection, "users");
            assert_eq!(document_id, "u1");
            assert_eq!(value, b"alice");
            assert_eq!(surrogate, 1);
            assert!(resolved_sum_targets.is_empty());
            assert!(resolved_sum_target_bindings.is_empty());
            assert_eq!(returning, None);
            assert!(rls_filters.is_empty());
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
            resolved_sum_targets: vec![("acc-1".into(), 4242)],
            resolved_sum_target_bindings: vec![ReplicatedSumTarget {
                target_collection: "accounts".into(),
                join_value: "acc-1".into(),
                surrogate: 4242,
            }],
            returning: None,
            rls_filters: Vec::new(),
        },
        ReplicatedWrite::PointDelete {
            collection: "c".into(),
            document_id: "d".into(),
            surrogate: 1,
            resolved_sum_targets: Vec::new(),
            resolved_sum_target_bindings: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
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
            surrogate: 5,
        },
        ReplicatedWrite::CrdtApplyFenced {
            collection: "c".into(),
            document_id: "d".into(),
            delta: vec![0xAC],
            peer_id: 8,
            provenance: None,
            constraint_version_required: 1,
            expected_frontier_digest: [1; 32],
            surrogate: 6,
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

    // The apply side stamps the written collection's post-write `coll_write_lsn`
    // alongside the payload; the waiter must receive BOTH — it is the proposer's
    // only channel for a version minted on the apply path.
    assert!(tracker.complete(
        1,
        5,
        0xdead_beef,
        Ok(AppliedWrite {
            payload: b"result".to_vec(),
            write_version: Lsn::new(137),
        }),
    ));

    let result = rx.try_recv().unwrap().unwrap();
    assert_eq!(result.payload, b"result");
    assert_eq!(result.write_version, Lsn::new(137));
}

#[test]
fn propose_tracker_no_waiter_returns_false() {
    let tracker = ProposeTracker::new();
    assert!(!tracker.complete(1, 99, 0, Ok(AppliedWrite::unversioned(Vec::new()))));
}

#[test]
fn propose_tracker_key_mismatch_surfaces_retryable_leader_change() {
    let tracker = ProposeTracker::new();
    let mut rx = tracker.register(1, 5, 0xaaaa);

    // A different proposer's entry (different idempotency_key)
    // committed at the same (group_id, log_index). The waiter must
    // see RetryableLeaderChange, not the success result that belongs
    // to a different proposal.
    assert!(tracker.complete(
        1,
        5,
        0xbbbb,
        Ok(AppliedWrite::unversioned(
            b"other-proposers-payload".to_vec()
        )),
    ));

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
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    });
    assert!(
        to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .is_some()
    );

    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: "c".into(),
        document_id: "d".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    assert!(
        to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
            .expect("encode must not error")
            .is_none()
    );
}

/// The materialized-sum resolution survives the wire, on the insert shape and
/// on the predicate shape.
///
/// A replica re-executes the write and folds its own delta, so the join-key →
/// target-surrogate table and the deferral list have to arrive with it. Losing
/// either is invisible until a balance is read: an empty resolution makes the
/// fold fail on a write the leader accepted, and a lost deferral makes the
/// replica fold a delta its sibling `ApplyBalanceDelta` entry also applies.
#[test]
fn materialized_sum_resolution_roundtrips() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: "entries".into(),
        document_id: "e1".into(),
        value: vec![1, 2, 3],
        if_absent: false,
        surrogate: Surrogate::new(900),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: vec![
            ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
            ResolvedSumTarget::new("accounts", "acc-2", Surrogate::new(4243)),
            // A SECOND binding of the same source, reading the same join column
            // into a different target. Keyed on the join value alone this entry
            // could not travel at all — its value is already spoken for.
            ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
        ],
        deferred_sum_targets: vec!["accounts_elsewhere".to_string()],
    });
    let bytes = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("a document insert must replicate")
        .to_bytes();
    let (_, _, decoded, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            resolved_sum_targets,
            deferred_sum_targets,
            ..
        }) => {
            assert_eq!(
                resolved_sum_targets,
                vec![
                    ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
                    ResolvedSumTarget::new("accounts", "acc-2", Surrogate::new(4243)),
                    ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
                ],
                "a replica cannot resolve a join key itself — the table must arrive with \
                 the write, and each entry must arrive with the TARGET it was resolved \
                 against"
            );
            assert_eq!(
                deferred_sum_targets,
                vec!["accounts_elsewhere".to_string()],
                "a lost deferral is a double count, not a missing one"
            );
        }
        other => panic!("expected PointInsert, got {other:?}"),
    }

    let bulk = PhysicalPlan::Document(DocumentOp::BulkDelete {
        collection: "entries".into(),
        filters: vec![7, 7],
        returning: None,
        ollp_predicted_surrogates: None,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: vec![ResolvedSumTarget::new(
            "accounts",
            "acc-1",
            Surrogate::new(4242),
        )],
    });
    let bytes = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &bulk)
        .expect("encode must not error")
        .expect("a single-shard bulk delete must replicate")
        .to_bytes();
    let (_, _, decoded, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            resolved_sum_targets,
            ..
        }) => assert_eq!(
            resolved_sum_targets,
            vec![ResolvedSumTarget::new(
                "accounts",
                "acc-1",
                Surrogate::new(4242)
            )],
            "a replica re-derives which rows matched, never which target they credit"
        ),
        other => panic!("expected BulkDelete, got {other:?}"),
    }
}

/// A record committed BEFORE the target collection travelled on the wire still
/// decodes, and its entries still resolve.
///
/// Every node replays its own committed Raft log across an upgrade, so refusing
/// such a record would refuse to start. The superseded slot names no target, so
/// its entries are lifted UNTARGETED and match any binding by join value alone —
/// which is exactly what that record meant when the proposing node wrote it.
#[test]
fn a_record_without_target_collections_decodes_as_untargeted() {
    let entry = ReplicatedEntry::new(
        1,
        0,
        0,
        ReplicatedWrite::PointDelete {
            collection: "entries".into(),
            document_id: "e1".into(),
            surrogate: 900,
            resolved_sum_targets: vec![("acc-1".into(), 4242)],
            resolved_sum_target_bindings: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        },
    );
    let (_, _, decoded, _) = decode::from_replicated_entry(&entry.to_bytes(), None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded {
        PhysicalPlan::Document(DocumentOp::PointDelete {
            resolved_sum_targets,
            ..
        }) => {
            assert_eq!(
                resolved_sum_targets,
                vec![ResolvedSumTarget::untargeted("acc-1", Surrogate::new(4242))],
                "the older slot must still be read when the newer one is empty"
            );
            assert!(
                resolved_sum_targets[0].addresses("accounts", "acc-1"),
                "an untargeted entry answers for whichever binding asks"
            );
        }
        other => panic!("expected PointDelete, got {other:?}"),
    }
}

/// A record a current node writes carries the resolution in BOTH slots, and the
/// newer one is what a current node reads.
///
/// The older slot stays populated so a peer running an older binary parses the
/// record and behaves exactly as it does today, rather than seeing an empty
/// resolution and dropping every balance. It is derived from the newer slot, so
/// the two cannot disagree.
#[test]
fn a_current_record_carries_both_slots_and_reads_the_newer_one() {
    let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: "entries".into(),
        document_id: "e1".into(),
        surrogate: Surrogate::new(900),
        pk_bytes: b"e1".to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: vec![
            ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
            ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
        ],
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("a point delete must replicate");
    match &entry.write {
        ReplicatedWrite::PointDelete {
            resolved_sum_targets,
            resolved_sum_target_bindings,
            ..
        } => {
            assert_eq!(
                resolved_sum_target_bindings.len(),
                2,
                "both bindings must travel; the newer slot is the authoritative one"
            );
            assert_eq!(
                resolved_sum_targets,
                &vec![("acc-1".to_string(), 4242)],
                "the superseded slot keeps its one-entry-per-value shape, so an older \
                 peer reads what it has always read"
            );
        }
        other => panic!("expected PointDelete, got {other:?}"),
    }

    let (_, _, decoded, _) = decode::from_replicated_entry(&entry.to_bytes(), None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded {
        PhysicalPlan::Document(DocumentOp::PointDelete {
            resolved_sum_targets,
            ..
        }) => assert_eq!(
            resolved_sum_targets,
            vec![
                ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(4242)),
                ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(9001)),
            ],
            "the newer slot wins, so the second binding keeps its own target row"
        ),
        other => panic!("expected PointDelete, got {other:?}"),
    }
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
        .expect("encode must not error")
        .expect("VectorInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    let decoded_plan = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    let (_, _, decoded_plan, _) = decoded_plan;
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
        .expect("encode must not error")
        .expect("VectorInsert(no provenance) should produce a ReplicatedEntry");
    let bytes_none = entry_none.to_bytes();
    let (_, _, decoded_none, _) = decode::from_replicated_entry(&bytes_none, None)
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
fn crdt_apply_legacy_and_fenced_wire_compatibility() {
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
        expected_frontier_digest: Some([42; 32]),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtApply should produce a ReplicatedEntry");
    assert!(matches!(
        &entry.write,
        ReplicatedWrite::CrdtApplyFenced { .. }
    ));
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::Apply {
            provenance,
            constraint_version_required,
            expected_frontier_digest,
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
            assert_eq!(
                expected_frontier_digest,
                Some([42; 32]),
                "CrdtApply expected_frontier_digest should round-trip"
            );
        }
        other => panic!("expected CrdtApply, got {other:?}"),
    }

    // Construct the legacy enum shape directly. Its positional bytes must
    // still decode through the full replicated-entry path with no fence.
    let legacy = ReplicatedEntry::new(
        tenant.as_u64(),
        DatabaseId::DEFAULT.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::CrdtApply {
            collection: "docs".into(),
            document_id: "doc-legacy".into(),
            delta: vec![0xBE, 0xEF],
            peer_id: 8,
            provenance: None,
            constraint_version_required: 0,
            // Pre-migration wire shape: no surrogate was ever assigned on
            // this record.
            surrogate: 0,
        },
    );
    let legacy_bytes = legacy.to_bytes();
    let (_, _, decoded_legacy, _) = decode::from_replicated_entry(&legacy_bytes, None)
        .expect("legacy CrdtApply must decode")
        .expect("legacy CrdtApply must produce a plan");
    match decoded_legacy {
        PhysicalPlan::Crdt(CrdtOp::Apply {
            provenance,
            expected_frontier_digest,
            ..
        }) => {
            assert_eq!(provenance, None, "legacy provenance should remain absent");
            assert_eq!(
                expected_frontier_digest, None,
                "legacy CrdtApply must decode without a frontier fence"
            );
        }
        other => panic!("expected legacy CrdtApply, got {other:?}"),
    }
}

#[test]
fn crdt_list_insert_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Crdt(CrdtOp::ListInsert {
        collection: "notes".into(),
        document_id: "doc-1".into(),
        list_path: "blocks".into(),
        index: 2,
        fields_json: r#"{"type":"text"}"#.into(),
        surrogate: Surrogate::ZERO,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtOp::ListInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
            ..
        }) => {
            assert_eq!(collection, "notes");
            assert_eq!(document_id, "doc-1");
            assert_eq!(list_path, "blocks");
            assert_eq!(index, 2, "index must round-trip");
            assert_eq!(fields_json, r#"{"type":"text"}"#);
        }
        other => panic!("expected CrdtOp::ListInsert, got {other:?}"),
    }
}

#[test]
fn doc_batch_insert_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let documents = vec![
        ("d1".to_string(), vec![1u8, 2, 3]),
        ("d2".to_string(), vec![4u8, 5]),
        ("d3".to_string(), vec![6u8, 7, 8, 9]),
    ];
    let surrogates = vec![Surrogate::new(11), Surrogate::new(22), Surrogate::new(33)];

    let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
        collection: "docs".into(),
        documents: documents.clone(),
        surrogates: surrogates.clone(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("DocumentOp::BatchInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    // Decode with no assigner: carried surrogates fall through verbatim, so we
    // can assert every (doc_id, body) pair and every surrogate round-trips
    // exactly — none dropped or reordered.
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection,
            documents: decoded_docs,
            surrogates: decoded_surrogates,
            ..
        }) => {
            assert_eq!(collection, "docs");
            assert_eq!(
                decoded_docs, documents,
                "every (doc_id, body) pair must round-trip"
            );
            assert_eq!(
                decoded_surrogates, surrogates,
                "every surrogate must round-trip in order, none dropped"
            );
        }
        other => panic!("expected Document(BatchInsert), got {other:?}"),
    }
}

#[test]
fn doc_truncate_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Document(DocumentOp::Truncate {
        collection: "docs".into(),
        restart_identity: true,
        resolved_sum_targets: Vec::new(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("DocumentOp::Truncate should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection,
            restart_identity,
            ..
        }) => {
            assert_eq!(collection, "docs");
            assert!(restart_identity, "restart_identity must round-trip");
        }
        other => panic!("expected Document(Truncate), got {other:?}"),
    }
}

#[test]
fn kv_truncate_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    use nodedb_physical::physical_plan::KvOp;
    let plan = PhysicalPlan::Kv(KvOp::Truncate {
        collection: "kv".into(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("KvOp::Truncate should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Kv(KvOp::Truncate { collection }) => {
            assert_eq!(collection, "kv");
        }
        other => panic!("expected Kv(Truncate), got {other:?}"),
    }
}

/// `KvOp::RegisterIndex` (KV secondary-index DDL) must produce a
/// `ReplicatedEntry` and round-trip verbatim. Regression guard: this op was
/// previously classified as a non-replicated write (`kv_write` returned
/// `None`), so `CREATE INDEX` on a KV collection committed only on the leader
/// and never reached followers — replica divergence. Every field, especially
/// `backfill` and `field_position` (neither inferable at apply time), must
/// survive the wire.
#[test]
fn kv_register_index_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    use nodedb_physical::physical_plan::KvOp;
    let plan = PhysicalPlan::Kv(KvOp::RegisterIndex {
        collection: "players".into(),
        field: "name".into(),
        field_position: 2,
        backfill: true,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("KvOp::RegisterIndex must now produce a ReplicatedEntry (cluster-replicated)");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, resolved_now_ms) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    assert_eq!(resolved_now_ms, None, "index DDL carries no TTL instant");
    match decoded_plan {
        PhysicalPlan::Kv(KvOp::RegisterIndex {
            collection,
            field,
            field_position,
            backfill,
        }) => {
            assert_eq!(collection, "players");
            assert_eq!(field, "name");
            assert_eq!(field_position, 2, "field_position must round-trip");
            assert!(
                backfill,
                "backfill must round-trip (not inferable at apply)"
            );
        }
        other => panic!("expected Kv(RegisterIndex), got {other:?}"),
    }

    // The `backfill = false` shape must survive distinctly, not default to true.
    let plan = PhysicalPlan::Kv(KvOp::RegisterIndex {
        collection: "players".into(),
        field: "name".into(),
        field_position: 0,
        backfill: false,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("KvOp::RegisterIndex must produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Kv(KvOp::RegisterIndex { backfill, .. }) => {
            assert!(!backfill, "backfill = false must round-trip distinctly");
        }
        other => panic!("expected Kv(RegisterIndex), got {other:?}"),
    }
}

/// `KvOp::DropIndex` (KV secondary-index DDL) — same cluster-replication
/// regression guard as [`kv_register_index_roundtrip`].
#[test]
fn kv_drop_index_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    use nodedb_physical::physical_plan::KvOp;
    let plan = PhysicalPlan::Kv(KvOp::DropIndex {
        collection: "players".into(),
        field: "name".into(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("KvOp::DropIndex must now produce a ReplicatedEntry (cluster-replicated)");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Kv(KvOp::DropIndex { collection, field }) => {
            assert_eq!(collection, "players");
            assert_eq!(field, "name");
        }
        other => panic!("expected Kv(DropIndex), got {other:?}"),
    }
}

#[test]
fn crdt_list_delete_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Crdt(CrdtOp::ListDelete {
        collection: "notes".into(),
        document_id: "doc-1".into(),
        list_path: "blocks".into(),
        index: 5,
        surrogate: Surrogate::ZERO,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtOp::ListDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection,
            document_id,
            list_path,
            index,
            ..
        }) => {
            assert_eq!(collection, "notes");
            assert_eq!(document_id, "doc-1");
            assert_eq!(list_path, "blocks");
            assert_eq!(index, 5, "index must round-trip");
        }
        other => panic!("expected CrdtOp::ListDelete, got {other:?}"),
    }
}

/// Also proves the fix motivating `CrdtListOpWalRecord`'s design: `from_index`
/// and `to_index` are two distinct required wire fields, not one `Option<u64>`
/// slot each — they round-trip distinctly and never collapse to the same
/// value or to zero.
#[test]
fn crdt_list_move_roundtrip_distinct_indices() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Crdt(CrdtOp::ListMove {
        collection: "notes".into(),
        document_id: "doc-1".into(),
        list_path: "blocks".into(),
        from_index: 3,
        to_index: 1,
        surrogate: Surrogate::ZERO,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtOp::ListMove should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
            ..
        }) => {
            assert_eq!(collection, "notes");
            assert_eq!(document_id, "doc-1");
            assert_eq!(list_path, "blocks");
            assert_eq!(from_index, 3, "from_index must survive the round trip");
            assert_eq!(to_index, 1, "to_index must survive the round trip");
            assert_ne!(
                from_index, to_index,
                "distinct indices must never collapse to the same value"
            );
        }
        other => panic!("expected CrdtOp::ListMove, got {other:?}"),
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
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("ColumnarIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    match &decoded_entry.write {
        ReplicatedWrite::ColumnarIngest { surrogates, .. } => {
            assert_eq!(surrogates, &vec![42u32, 43u32], "surrogates must roundtrip");
        }
        other => panic!("expected ColumnarIngest, got {other:?}"),
    }
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
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
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("TimeseriesIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
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

/// Pins the bug at `decode_sync_engines::columnar_ingest`, which used to
/// hardcode `on_conflict_updates: Vec::new()` on decode: an `ON CONFLICT DO
/// UPDATE` replicated to followers as a plain overwrite, a lost update.
#[test]
fn columnar_ingest_on_conflict_updates_roundtrip() {
    let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: "metrics".into(),
        payload: b"[{}]".to_vec(),
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::Put,
        on_conflict_updates: vec![("count".into(), UpdateValue::Literal(b"5".to_vec()))],
        surrogates: vec![nodedb_types::Surrogate::new(1)],
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("ColumnarIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            on_conflict_updates,
            ..
        }) => {
            assert_eq!(
                on_conflict_updates,
                vec![("count".to_owned(), UpdateValue::Literal(b"5".to_vec()))],
                "ON CONFLICT DO UPDATE assignments must survive replication"
            );
        }
        other => panic!("expected Columnar(Insert), got {other:?}"),
    }
}

/// Pins the bug at `decode_sync_engines::columnar_ingest`, which used to
/// hardcode `intent: ColumnarInsertIntent::Insert`: an `ON CONFLICT DO
/// NOTHING` insert silently became a plain insert on replication.
#[test]
fn columnar_ingest_intent_roundtrip() {
    let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: "metrics".into(),
        payload: b"[{}]".to_vec(),
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::InsertIfAbsent,
        on_conflict_updates: Vec::new(),
        surrogates: vec![nodedb_types::Surrogate::new(1)],
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("ColumnarIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Columnar(ColumnarOp::Insert { intent, .. }) => {
            assert_eq!(
                intent,
                ColumnarInsertIntent::InsertIfAbsent,
                "ON CONFLICT DO NOTHING must not degrade to a plain insert on replication"
            );
        }
        other => panic!("expected Columnar(Insert), got {other:?}"),
    }
}

/// Pins the bug at `decode_sync_engines::columnar_ingest`, which used to
/// hardcode `format: "msgpack".to_owned()`: a native-protocol JSON payload
/// was mis-tagged and misparsed on replication.
#[test]
fn columnar_ingest_json_format_roundtrip() {
    let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: "metrics".into(),
        payload: b"[{}]".to_vec(),
        format: "json".into(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: vec![nodedb_types::Surrogate::new(1)],
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("ColumnarIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Columnar(ColumnarOp::Insert { format, .. }) => {
            assert_eq!(
                format, "json",
                "a JSON payload must not be mis-tagged as msgpack on replication"
            );
        }
        other => panic!("expected Columnar(Insert), got {other:?}"),
    }
}

/// Pins the bug at `decode_sync_engines::columnar_ingest`, which used to
/// hardcode `returning: None`: a `RETURNING` insert silently yielded no rows
/// once the write was replicated.
#[test]
fn columnar_ingest_returning_roundtrip() {
    let spec = ReturningSpec {
        columns: ReturningColumns::Named(vec![ReturningItem {
            name: "id".into(),
            alias: None,
        }]),
    };
    let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: "metrics".into(),
        payload: b"[{}]".to_vec(),
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: vec![nodedb_types::Surrogate::new(1)],
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: Some(spec.clone()),
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("ColumnarIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Columnar(ColumnarOp::Insert { returning, .. }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING request must not silently yield no rows on replication"
            );
        }
        other => panic!("expected Columnar(Insert), got {other:?}"),
    }
}

/// Pins the bug at `decode_sync_engines::columnar_ingest`, which used to
/// hardcode `rls_filters: Vec::new()`: once `returning` is fixed, an
/// unreplicated read policy would let a `RETURNING` row set exceed what a
/// `SELECT` by the same principal may see.
#[test]
fn columnar_ingest_rls_filters_roundtrip() {
    let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: "metrics".into(),
        payload: b"[{}]".to_vec(),
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: vec![nodedb_types::Surrogate::new(1)],
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: b"rls-predicate".to_vec(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("ColumnarIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Columnar(ColumnarOp::Insert { rls_filters, .. }) => {
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Columnar(Insert), got {other:?}"),
    }
}

/// Pins the same `returning` / `rls_filters` bug as the columnar tests above,
/// on the `TimeseriesOp::Ingest` sibling.
#[test]
fn timeseries_ingest_returning_and_rls_filters_roundtrip() {
    let spec = ReturningSpec {
        columns: ReturningColumns::Star,
    };
    let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: "temps".into(),
        payload: b"data".to_vec(),
        format: "ilp".into(),
        wal_lsn: None,
        surrogates: vec![nodedb_types::Surrogate::new(99)],
        provenance: None,
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("TimeseriesIngest should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING request must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Timeseries(Ingest), got {other:?}"),
    }
}

/// Pins the class of bug fixed for the document engine in
/// `entry_document.rs` / `decode/document.rs`: encode bound `returning` /
/// `rls_filters` to `_` on every document write, and decode hardcoded
/// `returning: None`, `rls_filters: Vec::new()`. Because the LEADER re-derives
/// its own executed plan from the committed Raft entry, this silently dropped
/// `RETURNING` for the ORIGINATING request, not just for followers.
#[test]
fn document_point_put_returning_and_rls_filters_roundtrip() {
    let spec = ReturningSpec {
        columns: ReturningColumns::Star,
    };
    let plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "users".into(),
        document_id: "u1".into(),
        value: b"alice".to_vec(),
        surrogate: Surrogate::new(1),
        pk_bytes: b"u1".to_vec(),
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
        resolved_sum_targets: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("PointPut should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING insert must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Document(PointPut), got {other:?}"),
    }
}

/// See `document_point_put_returning_and_rls_filters_roundtrip`; same bug,
/// `DocumentOp::PointUpdate`.
#[test]
fn document_point_update_returning_and_rls_filters_roundtrip() {
    let spec = ReturningSpec {
        columns: ReturningColumns::Named(vec![ReturningItem {
            name: "balance".into(),
            alias: None,
        }]),
    };
    let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: "accounts".into(),
        document_id: "a1".into(),
        surrogate: Surrogate::new(2),
        pk_bytes: b"a1".to_vec(),
        updates: vec![("balance".into(), UpdateValue::Literal(b"5".to_vec()))],
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("PointUpdate should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING update must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Document(PointUpdate), got {other:?}"),
    }
}

/// See `document_point_put_returning_and_rls_filters_roundtrip`; same bug,
/// `DocumentOp::PointDelete`.
#[test]
fn document_point_delete_returning_and_rls_filters_roundtrip() {
    let spec = ReturningSpec {
        columns: ReturningColumns::Star,
    };
    let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: "accounts".into(),
        document_id: "a1".into(),
        surrogate: Surrogate::new(3),
        pk_bytes: b"a1".to_vec(),
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("PointDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::PointDelete {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING delete must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Document(PointDelete), got {other:?}"),
    }
}

/// See `document_point_put_returning_and_rls_filters_roundtrip`; same bug,
/// `DocumentOp::Upsert` (`INSERT ... ON CONFLICT DO UPDATE`).
#[test]
fn document_upsert_returning_and_rls_filters_roundtrip() {
    let spec = ReturningSpec {
        columns: ReturningColumns::Star,
    };
    let plan = PhysicalPlan::Document(DocumentOp::Upsert {
        collection: "accounts".into(),
        document_id: "a1".into(),
        value: b"{}".to_vec(),
        on_conflict_updates: vec![("balance".into(), UpdateValue::Literal(b"5".to_vec()))],
        surrogate: Surrogate::new(4),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
        resolved_sum_targets: Vec::new(),
    });
    let entry = to_replicated_entry(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &plan,
    )
    .expect("encode must not error")
    .expect("Upsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::Upsert {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING upsert must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Document(Upsert), got {other:?}"),
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
        .expect("encode must not error")
        .expect("FtsIndex should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
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
        .expect("encode must not error")
        .expect("FtsDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
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
        .expect("encode must not error")
        .expect("SpatialInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
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
        .expect("encode must not error")
        .expect("SpatialDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
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
        .expect("encode must not error")
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
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
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
fn array_cell_put_roundtrips_and_carries_surrogate() {
    use crate::engine::array::wal::ArrayPutCell;
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_physical::physical_plan::ArrayOp;

    let tenant = TenantId::new(3);
    let vshard = VShardId::new(7);
    let array_id = ArrayId::new(tenant, "genome");

    // A cell carrying a real surrogate — the losslessness subject.
    let cell = ArrayPutCell {
        coord: vec![CoordValue::Int64(5), CoordValue::Int64(7)],
        attrs: vec![CellValue::Float64(42.0)],
        surrogate: Surrogate::new(9999),
        system_from_ms: 1,
        valid_from_ms: 1,
        valid_until_ms: i64::MAX,
    };
    let cells_msgpack = zerompk::to_msgpack_vec(&vec![cell]).unwrap();

    let plan = PhysicalPlan::Array(ArrayOp::Put {
        array_id: array_id.clone(),
        cells_msgpack: cells_msgpack.clone(),
        wal_lsn: 123,
        provenance: None,
    });

    // Must encode (no longer a replication gap).
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("ArrayOp::Put must encode to a ReplicatedWrite");
    let bytes = entry.to_bytes();

    // Decode with no assigner (surrogate binding is a no-op) — the cells (and
    // thus the carried surrogate) must survive verbatim, wal_lsn resets to 0.
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("ArrayCellPut must decode to a plan");
    match decoded_plan {
        PhysicalPlan::Array(ArrayOp::Put {
            array_id: decoded_id,
            cells_msgpack: decoded_cells,
            wal_lsn,
            provenance,
        }) => {
            assert_eq!(
                decoded_id, array_id,
                "array id reconstructed from header tenant + name"
            );
            assert_eq!(
                decoded_cells, cells_msgpack,
                "cells (with surrogate) carried verbatim"
            );
            assert_eq!(wal_lsn, 0, "follower allocates its own wal_lsn at apply");
            assert!(provenance.is_none());
        }
        other => panic!("expected Array(Put), got {other:?}"),
    }
}

#[test]
fn array_cell_delete_roundtrips_verbatim() {
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_physical::physical_plan::ArrayOp;

    let tenant = TenantId::new(2);
    let vshard = VShardId::new(4);
    let array_id = ArrayId::new(tenant, "genome");

    let coords = vec![vec![CoordValue::Int64(1), CoordValue::Int64(2)]];
    let coords_msgpack = zerompk::to_msgpack_vec(&coords).unwrap();

    let plan = PhysicalPlan::Array(ArrayOp::Delete {
        array_id: array_id.clone(),
        coords_msgpack: coords_msgpack.clone(),
        wal_lsn: 55,
        provenance: None,
    });

    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("ArrayOp::Delete must encode to a ReplicatedWrite");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("ArrayCellDelete must decode to a plan");
    match decoded_plan {
        PhysicalPlan::Array(ArrayOp::Delete {
            array_id: decoded_id,
            coords_msgpack: decoded_coords,
            wal_lsn,
            provenance,
        }) => {
            assert_eq!(decoded_id, array_id);
            assert_eq!(decoded_coords, coords_msgpack, "coords carried verbatim");
            assert_eq!(wal_lsn, 0);
            assert!(provenance.is_none());
        }
        other => panic!("expected Array(Delete), got {other:?}"),
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
    let (_, _, plan, _) = decode::from_replicated_entry(&bytes, None)
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
    let (_, _, plan, _) = decode::from_replicated_entry(&bytes, None)
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
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    });

    let entry = to_replicated_entry(tenant, database, vshard, &plan)
        .expect("encode must not error")
        .expect("PointPut should produce a ReplicatedEntry");
    assert_eq!(entry.database_id, database.as_u64());

    let bytes = entry.to_bytes();
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    assert_eq!(
        decoded_entry.database_id,
        database.as_u64(),
        "database_id must survive the byte round-trip"
    );

    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) => {
            assert_eq!(collection, "c");
        }
        other => panic!("expected Document(PointPut), got {other:?}"),
    }
}

// ---- Regression coverage for the six `VectorOp` writes that used to hit
// `to_replicated_entry`'s `_ => return None` catch-all and were therefore
// NEVER proposed to Raft in a cluster (autocommit, not just in-transaction —
// see `encode/vector.rs::encode` and `decode/vector.rs::decode_arm`). Each
// test drives the real production `to_replicated_entry` /
// `from_replicated_entry` functions end to end (never a hand-rolled
// encoding) and asserts the reconstructed plan equals the original,
// including the exact surrogate value carried verbatim.

#[test]
fn vector_extended_variants_all_encode_to_some() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let plans = vec![
        PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
            collection: "vecs".into(),
            surrogate: Surrogate::new(1),
            field_name: "emb".into(),
            provenance: None,
        }),
        PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: "vecs".into(),
            field_name: "sparse".into(),
            doc_id: "d1".into(),
            entries: vec![(1, 0.5)],
        }),
        PhysicalPlan::Vector(VectorOp::SparseDelete {
            collection: "vecs".into(),
            field_name: "sparse".into(),
            doc_id: "d1".into(),
        }),
        PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            collection: "vecs".into(),
            field_name: "colbert".into(),
            document_surrogate: Surrogate::new(2),
            vectors: vec![0.1, 0.2, 0.3, 0.4],
            count: 2,
            dim: 2,
        }),
        PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: "vecs".into(),
            field_name: "colbert".into(),
            document_surrogate: Surrogate::new(2),
        }),
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: "vecs".into(),
            field: "emb".into(),
            surrogate: Surrogate::new(3),
            vector: vec![0.5, 0.6],
            payload: vec![1, 2, 3],
            quantization: VectorQuantization::RaBitQ,
            storage_dtype: VectorStorageDtype::F16,
            payload_indexes: vec![("tenant_id".into(), PayloadIndexKind::Equality)],
            returning: None,
            rls_filters: Vec::new(),
        }),
    ];
    for plan in &plans {
        // On the pre-fix code this is `None` for all six — that regression
        // is exactly what this assertion catches.
        assert!(
            to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, plan)
                .expect("encode must not error")
                .is_some(),
            "expected {plan:?} to be replicated, but to_replicated_entry returned None \
             (this Vector write would execute locally and never reach Raft)"
        );
    }
}

#[test]
fn sparse_insert_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let plan = PhysicalPlan::Vector(VectorOp::SparseInsert {
        collection: "vecs".into(),
        field_name: "splade".into(),
        doc_id: "doc-42".into(),
        entries: vec![(10, 0.25), (20, 0.75)],
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("SparseInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    assert_eq!(decoded_plan, plan, "SparseInsert must round-trip exactly");
}

#[test]
fn sparse_delete_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let plan = PhysicalPlan::Vector(VectorOp::SparseDelete {
        collection: "vecs".into(),
        field_name: "splade".into(),
        doc_id: "doc-42".into(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("SparseDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    assert_eq!(decoded_plan, plan, "SparseDelete must round-trip exactly");
}

#[test]
fn multi_vector_insert_roundtrip_shares_one_surrogate() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let shared_surrogate = Surrogate::new(777);
    // Three vectors, dim 2 each, all bound to the SAME document_surrogate.
    let plan = PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
        collection: "vecs".into(),
        field_name: "colbert".into(),
        document_surrogate: shared_surrogate,
        vectors: vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
        count: 3,
        dim: 2,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("MultiVectorInsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();

    // Wire representation carries the raw u32 verbatim.
    let decoded_entry = ReplicatedEntry::from_bytes(&bytes).expect("decode failed");
    match &decoded_entry.write {
        ReplicatedWrite::MultiVectorInsert {
            document_surrogate,
            count,
            vectors,
            ..
        } => {
            assert_eq!(
                *document_surrogate, 777u32,
                "surrogate must roundtrip on wire"
            );
            assert_eq!(*count, 3);
            assert_eq!(vectors.len(), 6, "flat vector data must roundtrip in full");
        }
        other => panic!("expected MultiVectorInsert, got {other:?}"),
    }

    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match &decoded_plan {
        PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            document_surrogate,
            count,
            ..
        }) => {
            assert_eq!(
                *document_surrogate, shared_surrogate,
                "all vectors of the document must share the one carried surrogate"
            );
            assert_eq!(*count, 3);
        }
        other => panic!("expected Vector(MultiVectorInsert), got {other:?}"),
    }
    assert_eq!(
        decoded_plan, plan,
        "MultiVectorInsert must round-trip exactly"
    );
}

#[test]
fn multi_vector_delete_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let plan = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
        collection: "vecs".into(),
        field_name: "colbert".into(),
        document_surrogate: Surrogate::new(888),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("MultiVectorDelete should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match &decoded_plan {
        PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            document_surrogate, ..
        }) => {
            assert_eq!(*document_surrogate, Surrogate::new(888));
        }
        other => panic!("expected Vector(MultiVectorDelete), got {other:?}"),
    }
    assert_eq!(
        decoded_plan, plan,
        "MultiVectorDelete must round-trip exactly"
    );
}

#[test]
fn delete_by_surrogate_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let prov = SyncProvenance {
        producer_id: 4,
        epoch: 1,
        stream_id: 0,
        seq: 9,
    };
    let plan = PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
        collection: "vecs".into(),
        surrogate: Surrogate::new(555),
        field_name: "emb".into(),
        provenance: Some(prov.clone()),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("DeleteBySurrogate should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match &decoded_plan {
        PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
            surrogate,
            provenance,
            ..
        }) => {
            assert_eq!(*surrogate, Surrogate::new(555));
            assert_eq!(*provenance, Some(prov));
        }
        other => panic!("expected Vector(DeleteBySurrogate), got {other:?}"),
    }
    assert_eq!(
        decoded_plan, plan,
        "DeleteBySurrogate must round-trip exactly"
    );
}

#[test]
fn direct_upsert_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
        collection: "primary_vecs".into(),
        field: "embedding".into(),
        surrogate: Surrogate::new(999),
        vector: vec![0.1, 0.2, 0.3, 0.4],
        payload: b"\x81\xa4name\xa5alice".to_vec(),
        quantization: VectorQuantization::Bbq,
        storage_dtype: VectorStorageDtype::BF16,
        payload_indexes: vec![
            ("category".into(), PayloadIndexKind::Equality),
            ("price".into(), PayloadIndexKind::Range),
        ],
        returning: None,
        rls_filters: Vec::new(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("DirectUpsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match &decoded_plan {
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            surrogate,
            quantization,
            storage_dtype,
            payload_indexes,
            ..
        }) => {
            assert_eq!(*surrogate, Surrogate::new(999));
            assert_eq!(*quantization, VectorQuantization::Bbq);
            assert_eq!(*storage_dtype, VectorStorageDtype::BF16);
            assert_eq!(
                payload_indexes,
                &vec![
                    ("category".to_string(), PayloadIndexKind::Equality),
                    ("price".to_string(), PayloadIndexKind::Range),
                ]
            );
        }
        other => panic!("expected Vector(DirectUpsert), got {other:?}"),
    }
    assert_eq!(decoded_plan, plan, "DirectUpsert must round-trip exactly");
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
            resolved_sum_targets: Vec::new(),
            resolved_sum_target_bindings: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
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

    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) => {
            assert_eq!(collection, "c");
        }
        other => panic!("expected Document(PointPut), got {other:?}"),
    }
}

// ---- Pinned replication gaps: writes that `to_replicated_entry` classifies
// as `None` today because they have no `ReplicatedWrite` shape yet. The data
// still lands via the leader's own redb/WAL; only cross-node Raft replication
// of these ops is missing. Each assertion is a tripwire: it fails loudly if
// someone wires one of these, forcing them to update the tracking (and move
// the variant out of this list). The exhaustive `#![deny(wildcard...)]` match
// in `encode/entry*.rs` guarantees a NEW write variant cannot slip through as
// a silent `None` — these tests pin the KNOWN gaps that are `None` on purpose.

#[test]
fn known_write_gaps_are_not_replicated() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let gaps: Vec<(&str, PhysicalPlan)> = vec![
        (
            "Document::Merge",
            PhysicalPlan::Document(DocumentOp::Merge {
                target_collection: "docs".into(),
                source_collection: "staging".into(),
                source_alias: "s".into(),
                target_join_col: "id".into(),
                source_join_col: "id".into(),
                clauses: Vec::new(),
                returning: None,
                resolve_only: false,
                resolved_inserts: None,
                source_rows: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
        ),
        (
            "Document::UpdateFromJoin",
            PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                target_collection: "docs".into(),
                source_collection: "staging".into(),
                source_alias: "s".into(),
                target_join_col: "id".into(),
                source_join_col: "id".into(),
                updates: Vec::new(),
                target_filters: Vec::new(),
                returning: None,
                resolve_only: false,
                source_rows: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
            }),
        ),
        (
            "Crdt::RestoreToVersion",
            PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
                collection: "docs".into(),
                document_id: "id1".into(),
                target_version_json: "{}".into(),
                surrogate: Surrogate::new(1),
            }),
        ),
    ];

    for (name, plan) in &gaps {
        assert!(
            to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, plan)
                .expect("encode must not error")
                .is_none(),
            "{name} is a known replication gap; wiring is a tracked follow-up — \
             this test fails loudly if someone wires it so they update the tracking"
        );
    }
}

#[test]
fn crdt_set_constraints_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
        collection: "accounts".into(),
        constraint_version: 7,
        constraints: vec![vec![1, 2, 3], vec![4, 5]],
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtOp::SetConstraints should replicate as a ConstraintChange");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection,
            constraint_version,
            constraints,
        }) => {
            assert_eq!(collection, "accounts");
            assert_eq!(constraint_version, 7, "version fence must round-trip");
            assert_eq!(constraints, vec![vec![1, 2, 3], vec![4, 5]]);
        }
        other => panic!("expected CrdtOp::SetConstraints, got {other:?}"),
    }
}

#[test]
fn crdt_drop_constraints_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    let plan = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
        collection: "accounts".into(),
        constraint_version: 9,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtOp::DropConstraints should replicate as a ConstraintChange");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection,
            constraint_version,
        }) => {
            assert_eq!(collection, "accounts");
            assert_eq!(constraint_version, 9, "version fence must round-trip");
        }
        other => panic!("expected CrdtOp::DropConstraints, got {other:?}"),
    }
}

#[test]
fn representative_handled_writes_still_replicate() {
    use nodedb_physical::physical_plan::KvOp;

    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);

    // A live document write and a live KV write must still return `Some` — a
    // guard that the exhaustive-match refactor did not drop a handled arm.
    let point_put = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "docs".into(),
        document_id: "d1".into(),
        value: vec![1, 2, 3],
        surrogate: Surrogate::ZERO,
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    });
    assert!(
        to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &point_put)
            .expect("encode must not error")
            .is_some(),
        "Document::PointPut must still replicate"
    );

    let kv_put = PhysicalPlan::Kv(KvOp::Put {
        collection: "kv".into(),
        key: vec![1],
        value: vec![2],
        ttl_ms: 0,
        surrogate: Surrogate::new(7),
        returning: None,
        rls_filters: Vec::new(),
    });
    assert!(
        to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &kv_put)
            .expect("encode must not error")
            .is_some(),
        "Kv::Put must still replicate"
    );
}

/// Pins the bug: `entry_kv::kv_write` used to drop `KvOp::Put::returning` /
/// `rls_filters` via a bare `..`, and decode hardcoded `None` /
/// `Vec::new()` — a `RETURNING` write replicated via Raft silently produced
/// no rows, for the originating request too, since the leader re-derives
/// its own executed plan from the committed entry.
#[test]
fn kv_put_returning_and_rls_filters_roundtrip() {
    use nodedb_physical::physical_plan::KvOp;

    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let spec = ReturningSpec {
        columns: ReturningColumns::Star,
    };
    let plan = PhysicalPlan::Kv(KvOp::Put {
        collection: "kv".into(),
        key: b"k1".to_vec(),
        value: b"v1".to_vec(),
        ttl_ms: 0,
        surrogate: Surrogate::new(1),
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("KvOp::Put should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Kv(KvOp::Put {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING write must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Kv(Put), got {other:?}"),
    }
}

/// See `kv_put_returning_and_rls_filters_roundtrip`; same bug,
/// `KvOp::InsertOnConflictUpdate`.
#[test]
fn kv_insert_on_conflict_update_returning_and_rls_filters_roundtrip() {
    use nodedb_physical::physical_plan::KvOp;

    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let spec = ReturningSpec {
        columns: ReturningColumns::Named(vec![ReturningItem {
            name: "balance".into(),
            alias: None,
        }]),
    };
    let plan = PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
        collection: "accounts".into(),
        key: b"a1".to_vec(),
        value: b"v1".to_vec(),
        ttl_ms: 0,
        updates: vec![("balance".into(), UpdateValue::Literal(b"100".to_vec()))],
        surrogate: Surrogate::new(2),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("KvOp::InsertOnConflictUpdate should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING write must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Kv(InsertOnConflictUpdate), got {other:?}"),
    }
}

/// Pins the bug: `entry_vector::encode` used to drop
/// `VectorOp::DirectUpsert::returning` / `rls_filters` via named
/// `returning: _` / `rls_filters: _`, and decode hardcoded `None` /
/// `Vec::new()` — a `RETURNING` vector-primary upsert replicated via Raft
/// silently produced no rows.
#[test]
fn vector_direct_upsert_returning_and_rls_filters_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let spec = ReturningSpec {
        columns: ReturningColumns::Star,
    };
    let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
        collection: "vecs".into(),
        field: "emb".into(),
        surrogate: Surrogate::new(3),
        vector: vec![0.5, 0.6],
        payload: vec![1, 2, 3],
        quantization: VectorQuantization::RaBitQ,
        storage_dtype: VectorStorageDtype::F16,
        payload_indexes: vec![("tenant_id".into(), PayloadIndexKind::Equality)],
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("VectorOp::DirectUpsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING upsert must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Vector(DirectUpsert), got {other:?}"),
    }
}

/// Pins the bug: `entry_crdt::encode` used to drop
/// `CrdtOp::DocUpsert::returning` / `rls_filters` via named `returning: _`
/// / `rls_filters: _`, and decode hardcoded `None` / `Vec::new()` — a
/// `RETURNING` CRDT document upsert replicated via Raft silently produced
/// no rows.
#[test]
fn crdt_doc_upsert_returning_and_rls_filters_roundtrip() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let spec = ReturningSpec {
        columns: ReturningColumns::Star,
    };
    let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
        collection: "docs".into(),
        document_id: "d1".into(),
        fields_json: "{}".into(),
        surrogate: Surrogate::new(4),
        partial: false,
        returning: Some(spec.clone()),
        rls_filters: b"rls-predicate".to_vec(),
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtOp::DocUpsert should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, None)
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            returning,
            rls_filters,
            ..
        }) => {
            assert_eq!(
                returning,
                Some(spec),
                "a RETURNING upsert must not silently yield no rows on replication"
            );
            assert_eq!(
                rls_filters,
                b"rls-predicate".to_vec(),
                "the RETURNING read-policy filter must survive replication"
            );
        }
        other => panic!("expected Crdt(DocUpsert), got {other:?}"),
    }
}

/// Build a real (non-`Noop`) `SurrogateAssigner` over a temp `redb` catalog,
/// mirroring `surrogate::assign::core::assign_ops::tests::open_test`. Needed
/// here (rather than `assigner: None`) to prove decode BINDS the carried
/// surrogate into the catalog instead of allocating a fresh one.
fn open_test_assigner() -> (
    tempfile::TempDir,
    crate::control::surrogate::SurrogateAssigner,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let credentials = std::sync::Arc::new(
        crate::control::security::credential::CredentialStore::open(
            &dir.path().join("system.redb"),
        )
        .expect("open credential store"),
    );
    let registry = std::sync::Arc::new(std::sync::RwLock::new(
        crate::control::surrogate::SurrogateRegistry::new(),
    ));
    let wal: std::sync::Arc<dyn crate::control::surrogate::SurrogateWalAppender> =
        std::sync::Arc::new(crate::control::surrogate::NoopWalAppender);
    let assigner = crate::control::surrogate::SurrogateAssigner::new(registry, credentials, wal);
    (dir, assigner)
}

/// `CrdtOp::Apply` carries the leader-assigned surrogate on the wire and
/// decode BINDS it (first-wins) rather than re-deriving via this node's own
/// allocator. Advance the local allocator past the carried value first, so
/// a divergent-by-construction fresh `assign()` (which would return the
/// NEXT local value) is distinguishable from a correct bind (which installs
/// the carried value verbatim).
#[test]
fn crdt_apply_binds_carried_surrogate_not_fresh_allocation() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let (_dir, assigner) = open_test_assigner();

    // Burn this node's next few local allocations on unrelated keys so a
    // fresh `assign()` for "doc-1" would return something other than the
    // leader-carried value below.
    for i in 0..5 {
        assigner
            .assign(
                DatabaseId::DEFAULT,
                tenant,
                "docs",
                format!("burn-{i}").as_bytes(),
            )
            .expect("burn allocation");
    }

    let leader_surrogate = Surrogate::new(9_001);
    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: "docs".into(),
        document_id: "doc-1".into(),
        delta: vec![0xDE, 0xAD],
        peer_id: 1,
        mutation_id: 0,
        surrogate: leader_surrogate,
        provenance: None,
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &plan)
        .expect("encode must not error")
        .expect("CrdtOp::Apply should produce a ReplicatedEntry");
    let bytes = entry.to_bytes();

    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, Some(&assigner))
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::Apply { surrogate, .. }) => {
            assert_eq!(
                surrogate, leader_surrogate,
                "decode must bind the leader-carried surrogate, not allocate a fresh one"
            );
        }
        other => panic!("expected Crdt(Apply), got {other:?}"),
    }

    // The bind must be durably installed: a second decode of the SAME
    // entry (replay / retry) must return the identical value via the
    // first-wins pre-check, never re-allocate or overwrite.
    let (_, _, decoded_again, _) = decode::from_replicated_entry(&bytes, Some(&assigner))
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_again {
        PhysicalPlan::Crdt(CrdtOp::Apply { surrogate, .. }) => {
            assert_eq!(
                surrogate, leader_surrogate,
                "replaying the same entry must resolve to the same bound surrogate"
            );
        }
        other => panic!("expected Crdt(Apply), got {other:?}"),
    }

    assert_eq!(
        assigner
            .lookup(DatabaseId::DEFAULT, tenant, "docs", b"doc-1")
            .expect("catalog lookup"),
        Some(leader_surrogate),
        "the carried surrogate must be installed in the local catalog"
    );
}

/// A `CrdtApply` entry written before the surrogate field existed
/// (`surrogate: 0` on the wire, the `#[serde(default)]`) has no leader
/// value to bind. Decode must still resolve a surrogate via this node's own
/// allocator (the documented, loud, pre-migration-only fallback) rather
/// than propagating `Surrogate::ZERO` into a fresh document row.
#[test]
fn crdt_apply_legacy_no_surrogate_falls_back_to_local_assign() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let (_dir, assigner) = open_test_assigner();

    let legacy = ReplicatedEntry::new(
        tenant.as_u64(),
        DatabaseId::DEFAULT.as_u64(),
        vshard.as_u32(),
        ReplicatedWrite::CrdtApply {
            collection: "docs".into(),
            document_id: "doc-legacy-2".into(),
            delta: vec![0x01],
            peer_id: 3,
            provenance: None,
            constraint_version_required: 0,
            surrogate: 0,
        },
    );
    let bytes = legacy.to_bytes();
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&bytes, Some(&assigner))
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::Apply { surrogate, .. }) => {
            assert_ne!(
                surrogate,
                Surrogate::ZERO,
                "the legacy fallback must still allocate a real identity, not ZERO"
            );
        }
        other => panic!("expected Crdt(Apply), got {other:?}"),
    }
}

/// `CrdtOp::ListInsert` / `ListDelete` / `ListMove` carry the parent
/// document's surrogate on the wire and decode binds it via
/// `bind_or_lookup` — same identity, no fresh allocation — even though the
/// live dispatch handler does not yet consume the field.
#[test]
fn crdt_list_ops_bind_carried_surrogate_not_fresh_allocation() {
    let tenant = TenantId::new(1);
    let vshard = VShardId::new(0);
    let (_dir, assigner) = open_test_assigner();

    let parent_surrogate = Surrogate::new(4_242);
    // Establish the parent document's binding first, exactly as `DocUpsert`
    // would have when the document was created.
    assigner
        .bind(
            DatabaseId::DEFAULT,
            tenant,
            "notes",
            b"doc-1",
            parent_surrogate,
        )
        .expect("seed parent binding");

    let insert_plan = PhysicalPlan::Crdt(CrdtOp::ListInsert {
        collection: "notes".into(),
        document_id: "doc-1".into(),
        list_path: "blocks".into(),
        index: 0,
        fields_json: "{}".into(),
        surrogate: parent_surrogate,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &insert_plan)
        .expect("encode must not error")
        .expect("CrdtOp::ListInsert should produce a ReplicatedEntry");
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&entry.to_bytes(), Some(&assigner))
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::ListInsert { surrogate, .. }) => {
            assert_eq!(
                surrogate, parent_surrogate,
                "ListInsert must resolve to the parent document's existing surrogate"
            );
        }
        other => panic!("expected Crdt(ListInsert), got {other:?}"),
    }

    let delete_plan = PhysicalPlan::Crdt(CrdtOp::ListDelete {
        collection: "notes".into(),
        document_id: "doc-1".into(),
        list_path: "blocks".into(),
        index: 0,
        surrogate: parent_surrogate,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &delete_plan)
        .expect("encode must not error")
        .expect("CrdtOp::ListDelete should produce a ReplicatedEntry");
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&entry.to_bytes(), Some(&assigner))
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::ListDelete { surrogate, .. }) => {
            assert_eq!(
                surrogate, parent_surrogate,
                "ListDelete must resolve to the parent document's existing surrogate"
            );
        }
        other => panic!("expected Crdt(ListDelete), got {other:?}"),
    }

    let move_plan = PhysicalPlan::Crdt(CrdtOp::ListMove {
        collection: "notes".into(),
        document_id: "doc-1".into(),
        list_path: "blocks".into(),
        from_index: 0,
        to_index: 1,
        surrogate: parent_surrogate,
    });
    let entry = to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, &move_plan)
        .expect("encode must not error")
        .expect("CrdtOp::ListMove should produce a ReplicatedEntry");
    let (_, _, decoded_plan, _) = decode::from_replicated_entry(&entry.to_bytes(), Some(&assigner))
        .expect("from_replicated_entry error")
        .expect("from_replicated_entry returned None");
    match decoded_plan {
        PhysicalPlan::Crdt(CrdtOp::ListMove { surrogate, .. }) => {
            assert_eq!(
                surrogate, parent_surrogate,
                "ListMove must resolve to the parent document's existing surrogate"
            );
        }
        other => panic!("expected Crdt(ListMove), got {other:?}"),
    }
}
