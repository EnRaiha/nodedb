// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::DocumentOp;

#[test]
fn replicated_entry_roundtrip() {
    let entry = ReplicatedEntry::new(
        1,
        42,
        ReplicatedWrite::PointPut {
            collection: "users".into(),
            document_id: "u1".into(),
            value: b"alice".to_vec(),
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
        } => {
            assert_eq!(collection, "users");
            assert_eq!(document_id, "u1");
            assert_eq!(value, b"alice");
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
        },
        ReplicatedWrite::PointDelete {
            collection: "c".into(),
            document_id: "d".into(),
        },
        ReplicatedWrite::VectorInsert {
            collection: "v".into(),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: "embedding".into(),
            pk_bytes: Some(b"doc-1".to_vec()),
        },
        ReplicatedWrite::CrdtApply {
            collection: "c".into(),
            document_id: "d".into(),
            delta: vec![0xAB],
            peer_id: 7,
        },
        ReplicatedWrite::EdgePut {
            collection: "col".into(),
            src_id: "a".into(),
            label: "knows".into(),
            dst_id: "b".into(),
            properties: vec![],
        },
        ReplicatedWrite::EdgeDelete {
            collection: "col".into(),
            src_id: "a".into(),
            label: "knows".into(),
            dst_id: "b".into(),
        },
        ReplicatedWrite::ArrayOp {
            array: "genome".into(),
            op_bytes: vec![0xde, 0xad],
            schema_hlc_bytes: [0u8; 18],
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
