// SPDX-License-Identifier: BUSL-1.1

//! Resolve/apply round trips for governed state-dependent KV writes.

use std::sync::Arc;

use nodedb_physical::physical_plan::{KvOp, KvResolveOutcome, KvResolvedMutation};
use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, VShardId};

const TID: u64 = 1;
const COLLECTION: &str = "kv_resolved";
const DEST_COLLECTION: &str = "kv_resolved_dest";

struct CoreHarness {
    core: CoreLoop,
    _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
    _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
    _dir: tempfile::TempDir,
}

fn make_core() -> CoreHarness {
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use nodedb_bridge::buffer::RingBuffer;

    let dir = tempfile::tempdir().expect("tempdir");
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        Arc::new(nodedb_types::OrdinalClock::new()),
    )
    .expect("open core");
    CoreHarness {
        core,
        _req_tx: req_tx,
        _resp_rx: resp_rx,
        _dir: dir,
    }
}

fn did() -> u64 {
    DatabaseId::DEFAULT.as_u64()
}

fn task() -> ExecutionTask {
    CoreLoop::replay_task(
        TenantId::new(TID),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            key: b"seed".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }),
        None,
    )
}

fn seed(core: &mut CoreLoop, collection: &str, key: &[u8], value: &[u8]) {
    core.kv_engine.put(crate::engine::kv::KvPutParams {
        database_id: did(),
        tenant_id: TID,
        collection,
        key,
        value,
        ttl_ms: 0,
        now_ms: crate::engine::kv::current_ms(),
        surrogate: Surrogate::new(1),
    });
}

fn stored(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<Vec<u8>> {
    core.kv_engine
        .get(did(), TID, collection, key, crate::engine::kv::current_ms())
}

fn i64_bytes(v: i64) -> Vec<u8> {
    zerompk::to_msgpack_vec(&v).expect("encode i64")
}

/// Run the resolve handler and decode its outcome.
fn resolve(h: &CoreHarness, op: &KvOp) -> KvResolveOutcome {
    let t = task();
    let resp = h.core.execute_kv_resolve_write(&t, did(), TID, op);
    assert_eq!(
        resp.status,
        Status::Ok,
        "resolve failed: {:?}",
        resp.error_code
    );
    zerompk::from_msgpack(resp.payload.as_bytes()).expect("decode resolve outcome")
}

/// Apply an already-resolved outcome, as a replica does.
fn apply(h: &mut CoreHarness, outcome: &KvResolveOutcome) -> crate::bridge::envelope::Response {
    let t = task();
    h.core.execute_kv_resolved_write(
        &t,
        did(),
        TID,
        &outcome.mutations,
        &outcome.response_payload,
        &RlsWriteCheck::decided_earlier_in_request(),
    )
}

fn incr_op(key: &[u8], delta: i64) -> KvOp {
    KvOp::Incr {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        key: key.to_vec(),
        delta,
        ttl_ms: 0,
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    }
}

#[test]
fn incr_resolves_to_the_computed_post_image_and_applies_it() {
    let mut h = make_core();
    seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(5));

    let outcome = resolve(&h, &incr_op(b"counter", 3));
    assert_eq!(outcome.mutations.len(), 1);
    match &outcome.mutations[0] {
        KvResolvedMutation::Put {
            collection,
            key,
            value,
            precondition,
            ..
        } => {
            assert_eq!(collection.as_str(), COLLECTION);
            assert_eq!(key.as_slice(), b"counter");
            assert_eq!(value, &i64_bytes(8), "post-image must be 5 + 3");
            assert_eq!(
                precondition.as_deref(),
                Some(i64_bytes(5).as_slice()),
                "precondition must pin the exact image the resolve read"
            );
        }
        other => panic!("expected a Put mutation, got {other:?}"),
    }

    let resp = apply(&mut h, &outcome);
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert_eq!(stored(&h.core, COLLECTION, b"counter"), Some(i64_bytes(8)));
    assert_eq!(
        resp.payload.as_bytes(),
        outcome.response_payload.as_slice(),
        "the apply must hand back the resolved payload verbatim"
    );
}

#[test]
fn drifted_precondition_retries_and_mutates_nothing() {
    let mut h = make_core();
    seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(5));

    let outcome = resolve(&h, &incr_op(b"counter", 3));

    // A concurrent write lands between the resolve and the apply.
    seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(99));

    let resp = apply(&mut h, &outcome);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired),
        "a drifted precondition must yield OllpRetryRequired"
    );
    assert_eq!(
        stored(&h.core, COLLECTION, b"counter"),
        Some(i64_bytes(99)),
        "a refused apply must not mutate anything"
    );
}

#[test]
fn drift_scan_refuses_before_the_first_mutation_applies() {
    let mut h = make_core();
    seed(&mut h.core, COLLECTION, b"a", &i64_bytes(1));
    seed(&mut h.core, COLLECTION, b"b", &i64_bytes(2));

    // Hand-built two-mutation write whose SECOND precondition is stale: the
    // first must not land either.
    let outcome = KvResolveOutcome {
        mutations: vec![
            KvResolvedMutation::Put {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                key: b"a".to_vec(),
                value: i64_bytes(10),
                ttl_ms: 0,
                expire_at_ms: 0,
                surrogate: Surrogate::new(1),
                precondition: Some(i64_bytes(1)),
            },
            KvResolvedMutation::Put {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                key: b"b".to_vec(),
                value: i64_bytes(20),
                ttl_ms: 0,
                expire_at_ms: 0,
                surrogate: Surrogate::new(2),
                precondition: Some(i64_bytes(777)),
            },
        ],
        response_payload: Vec::new(),
    };

    let resp = apply(&mut h, &outcome);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired)
    );
    assert_eq!(stored(&h.core, COLLECTION, b"a"), Some(i64_bytes(1)));
    assert_eq!(stored(&h.core, COLLECTION, b"b"), Some(i64_bytes(2)));
}

#[test]
fn absent_key_precondition_requires_the_key_to_stay_absent() {
    let mut h = make_core();

    let outcome = KvResolveOutcome {
        mutations: vec![KvResolvedMutation::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            key: b"fresh".to_vec(),
            value: i64_bytes(1),
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: Surrogate::new(1),
            precondition: None,
        }],
        response_payload: Vec::new(),
    };

    // Someone created the key first: absent-means-absent, so this drifts.
    seed(&mut h.core, COLLECTION, b"fresh", &i64_bytes(42));
    let resp = apply(&mut h, &outcome);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::OllpRetryRequired)
    );
    assert_eq!(stored(&h.core, COLLECTION, b"fresh"), Some(i64_bytes(42)));
}

#[test]
fn cas_mismatch_resolves_to_zero_mutations_and_still_replies() {
    let mut h = make_core();
    let stored_value = zerompk::to_msgpack_vec(&"actual").expect("encode");
    seed(&mut h.core, COLLECTION, b"slot", &stored_value);

    let op = KvOp::Cas {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        key: b"slot".to_vec(),
        expected: b"not-the-stored-value".to_vec(),
        new_value: b"next".to_vec(),
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    };
    let outcome = resolve(&h, &op);
    assert!(
        outcome.mutations.is_empty(),
        "a CAS that did not match writes nothing"
    );
    assert!(
        !outcome.response_payload.is_empty(),
        "it still owes the caller its failure reply"
    );

    let reported: serde_json::Value =
        nodedb_types::json_from_msgpack(&outcome.response_payload).expect("decode cas reply");
    assert_eq!(reported["success"], serde_json::json!(false));

    let resp = apply(&mut h, &outcome);
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert_eq!(stored(&h.core, COLLECTION, b"slot"), Some(stored_value));
}

#[test]
fn cas_match_resolves_to_one_put() {
    let mut h = make_core();
    seed(&mut h.core, COLLECTION, b"slot", b"actual");

    let op = KvOp::Cas {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        key: b"slot".to_vec(),
        expected: b"actual".to_vec(),
        new_value: b"next".to_vec(),
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    };
    let outcome = resolve(&h, &op);
    assert_eq!(outcome.mutations.len(), 1);

    let resp = apply(&mut h, &outcome);
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert_eq!(stored(&h.core, COLLECTION, b"slot"), Some(b"next".to_vec()));
}

#[test]
fn transfer_item_resolves_a_delete_and_a_put_across_two_collections() {
    let mut h = make_core();
    let body = nodedb_types::json_to_msgpack(&serde_json::json!({ "sword": 1 })).expect("encode");
    seed(&mut h.core, COLLECTION, b"item", &body);

    let op = KvOp::TransferItem {
        source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, DEST_COLLECTION),
        item_key: b"item".to_vec(),
        dest_key: b"owned".to_vec(),
        surrogate: Surrogate::new(7),
        source_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        dest_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    };
    let outcome = resolve(&h, &op);
    assert_eq!(outcome.mutations.len(), 2);
    match &outcome.mutations[0] {
        KvResolvedMutation::Delete {
            collection,
            key,
            precondition,
        } => {
            assert_eq!(collection.as_str(), COLLECTION);
            assert_eq!(key.as_slice(), b"item");
            assert_eq!(precondition.as_deref(), Some(body.as_slice()));
        }
        other => panic!("expected the source Delete first, got {other:?}"),
    }
    match &outcome.mutations[1] {
        KvResolvedMutation::Put {
            collection,
            key,
            value,
            precondition,
            ..
        } => {
            assert_eq!(collection.as_str(), DEST_COLLECTION);
            assert_eq!(key.as_slice(), b"owned");
            assert_eq!(value, &body);
            assert_eq!(
                precondition.as_deref(),
                None,
                "the destination key was absent, so the apply requires it to stay absent"
            );
        }
        other => panic!("expected the destination Put second, got {other:?}"),
    }

    let resp = apply(&mut h, &outcome);
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert_eq!(stored(&h.core, COLLECTION, b"item"), None);
    assert_eq!(stored(&h.core, DEST_COLLECTION, b"owned"), Some(body));
}

#[test]
fn transfer_item_on_a_missing_row_is_not_found() {
    let h = make_core();
    let op = KvOp::TransferItem {
        source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, DEST_COLLECTION),
        item_key: b"nope".to_vec(),
        dest_key: b"owned".to_vec(),
        surrogate: Surrogate::new(7),
        source_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        dest_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    };
    let t = task();
    let resp = h.core.execute_kv_resolve_write(&t, did(), TID, &op);
    assert_eq!(resp.error_code.as_deref(), Some(&ErrorCode::NotFound));
}

#[test]
fn persist_on_an_absent_key_reports_not_found_at_apply() {
    let mut h = make_core();
    let outcome = KvResolveOutcome {
        mutations: vec![KvResolvedMutation::Persist {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            key: b"gone".to_vec(),
            precondition: None,
        }],
        response_payload: Vec::new(),
    };
    let resp = apply(&mut h, &outcome);
    assert_eq!(resp.error_code.as_deref(), Some(&ErrorCode::NotFound));
}

#[test]
fn resolve_refuses_an_op_with_no_state_dependent_image() {
    let h = make_core();
    let op = KvOp::Get {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        key: b"k".to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    };
    let t = task();
    let resp = h.core.execute_kv_resolve_write(&t, did(), TID, &op);
    assert_eq!(resp.status, Status::Error);
}

#[test]
fn resolve_mutates_nothing() {
    let mut h = make_core();
    seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(5));
    let _ = resolve(&h, &incr_op(b"counter", 3));
    assert_eq!(
        stored(&h.core, COLLECTION, b"counter"),
        Some(i64_bytes(5)),
        "resolve is read-only"
    );
}
