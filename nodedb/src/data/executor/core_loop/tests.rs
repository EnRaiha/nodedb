// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Priority, Request, Status};
use crate::data::executor::handlers::point::put::PointPutExec;
use crate::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{DocumentOp, MetaOp};
use nodedb_types::{Surrogate, SurrogateBitmap};
use std::time::{Duration, Instant};

fn make_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

pub fn make_core_with_dir(
    dir: &std::path::Path,
) -> (CoreLoop, Producer<BridgeRequest>, Consumer<BridgeResponse>) {
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir,
        std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
    )
    .unwrap();
    (core, req_tx, resp_rx)
}

/// A minimal `ExecutionTask` (DEFAULT database/tenant, vShard 0, no WAL LSN)
/// for handler unit tests that only read `request.database_id`. The carried
/// plan is inert — edge/point handlers take their parameters directly.
pub fn make_default_task() -> crate::data::executor::task::ExecutionTask {
    crate::data::executor::task::ExecutionTask::new(make_request(PhysicalPlan::Document(
        DocumentOp::PointGet {
            collection: "x".into(),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        },
    )))
}

fn make_request(plan: PhysicalPlan) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Admitted,
    }
}

#[test]
fn empty_tick_processes_nothing() {
    let (mut core, _, _, _dir) = make_core();
    assert_eq!(core.tick(), 0);
}

// ── Per-core last-write-LSN version index ──────────────────────────────────

use crate::data::executor::core_loop::write_index::{CollKey, KeyRepr, WriteKey};
use crate::data::executor::task::ExecutionTask;

/// A msgpack-tagged `{k: v}` document body.
fn doc_value(k: &str, v: &str) -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    obj.insert(k.to_string(), nodedb_types::Value::String(v.into()));
    zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap()
}

/// An `ExecutionTask` carrying a known WAL LSN, tenant 1 / database DEFAULT.
fn wal_task(lsn: u64) -> ExecutionTask {
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: "x".into(),
        document_id: "y".into(),
        surrogate: Surrogate::ZERO,
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    ExecutionTask::with_wal_lsn(make_request(plan), Some(Lsn::new(lsn)))
}

fn surrogate_key(collection: &str, surrogate: u32) -> WriteKey {
    WriteKey {
        db: DatabaseId::DEFAULT,
        tenant: TenantId::new(1),
        collection: Box::from(collection),
        key: KeyRepr::Surrogate(surrogate),
    }
}

fn coll_key(collection: &str) -> CollKey {
    CollKey {
        db: DatabaseId::DEFAULT,
        tenant: TenantId::new(1),
        collection: Box::from(collection),
    }
}

#[test]
fn point_put_records_write_version_and_advances_watermark() {
    let (mut core, _, _, _dir) = make_core();

    let task = wal_task(10);
    let resp = core.execute_point_put(
        &task,
        PointPutExec {
            tid: 1,
            collection: "orders",
            document_id: "o1",
            surrogate: Surrogate::new(7),
            value: &doc_value("a", "1"),
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(resp.status, Status::Ok);

    assert_eq!(
        core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
        Some(Lsn::new(10))
    );
    assert_eq!(
        core.write_index.collection_write_lsn(&coll_key("orders")),
        Some(Lsn::new(10))
    );
    assert_eq!(core.watermark, Lsn::new(10));

    // Second write to the same key with a larger LSN overwrites monotonically.
    let task = wal_task(20);
    core.execute_point_put(
        &task,
        PointPutExec {
            tid: 1,
            collection: "orders",
            document_id: "o1",
            surrogate: Surrogate::new(7),
            value: &doc_value("a", "2"),
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(
        core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
        Some(Lsn::new(20))
    );
    assert_eq!(
        core.write_index.collection_write_lsn(&coll_key("orders")),
        Some(Lsn::new(20))
    );
    assert_eq!(core.watermark, Lsn::new(20));

    // A lower LSN never regresses an existing entry or the watermark.
    let task = wal_task(15);
    core.execute_point_put(
        &task,
        PointPutExec {
            tid: 1,
            collection: "orders",
            document_id: "o1",
            surrogate: Surrogate::new(7),
            value: &doc_value("a", "3"),
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(
        core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
        Some(Lsn::new(20))
    );
    assert_eq!(core.watermark, Lsn::new(20));

    // A second collection tracks its own max independently.
    let task = wal_task(30);
    core.execute_point_put(
        &task,
        PointPutExec {
            tid: 1,
            collection: "items",
            document_id: "i1",
            surrogate: Surrogate::new(9),
            value: &doc_value("a", "4"),
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(
        core.write_index.collection_write_lsn(&coll_key("items")),
        Some(Lsn::new(30))
    );
    assert_eq!(
        core.write_index.collection_write_lsn(&coll_key("orders")),
        Some(Lsn::new(20))
    );
    assert_eq!(core.watermark, Lsn::new(30));
}

#[test]
fn kv_put_records_kvkey_version() {
    let (mut core, _, _, _dir) = make_core();
    let task = wal_task(42);
    let resp = core.execute_kv_put(
        &task,
        crate::data::executor::handlers::kv::crud::KvWriteParams {
            did: DatabaseId::DEFAULT.as_u64(),
            tid: 1,
            collection: "kv",
            key: b"k1".as_slice(),
            value: b"v1".as_slice(),
            ttl_ms: 0,
            surrogate: Surrogate::new(3),
            returning: None,
            rls_filters: &[],
        },
    );
    assert_eq!(resp.status, Status::Ok);

    let wk = WriteKey {
        db: DatabaseId::DEFAULT,
        tenant: TenantId::new(1),
        collection: Box::from("kv"),
        key: KeyRepr::KvKey(Box::from(b"k1".as_slice())),
    };
    assert_eq!(core.write_index.key_write_lsn(&wk), Some(Lsn::new(42)));
    assert_eq!(
        core.write_index.collection_write_lsn(&coll_key("kv")),
        Some(Lsn::new(42))
    );
    assert_eq!(core.watermark, Lsn::new(42));
}

#[test]
fn edge_put_records_edge_version() {
    let (mut core, _, _, _dir) = make_core();
    let task = wal_task(50);
    let resp = core.execute_edge_put(
        &task,
        crate::data::executor::handlers::graph::EdgePutParams {
            tid: 1,
            collection: "graph",
            src_id: "a",
            label: "KNOWS",
            dst_id: "b",
            properties: &[],
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        },
    );
    assert_eq!(resp.status, Status::Ok);

    let wk = WriteKey {
        db: DatabaseId::DEFAULT,
        tenant: TenantId::new(1),
        collection: Box::from("graph"),
        key: KeyRepr::Edge {
            src: Box::from("a"),
            label: Box::from("KNOWS"),
            dst: Box::from("b"),
        },
    };
    assert_eq!(core.write_index.key_write_lsn(&wk), Some(Lsn::new(50)));
    assert_eq!(core.watermark, Lsn::new(50));
}

#[test]
fn transaction_batch_records_sub_plan_versions() {
    let (mut core, _, _, _dir) = make_core();
    let task = wal_task(60);
    let plans = vec![PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "batch".into(),
        document_id: "d1".into(),
        value: doc_value("a", "1"),
        surrogate: Surrogate::new(11),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })];
    let resp = core.execute_transaction_batch(&task, 1, &plans, &[], None);
    assert_eq!(resp.status, Status::Ok);

    assert_eq!(
        core.write_index.key_write_lsn(&surrogate_key("batch", 11)),
        Some(Lsn::new(60))
    );
    assert_eq!(
        core.write_index.collection_write_lsn(&coll_key("batch")),
        Some(Lsn::new(60))
    );
    assert_eq!(core.watermark, Lsn::new(60));
}

#[test]
fn no_wal_lsn_records_nothing() {
    let (mut core, _, _, _dir) = make_core();
    // Task without a WAL LSN — the version index is skipped, not advanced.
    let task = ExecutionTask::new(make_request(PhysicalPlan::Document(DocumentOp::PointGet {
        collection: "x".into(),
        document_id: "y".into(),
        surrogate: Surrogate::ZERO,
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    })));
    core.execute_point_put(
        &task,
        PointPutExec {
            tid: 1,
            collection: "orders",
            document_id: "o1",
            surrogate: Surrogate::new(7),
            value: &doc_value("a", "1"),
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(
        core.write_index.key_write_lsn(&surrogate_key("orders", 7)),
        None
    );
    assert_eq!(core.watermark, Lsn::ZERO);
}

#[test]
fn horizon_gc_evicts_stale_keys_keeps_recent_and_collection() {
    let (mut core, _, _, _dir) = make_core();
    let db = DatabaseId::DEFAULT;
    let tenant = TenantId::new(1);

    // A stale per-key entry, then a recent write that drives the watermark far
    // past the retain window.
    core.note_write_lsn(db, tenant, "c", Some(KeyRepr::Surrogate(1)), Lsn::new(10));
    core.note_write_lsn(
        db,
        tenant,
        "c",
        Some(KeyRepr::Surrogate(2)),
        Lsn::new(1_000_000),
    );
    assert_eq!(core.watermark, Lsn::new(1_000_000));

    core.gc_write_index();

    // Stale key evicted; recent key retained; collection floor survives GC.
    assert_eq!(core.write_index.key_write_lsn(&surrogate_key("c", 1)), None);
    assert_eq!(
        core.write_index.key_write_lsn(&surrogate_key("c", 2)),
        Some(Lsn::new(1_000_000))
    );
    assert_eq!(
        core.write_index.collection_write_lsn(&coll_key("c")),
        Some(Lsn::new(1_000_000))
    );
}

#[test]
fn expired_task_returns_deadline_exceeded() {
    let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();
    req_tx
        .try_push(BridgeRequest {
            inner: Request {
                deadline: Instant::now() - Duration::from_secs(1),
                ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: Vec::new(),
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                }))
            },
        })
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(resp.inner.status, Status::Error);
    assert_eq!(
        resp.inner.error_code.as_deref(),
        Some(&ErrorCode::DeadlineExceeded)
    );
}

#[test]
fn watermark_in_response() {
    let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();
    core.advance_watermark(Lsn::new(99));
    core.sparse.put(0, 1, "x", "y", b"data").unwrap();
    req_tx
        .try_push(BridgeRequest {
            inner: make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "x".into(),
                document_id: "y".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            })),
        })
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(resp.inner.watermark_lsn, Lsn::new(99));
}

#[test]
fn cancel_removes_pending_task() {
    let (mut core, mut req_tx, _resp_rx, _dir) = make_core();
    req_tx
        .try_push(BridgeRequest {
            inner: Request {
                request_id: RequestId::new(10),
                deadline: Instant::now() + Duration::from_secs(60),
                ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                    surrogate: nodedb_types::Surrogate::ZERO,
                    pk_bytes: Vec::new(),
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                }))
            },
        })
        .unwrap();
    core.drain_requests();
    assert_eq!(core.pending_count(), 1);

    req_tx
        .try_push(BridgeRequest {
            inner: Request {
                request_id: RequestId::new(99),
                priority: Priority::Critical,
                consistency: ReadConsistency::Eventual,
                ..make_request(PhysicalPlan::Meta(MetaOp::Cancel {
                    target_request_id: RequestId::new(10),
                }))
            },
        })
        .unwrap();
    // Cancel runs at Critical priority and is drained before the Normal-priority
    // target. The cancel removes id=10 from the queue, so only the Cancel itself
    // is processed in this tick (no response is emitted for the cancelled task).
    assert_eq!(core.tick(), 1);
    assert_eq!(core.pending_count(), 0);
}

#[test]
fn point_put_stores_schemaless_docs_as_canonical_msgpack_maps() {
    let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();

    let mut obj = std::collections::HashMap::new();
    obj.insert(
        "user_id".to_string(),
        nodedb_types::Value::String("u1".into()),
    );
    obj.insert(
        "item".to_string(),
        nodedb_types::Value::String("book".into()),
    );
    let tagged = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();

    req_tx
        .try_push(BridgeRequest {
            inner: make_request(PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "orders".into(),
                document_id: "o1".into(),
                value: tagged,
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            })),
        })
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(resp.inner.status, Status::Ok);

    // The handler hex-encodes the surrogate to compute the substrate
    // row key; this fixture used `Surrogate::ZERO`, which renders to
    // "00000000".
    let stored = core
        .sparse
        .get(0, 1, "orders", "00000000")
        .unwrap()
        .unwrap();
    assert!(nodedb_query::msgpack_scan::map_header(&stored, 0).is_some());
    assert!(nodedb_query::msgpack_scan::extract_field(&stored, 0, "user_id").is_some());
    assert!(nodedb_query::msgpack_scan::extract_field(&stored, 0, "item").is_some());
}

#[test]
fn scan_with_prefilter_returns_only_bitmap_members() {
    let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();

    // Insert three documents with surrogates 1, 2, and 3.
    let surrogates: &[(u32, &str)] = &[(1, "alpha"), (2, "beta"), (3, "gamma")];
    for (sur_val, name) in surrogates {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "name".to_string(),
            nodedb_types::Value::String((*name).into()),
        );
        let bytes = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "things".into(),
                    document_id: format!("doc_{sur_val}"),
                    value: bytes,
                    surrogate: Surrogate::new(*sur_val),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                })),
            })
            .unwrap();
        core.tick();
        let _ = resp_rx.try_pop().unwrap();
    }

    // Build a prefilter containing only surrogates 1 and 3 (not 2).
    let prefilter = SurrogateBitmap::from_iter([Surrogate::new(1), Surrogate::new(3)]);

    // Issue a scan with the prefilter.
    req_tx
        .try_push(BridgeRequest {
            inner: make_request(PhysicalPlan::Document(DocumentOp::Scan {
                collection: "things".into(),
                limit: 100,
                offset: 0,
                sort_keys: Vec::new(),
                filters: Vec::new(),
                distinct: false,
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: Some(prefilter),
            })),
        })
        .unwrap();
    core.tick();

    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(resp.inner.status, Status::Ok, "scan should succeed");

    // Decode the response payload: array of {id, data} maps.
    // Use msgpack_scan to iterate the outer array and extract each row's "id" field.
    let payload = resp.inner.payload.to_vec();
    let (count, mut pos) = nodedb_query::msgpack_scan::array_header(&payload, 0)
        .expect("payload should be a msgpack array");

    assert_eq!(count, 2, "expected exactly 2 rows after prefilter");

    let mut returned_ids = std::collections::HashSet::new();
    for _ in 0..count {
        // Each element is a 2-entry fixmap {"id": "...", "data": ...}.
        if let Some((id_start, _)) = nodedb_query::msgpack_scan::extract_field(&payload, pos, "id")
            && let Some(id_str) = nodedb_query::msgpack_scan::read_str(&payload, id_start)
        {
            returned_ids.insert(id_str.to_string());
        }
        pos = nodedb_query::msgpack_scan::skip_value(&payload, pos)
            .expect("should be able to skip map entry");
    }

    assert!(
        returned_ids.contains("00000001"),
        "surrogate 1 should be in results"
    );
    assert!(
        returned_ids.contains("00000003"),
        "surrogate 3 should be in results"
    );
    assert!(
        !returned_ids.contains("00000002"),
        "surrogate 2 (not in prefilter) must not appear"
    );
}

// ── Read-set validation against the write-version index ────────────────────

use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};

/// An `ExecutionTask` homing to `vshard_id`, carrying no WAL LSN.
fn task_with_vshard(vshard_id: VShardId) -> ExecutionTask {
    ExecutionTask::new(Request {
        vshard_id,
        ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "x".into(),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }))
    })
}

/// An `ExecutionTask` carrying WAL LSN `lsn` and homing to `vshard_id`.
fn wal_task_with_vshard(lsn: u64, vshard_id: VShardId) -> ExecutionTask {
    ExecutionTask::with_wal_lsn(
        Request {
            vshard_id,
            ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "x".into(),
                document_id: "y".into(),
                surrogate: Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }))
        },
        Some(Lsn::new(lsn)),
    )
}

/// The vShard `collection` homes to in the default database — mirrors the
/// homing `read_set_still_current` filters entries by.
fn local_vshard(collection: &str) -> VShardId {
    VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection)
}

/// Some vShard other than `than`, for exercising the cross-shard filter.
fn other_vshard(than: VShardId) -> VShardId {
    VShardId::new((than.as_u32() + 1) % VShardId::COUNT)
}

fn point_entry(collection: &str, surrogate: u32, read_lsn: u64) -> VersionedReadEntry {
    VersionedReadEntry {
        engine: EngineTag::Document,
        collection: collection.to_string(),
        key: ReadKeyIdent::Point(KeyRepr::Surrogate(surrogate)),
        read_lsn: Lsn::new(read_lsn),
    }
}

fn predicate_entry(collection: &str, read_lsn: u64) -> VersionedReadEntry {
    VersionedReadEntry {
        engine: EngineTag::Document,
        collection: collection.to_string(),
        key: ReadKeyIdent::Predicate,
        read_lsn: Lsn::new(read_lsn),
    }
}

#[test]
fn stale_point_read_is_detected_as_not_current() {
    let (mut core, _, _, _dir) = make_core();
    core.note_write_lsn(
        DatabaseId::DEFAULT,
        TenantId::new(1),
        "orders",
        Some(KeyRepr::Surrogate(7)),
        Lsn::new(20),
    );

    let task = task_with_vshard(local_vshard("orders"));
    let reads = vec![point_entry("orders", 7, 10)];
    assert!(!core.read_set_still_current(&task, 1, &reads));
}

#[test]
fn fresh_point_read_is_still_current() {
    let (mut core, _, _, _dir) = make_core();
    core.note_write_lsn(
        DatabaseId::DEFAULT,
        TenantId::new(1),
        "orders",
        Some(KeyRepr::Surrogate(7)),
        Lsn::new(20),
    );

    let task = task_with_vshard(local_vshard("orders"));
    assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 20)]));
    assert!(core.read_set_still_current(&task, 1, &[point_entry("orders", 7, 30)]));
}

#[test]
fn read_entry_homing_to_a_different_vshard_is_filtered_out() {
    let (mut core, _, _, _dir) = make_core();
    core.note_write_lsn(
        DatabaseId::DEFAULT,
        TenantId::new(1),
        "orders",
        Some(KeyRepr::Surrogate(7)),
        Lsn::new(20),
    );

    let local = local_vshard("orders");
    let remote_task = task_with_vshard(other_vshard(local));
    // Would conflict (read_lsn 10 < write_lsn 20) if this shard owned the
    // entry's collection; it homes elsewhere, so it is filtered out of this
    // shard's slice and the vacuous (empty-after-filter) result is `true`.
    let reads = vec![point_entry("orders", 7, 10)];
    assert!(core.read_set_still_current(&remote_task, 1, &reads));
}

#[test]
fn stale_predicate_read_is_detected_as_not_current() {
    let (mut core, _, _, _dir) = make_core();
    core.note_write_lsn(
        DatabaseId::DEFAULT,
        TenantId::new(1),
        "orders",
        None,
        Lsn::new(20),
    );

    let task = task_with_vshard(local_vshard("orders"));
    let reads = vec![predicate_entry("orders", 10)];
    assert!(!core.read_set_still_current(&task, 1, &reads));
}

#[test]
fn fresh_predicate_read_is_still_current() {
    let (mut core, _, _, _dir) = make_core();
    core.note_write_lsn(
        DatabaseId::DEFAULT,
        TenantId::new(1),
        "orders",
        None,
        Lsn::new(20),
    );

    let task = task_with_vshard(local_vshard("orders"));
    let reads = vec![predicate_entry("orders", 20)];
    assert!(core.read_set_still_current(&task, 1, &reads));
}

#[test]
fn empty_read_set_is_vacuously_current() {
    let (core, _, _, _dir) = make_core();
    let task = task_with_vshard(VShardId::new(0));
    assert!(core.read_set_still_current(&task, 1, &[]));
}

#[test]
fn conflicting_read_set_is_flagged_invalid_but_batch_still_applies() {
    let (mut core, _, _, _dir) = make_core();
    let vshard = local_vshard("orders");

    // First batch: a write to key 7 in "orders", recording its version at
    // LSN 10 (this is the same chokepoint a Calvin apply funnels through).
    let write_task = wal_task_with_vshard(10, vshard);
    let write_plans = vec![PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "orders".into(),
        document_id: "o7".into(),
        value: doc_value("a", "1"),
        surrogate: Surrogate::new(7),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })];
    let write_resp = core.execute_transaction_batch(&write_task, 1, &write_plans, &[], None);
    assert_eq!(write_resp.status, Status::Ok);
    assert_eq!(
        write_resp.read_set_valid,
        Some(true),
        "empty read-set is vacuously current"
    );

    // Second batch carries a synthetic read-set observing key 7 BEFORE the
    // write above (read_lsn = 5 < the recorded write's LSN 10), alongside its
    // own unrelated write. Proves: (a) the first batch's write really was
    // recorded into the version index (without it this would false-report
    // valid), and (b) an invalid read-set does not block the batch's own
    // apply (non-enforcing).
    let second_task = wal_task_with_vshard(20, vshard);
    let second_plans = vec![PhysicalPlan::Document(DocumentOp::PointPut {
        collection: "orders".into(),
        document_id: "o8".into(),
        value: doc_value("a", "2"),
        surrogate: Surrogate::new(8),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })];
    let stale_reads = vec![point_entry("orders", 7, 5)];
    let second_resp =
        core.execute_transaction_batch(&second_task, 1, &second_plans, &stale_reads, None);

    assert_eq!(
        second_resp.status,
        Status::Ok,
        "apply proceeds regardless of the read-set validation outcome"
    );
    assert_eq!(
        second_resp.read_set_valid,
        Some(false),
        "stale read against the recorded write must be detected as no longer current"
    );

    // The second batch's own write still landed despite the invalid read-set.
    assert_eq!(
        core.write_index.key_write_lsn(&surrogate_key("orders", 8)),
        Some(Lsn::new(20))
    );
}
