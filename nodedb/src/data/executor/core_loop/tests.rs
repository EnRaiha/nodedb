// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Priority, Request, Status};
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
        1,
        "orders",
        "o1",
        Surrogate::new(7),
        &doc_value("a", "1"),
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
        1,
        "orders",
        "o1",
        Surrogate::new(7),
        &doc_value("a", "2"),
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
        1,
        "orders",
        "o1",
        Surrogate::new(7),
        &doc_value("a", "3"),
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
        1,
        "items",
        "i1",
        Surrogate::new(9),
        &doc_value("a", "4"),
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
    })];
    let resp = core.execute_transaction_batch(&task, 1, &plans);
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
        1,
        "orders",
        "o1",
        Surrogate::new(7),
        &doc_value("a", "1"),
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
    assert_eq!(resp.inner.error_code, Some(ErrorCode::DeadlineExceeded));
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
