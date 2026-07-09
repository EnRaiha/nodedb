// SPDX-License-Identifier: BUSL-1.1

//! Staged Calvin apply on the Data Plane: `MetaOp::CalvinExecuteStatic`
//! VALIDATES + STAGES a transaction's write plans into the commit-pending
//! buffer WITHOUT mutating base, returning the local commit vote on
//! `read_set_valid`. A subsequent `MetaOp::CalvinFlush` replays the staged
//! plans to base (making the write visible), or `MetaOp::CalvinDrop` discards
//! them (leaving base unchanged).
//!
//! These drive a `CoreLoop` directly through the SPSC ring so the atomicity
//! seam is observed without any scheduler timing: nothing a stage writes is
//! visible until the flush, and a drop never makes it visible.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Priority, Request, Response, Status};
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::*;
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_physical::physical_plan::{KvOp, MetaOp, PhysicalPlan};
use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};

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
        Arc::new(nodedb_types::OrdinalClock::new()),
    )
    .unwrap();
    (core, req_tx, resp_rx, dir)
}

/// Build a request for `plan` on `vshard`, carrying an optional committed WAL
/// LSN (present on the seed write so its version is recorded).
fn make_request(plan: PhysicalPlan, vshard: u32, wal_lsn: Option<Lsn>) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(vshard),
        database_id: DatabaseId::DEFAULT,
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: nodedb_types::TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: nodedb::event::EventSource::RaftFollower,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn,
        admission: nodedb::bridge::envelope::Admission::Admitted,
    }
}

fn send(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plan: PhysicalPlan,
    vshard: u32,
    wal_lsn: Option<Lsn>,
) -> Response {
    tx.try_push(BridgeRequest {
        inner: make_request(plan, vshard, wal_lsn),
    })
    .unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

fn kv_put(coll: &str, key: &[u8], value: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: coll.to_string(),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
    })
}

fn kv_get(coll: &str, key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: coll.to_string(),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    })
}

fn stage_static(
    epoch: u64,
    position: u32,
    plans: Vec<PhysicalPlan>,
    versioned_reads: Vec<VersionedReadEntry>,
) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
        epoch,
        position,
        tenant_id: TenantId::new(1),
        plans,
        epoch_system_ms: 0,
        is_group_leader: true,
        versioned_reads,
    })
}

/// A valid (empty read-set) staged write is NOT visible until the flush, and
/// the flush makes it visible — the stage/flush atomicity seam.
#[test]
fn flush_makes_staged_calvin_write_visible() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Stage a write with an empty read-set → vote is valid (commit).
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(6, 0, vec![kv_put("flushcoll", b"fk", b"fv")], Vec::new()),
        0,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed");
    assert_eq!(
        staged.read_set_valid,
        Some(true),
        "empty read-set is vacuously current → commit vote"
    );

    // Not yet applied: the staged write is invisible before the flush.
    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("flushcoll", b"fk"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "staged write must NOT be visible before flush; got {before:?}"
    );

    // Flush replays the staged plans to base.
    let flush = send(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::CalvinFlush {
            epoch: 6,
            position: 0,
        }),
        0,
        None,
    );
    assert_eq!(flush.status, Status::Ok, "flush must succeed: {flush:?}");

    // Now visible.
    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("flushcoll", b"fk"),
        0,
        None,
    );
    assert_eq!(
        after.status,
        Status::Ok,
        "read after flush must succeed: {after:?}"
    );
    assert!(
        !after.payload.is_empty(),
        "flushed write must be visible after flush"
    );
}

/// An invalid vote (a stale versioned read against a newer local write) STAGES
/// but never applies; the drop discards it and base stays unchanged.
#[test]
fn drop_discards_invalid_staged_calvin_write() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed a committed write to `dropcoll` at LSN 100 so its collection write
    // version floor is 100. The seed carries a WAL LSN so the version records.
    let seed = send(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![kv_put("dropcoll", b"seed", b"v")],
        }),
        0,
        Some(Lsn::new(100)),
    );
    assert_eq!(seed.status, Status::Ok, "seed write must commit: {seed:?}");

    // The read entry's collection must home to the staged request's vShard for
    // the read-set check to consider it.
    let read_vshard =
        VShardId::from_collection_in_database(DatabaseId::DEFAULT, "dropcoll").as_u32();

    // A read of `dropcoll` observed at LSN 50 — stale against the seed's write
    // at LSN 100 → the read-set is no longer current → abort vote.
    let stale_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "dropcoll".to_string(),
        key: ReadKeyIdent::Predicate,
        read_lsn: Lsn::new(50),
    };

    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            7,
            0,
            vec![kv_put("targetcoll", b"tk", b"tv")],
            vec![stale_read],
        ),
        read_vshard,
        None,
    );
    assert_eq!(
        staged.status,
        Status::Ok,
        "stage must succeed even on abort vote"
    );
    assert_eq!(
        staged.read_set_valid,
        Some(false),
        "stale read-set must produce an abort vote"
    );

    // Staged, not applied: the target write is invisible.
    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("targetcoll", b"tk"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "aborted staged write must NOT be visible; got {before:?}"
    );

    // Drop discards the staged plans and fires nothing. The drop must target
    // the SAME vShard the stage keyed under (as production dispatches it), so it
    // actually pops this participant's staged slice.
    let dropped = send(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::CalvinDrop {
            epoch: 7,
            position: 0,
        }),
        read_vshard,
        None,
    );
    assert_eq!(dropped.status, Status::Ok, "drop must succeed: {dropped:?}");

    // Still invisible after the drop — base was never mutated.
    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("targetcoll", b"tk"),
        0,
        None,
    );
    assert!(
        after.payload.is_empty() || after.status == Status::Error,
        "dropped write must never be visible; got {after:?}"
    );
}
