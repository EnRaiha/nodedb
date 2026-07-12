// SPDX-License-Identifier: BUSL-1.1

//! Local-data-plane dispatch helper used by the clone materializer.
//!
//! Mirrors the shape of `clone_write_dispatch::dispatch_data_plane_raw` but
//! is exposed in the maintenance module so the walker can issue scans and
//! writes against source/target collections without going through pgwire.
//! The materializer runs on a Tokio blocking thread (DDL handlers via
//! `spawn_blocking`, background sweep via `spawn_blocking`); it uses
//! [`tokio::runtime::Handle::block_on`] to drive these futures synchronously
//! from sync call sites.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;

/// Dispatch a `PhysicalPlan` to the local Data Plane and await the response.
///
/// Bypasses WAL replication coordination: the WAL append still happens inside
/// the engine handler when the plan mutates state. Read plans take the
/// standard read path. This is used by the materializer for both source-side
/// scans and target-side writes — both target the local node directly because
/// every shard the materializer touches is owned locally (vshard-affinity is
/// preserved by `VShardId::from_collection_in_database`).
///
/// `txn_id` stamps the dispatched request with the transaction whose staging
/// overlay the Data Plane handler must fold into its read. Autocommit callers
/// (the clone materializer, `run_merge` / `run_update_from_join`, the
/// `INSERT ... SELECT` orchestrator) pass `None` — no overlay exists, so the
/// handler reads base storage exactly as before. The COMMIT-time MERGE /
/// `UPDATE ... FROM` expanders pass the transaction's id so the RESOLVE pass
/// (and its source scan) observes rows staged by earlier statements in the
/// same transaction.
pub(crate) async fn dispatch_local(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection_qualified: &str,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let req_id = RequestId::new(state.request_id_counter.fetch_add(1, Ordering::Relaxed));
    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline_dur = Duration::from_secs(deadline_secs);
    let vshard_id = VShardId::from_collection_in_database(database_id, collection_qualified);
    let req = Request {
        request_id: req_id,
        tenant_id,
        vshard_id,
        database_id,
        plan,
        deadline: Instant::now() + deadline_dur,
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };
    let mut rx = state.tracker.register(req_id);
    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(req)?,
        Err(p) => p.into_inner().dispatch(req)?,
    }
    tokio::time::timeout(deadline_dur, rx.recv())
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id: req_id })?
        .ok_or(crate::Error::Dispatch {
            detail: "clone materializer: response channel closed".into(),
        })
}
