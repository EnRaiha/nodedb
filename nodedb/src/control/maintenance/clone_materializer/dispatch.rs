// SPDX-License-Identifier: BUSL-1.1

//! Local-data-plane dispatch helper used by the clone materializer.
//!
//! Lets the walker issue scans and writes against source/target collections
//! without pgwire. The materializer runs on a Tokio blocking thread and uses
//! [`tokio::runtime::Handle::block_on`] to drive these futures synchronously.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;

/// Dispatch a `PhysicalPlan` to the local Data Plane and await the response.
///
/// Bypasses WAL replication coordination (the engine handler still appends
/// the WAL on mutation). Used for both source scans and target writes; every
/// shard the materializer touches is owned locally.
///
/// `txn_id` stamps the request with the transaction whose staging overlay
/// the handler must fold in. Autocommit callers pass `None`; COMMIT-time
/// MERGE / `UPDATE ... FROM` expanders pass the transaction id so the
/// RESOLVE pass sees rows staged earlier in the same transaction.
pub(crate) async fn dispatch_local(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection_qualified: &str,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let vshard_id = VShardId::from_collection_in_database(database_id, collection_qualified);
    dispatch_local_on_vshard(state, tenant_id, database_id, vshard_id, plan, txn_id).await
}

/// [`dispatch_local`] for a plan whose home vShard is not derived from a
/// collection name.
///
/// A graph edge plan is key-homed on its endpoints, so the vShard that holds
/// the edge is `VShardId::from_key(src_id)` and not the collection's hash.
/// Routing such a plan by collection reads a shard that never held it.
pub(crate) async fn dispatch_local_on_vshard(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    let req_id = RequestId::new(state.request_id_counter.fetch_add(1, Ordering::Relaxed));
    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline_dur = Duration::from_secs(deadline_secs);
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
