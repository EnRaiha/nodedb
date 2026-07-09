// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for continuous-aggregate catalog entries.
//!
//! `PutContinuousAggregate` dispatches `MetaOp::RegisterContinuousAggregate`
//! to every core on this node so the local `continuous_agg_mgr` picks up
//! the new definition without re-issuing the DDL — this is what makes the
//! registration consistent across leader and followers after the raft
//! commit. `DeleteContinuousAggregate` dispatches the matching
//! `MetaOp::UnregisterContinuousAggregate`.

use std::sync::Arc;

use tracing::debug;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

/// Dispatch `MetaOp::RegisterContinuousAggregate` to every core on
/// this node. `def_bytes` is the MessagePack-encoded
/// `ContinuousAggregateDef` from the catalog row.
pub async fn put_async(tenant_id: u64, name: String, def_bytes: Vec<u8>, shared: Arc<SharedState>) {
    let def: ContinuousAggregateDef = match zerompk::from_msgpack(&def_bytes) {
        Ok(def) => def,
        Err(e) => {
            debug!(
                tenant_id,
                cagg = %name,
                error = %e,
                "continuous aggregate: failed to deserialize def — skipping register"
            );
            return;
        }
    };
    let database_id = def.database_id;
    dispatch_meta(
        shared,
        database_id,
        tenant_id,
        &name,
        MetaOp::RegisterContinuousAggregate { def },
        "register",
    )
    .await;
}

/// Dispatch `MetaOp::UnregisterContinuousAggregate` to every core
/// on this node. `database_id` scopes the unregister to the right
/// per-database manager map.
pub async fn delete_async(
    database_id: u64,
    tenant_id: u64,
    name: String,
    shared: Arc<SharedState>,
) {
    dispatch_meta(
        shared,
        database_id,
        tenant_id,
        &name,
        MetaOp::UnregisterContinuousAggregate { name: name.clone() },
        "unregister",
    )
    .await;
}

async fn dispatch_meta(
    shared: Arc<SharedState>,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    op: MetaOp,
    label: &'static str,
) {
    let num_cores = {
        let d = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        d.num_cores()
    };
    let timeout = std::time::Duration::from_secs(30);
    let mut receivers = Vec::with_capacity(num_cores);

    {
        let mut d = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        for core_id in 0..num_cores {
            let request_id = shared.next_request_id();
            let request = Request {
                request_id,
                tenant_id: TenantId::new(tenant_id),
                database_id: DatabaseId::new(database_id),
                vshard_id: VShardId::new(core_id as u32),
                plan: PhysicalPlan::Meta(op.clone()),
                deadline: std::time::Instant::now() + timeout,
                priority: Priority::Background,
                trace_id: TraceId::generate(),
                consistency: ReadConsistency::Eventual,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            };
            let rx = shared.tracker.register(request_id);
            if d.dispatch_to_core(core_id, request).is_err() {
                shared.tracker.cancel(&request_id);
                continue;
            }
            receivers.push((core_id, rx));
        }
    }

    for (core_id, mut rx) in receivers {
        match tokio::time::timeout(timeout, async { rx.recv().await.ok_or(()) }).await {
            Ok(Ok(resp)) if resp.status == Status::Ok => {
                debug!(tenant_id, cagg = %name, core_id, %label, "continuous aggregate ack");
            }
            _ => {
                debug!(
                    tenant_id,
                    cagg = %name,
                    core_id,
                    %label,
                    "continuous aggregate dispatch: core did not ack"
                );
            }
        }
    }
}
