// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for `CatalogEntry::DeleteMaterializedView`.
//!
//! Runs on **every node** so each follower reclaims its local copy
//! of the MV's columnar segment files + in-memory refresh state —
//! mirroring the `UnregisterCollection` pattern one level up.

use std::sync::Arc;

use tracing::debug;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

/// Dispatch `MetaOp::UnregisterMaterializedView` to every core on
/// this node. Fire-and-forget: any core that fails or times out
/// logs at debug — reclaim is idempotent and the next MV drop on
/// the same `(tenant, name)` picks up where this one left off.
pub async fn delete_async(tenant_id: u64, name: String, shared: Arc<SharedState>) {
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
                database_id: DatabaseId::DEFAULT,
                vshard_id: VShardId::new(core_id as u32),
                plan: PhysicalPlan::Meta(MetaOp::UnregisterMaterializedView {
                    tenant_id,
                    name: name.clone(),
                }),
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
                debug!(
                    tenant_id,
                    mv = %name,
                    core_id,
                    "materialized view reclaim ack"
                );
            }
            _ => {
                debug!(
                    tenant_id,
                    mv = %name,
                    core_id,
                    "materialized view reclaim: core did not ack (will retry idempotently on next drop)"
                );
            }
        }
    }
}
