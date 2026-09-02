// SPDX-License-Identifier: BUSL-1.1

//! Fan a post-apply physical plan out to every core on this node.
//!
//! A committed catalog entry whose physical work runs per node dispatches the
//! same plan to each core, matching how the boot seed installs state on every
//! core rather than one vshard. The caller owns the report for an unreached
//! core, because it alone knows which capture site names the mutation.

use std::time::Duration;

use tracing::debug;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

/// Deadline for one core's acknowledgement of a post-apply meta op.
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Scope and naming for one fan-out, as every log line and error reports it.
pub(super) struct CoreFanout<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection the plan targets.
    pub collection: &'a str,
    /// Names the mutation in the ack line and the unreached-core error.
    pub what: &'a str,
    /// Extra identity for the ack line. Empty when the collection names it all.
    pub detail: &'a str,
}

/// Dispatch `plan` to every core on this node, returning the cores that never
/// acknowledged it.
pub(super) async fn dispatch_to_every_core(
    shared: &SharedState,
    target: &CoreFanout<'_>,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    let num_cores = {
        let d = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        d.num_cores()
    };
    let mut receivers = Vec::with_capacity(num_cores);
    let mut unreached: Vec<usize> = Vec::new();

    {
        let mut d = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        for core_id in 0..num_cores {
            let request_id = shared.next_request_id();
            let request = Request {
                request_id,
                tenant_id: TenantId::new(target.tenant_id),
                database_id: DatabaseId::new(target.database_id),
                vshard_id: VShardId::new(core_id as u32),
                plan: plan.clone(),
                deadline: std::time::Instant::now() + DISPATCH_TIMEOUT,
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
                unreached.push(core_id);
                continue;
            }
            receivers.push((core_id, rx));
        }
    }

    for (core_id, mut rx) in receivers {
        match tokio::time::timeout(DISPATCH_TIMEOUT, async { rx.recv().await.ok_or(()) }).await {
            Ok(Ok(resp)) if resp.status == Status::Ok => {
                debug!(
                    tenant = target.tenant_id,
                    collection = %target.collection,
                    detail = target.detail,
                    core_id,
                    what = target.what,
                    "post-apply core ack"
                );
            }
            _ => unreached.push(core_id),
        }
    }

    if unreached.is_empty() {
        return Ok(());
    }
    Err(crate::Error::Internal {
        detail: format!("cores did not apply the {}: {unreached:?}", target.what),
    })
}
