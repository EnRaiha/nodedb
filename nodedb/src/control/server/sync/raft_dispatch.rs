// SPDX-License-Identifier: BUSL-1.1

//! Sync write dispatch helpers that route through Raft when clustered.
//!
//! In a multi-node deployment the `async_raft_proposer` field is set on
//! [`SharedState`] after Raft starts. When it is `Some` **and** the write
//! plan maps to a [`ReplicatedEntry`], the write is proposed to the Raft
//! group and blocks here until the entry is committed to a quorum and
//! applied on the local node. That gives quorum-durable ACK semantics: an
//! acknowledged sync write cannot be lost on leader failover.
//!
//! The idempotency gate embedded in every [`ReplicatedEntry`] runs on every
//! replica via the replicated provenance, so a reconnecting Lite client that
//! re-sends a delta on failover will be deduplicated on the new leader.
//!
//! Single-node deployments never set `async_raft_proposer`, so they always
//! fall through to the local Data Plane path — zero overhead.

use std::sync::Arc;
use std::time::Duration;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::{
    AuthorizedTask, authorize_collection, authorize_task_set,
};
use crate::control::state::SharedState;
use crate::control::wal_replication::{AsyncRaftProposer, ReplicatedEntry, to_replicated_entry};
use crate::event::EventSource;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub fn authorize_sync_collection(
    state: &SharedState,
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
) -> crate::Result<()> {
    let identity = identity.ok_or_else(|| crate::Error::RejectedAuthz {
        tenant_id,
        resource: "authenticated sync identity required".into(),
    })?;
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Write,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)
}

pub fn authorize_sync_task(
    state: &SharedState,
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<AuthorizedTask> {
    let identity = identity.ok_or_else(|| crate::Error::RejectedAuthz {
        tenant_id,
        resource: "authenticated sync identity required".into(),
    })?;
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "sync authorization returned no capability".into(),
    })
}

/// Propose a [`ReplicatedEntry`] through Raft and block until the entry is
/// committed to a quorum and applied on the local node.
///
/// Returns the apply-payload bytes produced by the Data Plane after the entry
/// is applied. These bytes carry the [`SyncAckResult`] that the handler decodes
/// to determine the idempotency gate verdict.
///
/// Retries transparently up to five times on [`crate::Error::RetryableLeaderChange`]
/// (leader failover during the propose). Any other error is mapped to
/// [`crate::Error::Dispatch`].
pub(crate) async fn propose_sync_write(
    state: &SharedState,
    entry: ReplicatedEntry,
    proposer: &Arc<AsyncRaftProposer>,
) -> crate::Result<Vec<u8>> {
    let idempotency_key = entry.idempotency_key;
    let data = entry.to_bytes();
    let vshard_id = entry.vshard_id;

    const BACKOFF_MS: [u64; 5] = [10, 25, 50, 100, 200];
    let mut payload: Option<Vec<u8>> = None;
    let mut last_err: Option<crate::Error> = None;

    for (attempt, backoff_ms) in BACKOFF_MS.iter().enumerate() {
        match proposer(vshard_id, idempotency_key, data.clone()).await {
            // The committed log index rides alongside the payload; the sync-ack
            // path only needs the payload bytes.
            Ok((p, _committed_version)) => {
                payload = Some(p);
                break;
            }
            Err(crate::Error::RetryableLeaderChange {
                group_id,
                log_index,
            }) => {
                state
                    .raft_propose_leader_change_retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    attempt,
                    group_id,
                    log_index,
                    "raft entry overwritten by leader change — re-proposing"
                );
                last_err = Some(crate::Error::RetryableLeaderChange {
                    group_id,
                    log_index,
                });
                tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
                continue;
            }
            Err(other @ crate::Error::DataPlane(_)) => return Err(other),
            Err(other) => {
                return Err(crate::Error::Dispatch {
                    detail: format!("raft propose failed: {other}"),
                });
            }
        }
    }

    payload.ok_or_else(|| {
        last_err.unwrap_or_else(|| crate::Error::Dispatch {
            detail: "raft propose retries exhausted".into(),
        })
    })
}

/// Dispatch a sync write that returns a full [`Response`].
///
/// Used by the columnar, timeseries, FTS, spatial, and vector sync handlers,
/// which need the raw `Response` to call `.payload.to_vec()`.
///
/// Cluster path: proposes through Raft, then wraps the apply payload in a
/// `Status::Ok` `Response`. The gate verdict is carried in the payload (as a
/// zerompk-encoded [`SyncAckResult`]); `Status::Ok` is always correct here
/// because a non-`Ok` status signals a protocol error, not an idempotency
/// gate rejection.
///
/// Single-node path: falls through to
/// [`crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source`].
fn reject_unadmitted_crdt_apply(plan: &PhysicalPlan) -> crate::Result<()> {
    if matches!(
        plan,
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
                | nodedb_physical::physical_plan::CrdtOp::ImportSnapshot { .. }
        )
    ) {
        return Err(crate::Error::CrdtApplyRequiresAdmission);
    }
    Ok(())
}

pub async fn dispatch_authorized_sync_response(
    state: &SharedState,
    authorized: AuthorizedTask,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    dispatch_sync_response_inner(
        state,
        task.tenant_id,
        task.database_id,
        task.vshard_id,
        task.plan,
        trace_id,
        event_source,
    )
    .await
}

/// Trusted-internal sync-shaped dispatch used by DDL index maintenance.
pub(crate) async fn dispatch_trusted_internal_sync_response(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    dispatch_sync_response_inner(
        state,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
    )
    .await
}

async fn dispatch_sync_response_inner(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    reject_unadmitted_crdt_apply(&plan)?;
    if let Some(proposer) = state.async_raft_proposer()
        && let Some(entry) = to_replicated_entry(tenant_id, database_id, vshard_id, &plan)
    {
        let payload = propose_sync_write(state, entry, proposer).await?;
        let request_id = state.next_request_id();
        return Ok(Response {
            request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            watermark_lsn: Lsn::new(0),
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        });
    }

    crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
        state,
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
    )
    .await
}

/// Sync-path convenience over authorized sync dispatch: dispatches `plan`
/// tagged [`EventSource::CrdtSync`] (so AFTER triggers are not re-fired on
/// synced data) with a zero trace id, and returns just the apply-payload
/// bytes — which carry the zerompk-encoded [`SyncAckResult`] the per-engine
/// handlers decode for the gate verdict.
///
/// Every `SharedState*Dispatcher` funnels through here so the dispatch policy
/// (event source, trace id, payload extraction) lives in exactly one place.
pub async fn dispatch_sync_payload(
    state: &SharedState,
    authorized: AuthorizedTask,
) -> crate::Result<Vec<u8>> {
    let response =
        dispatch_authorized_sync_response(state, authorized, TraceId::ZERO, EventSource::CrdtSync)
            .await?;
    Ok(response.payload.to_vec())
}

/// Build the loud error every `NoOp*Dispatcher` returns when a sync op reaches
/// a path that lacks `SharedState`.
///
/// Such a path would ACK the Lite client while silently dropping the write, so
/// the dispatcher fails loudly instead of no-op'ing. `op` names the operation
/// for the diagnostic, e.g. `"vector insert"` or `"timeseries push"`.
pub fn noop_dispatch_error(op: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "{op} routed through path lacking SharedState; \
             check listener wiring — {op} was ACKed but NOT applied"
        ),
    }
}

/// Dispatch a sync write that returns raw payload bytes.
///
/// Used by the CRDT async dispatch path, which already extracts the payload
/// bytes and decodes the [`SyncAckResult`] itself.
///
/// Cluster path: proposes through Raft and returns the apply payload bytes.
///
/// Single-node path: falls through to
/// [`crate::control::server::shared::ddl::sync_dispatch::dispatch_system_with_source`].
pub async fn dispatch_sync_bytes(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    timeout: Duration,
    event_source: EventSource,
    policy: &dyn crate::control::crdt_admission::CrdtPostImagePolicy,
) -> crate::Result<Vec<u8>> {
    // The sync inbound envelope carries no session database yet, so the Lite
    // sync path is scoped to the default database.
    if matches!(
        authorized.plan(),
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
        )
    ) {
        return crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted(
            state,
            crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
                authorized,
                collection,
                timeout,
                event_source,
                policy,
            },
        )
        .await;
    }
    dispatch_write_replicated(state, collection, authorized, timeout, event_source).await
}

/// Dispatch a write so it is quorum-durable when the node is clustered.
///
/// Computes the vshard from `database_id` + `collection`, then applies the same
/// proposer-gate-then-local-fallback policy used by every write that must not be
/// lost on leader failover:
///
/// * Cluster path — when `async_raft_proposer` is set and `plan` maps to a
///   [`ReplicatedEntry`], the write is proposed through Raft and blocks until the
///   entry is committed to a quorum and applied locally. This is what makes a
///   `crdt_apply` durable across replicas: without it the delta lands only on the
///   receiving node and is lost to every follower (and entirely on leader
///   failover).
/// * Single-node / non-replicated path — `async_raft_proposer` is `None` (or the
///   plan has no replicated form), so the write goes straight to the local Data
///   Plane and the tenant write-HLC is advanced on success.
///
/// Returns the apply-payload bytes the caller can map to its own success shape.
pub async fn dispatch_write_replicated(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    timeout: Duration,
    event_source: EventSource,
) -> crate::Result<Vec<u8>> {
    let task = authorized.into_physical_task();
    let tenant_id = task.tenant_id;
    let database_id = task.database_id;
    let vshard_id = task.vshard_id;
    let plan = task.plan;
    reject_unadmitted_crdt_apply(&plan)?;
    if vshard_id != VShardId::from_collection_in_database(database_id, collection) {
        return Err(crate::Error::Internal {
            detail: "authorized sync task vShard does not match collection".into(),
        });
    }
    let local_frontier_mutation = matches!(
        &plan,
        PhysicalPlan::Crdt(op) if crate::control::crdt_admission::changes_crdt_frontier(op)
    );

    if let Some(proposer) = state.async_raft_proposer()
        && let Some(entry) = to_replicated_entry(tenant_id, database_id, vshard_id, &plan)
    {
        return propose_sync_write(state, entry, proposer).await;
    }

    let resp = if local_frontier_mutation {
        state
            .vshard_admission_sequencer
            .run(vshard_id, || async {
                crate::control::server::shared::ddl::sync_dispatch::dispatch_system_response_with_source(
                    state,
                    crate::control::server::shared::ddl::sync_dispatch::SystemTask::new(
                        crate::control::server::shared::ddl::sync_dispatch::SystemReason::AdmittedContinuation,
                        tenant_id,
                        database_id,
                        collection,
                        plan,
                    ),
                    timeout,
                    event_source,
                )
                .await
            })
            .await?
    } else {
        crate::control::server::shared::ddl::sync_dispatch::dispatch_system_response_with_source(
            state,
            crate::control::server::shared::ddl::sync_dispatch::SystemTask::new(
                crate::control::server::shared::ddl::sync_dispatch::SystemReason::AdmittedContinuation,
                tenant_id,
                database_id,
                collection,
                plan,
            ),
            timeout,
            event_source,
        )
        .await?
    };

    if resp.status != Status::Ok {
        // Preserve the typed Data-Plane error code so the CRDT delta path can
        // build a precise compensation hint by type instead of substring-matching
        // a human message.
        return Err(match resp.error_code {
            Some(code) => crate::Error::DataPlane(*code),
            None => crate::Error::Internal {
                detail: String::from_utf8_lossy(&resp.payload).into_owned(),
            },
        });
    }

    // Mirror `dispatch_system_with_source`: advance the tenant write-HLC on the
    // success path (the Response-returning core leaves this to the caller).
    state.advance_tenant_write_hlc(tenant_id.as_u64());
    Ok(resp.payload.to_vec())
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::CrdtOp;
    use nodedb_types::Surrogate;

    use super::*;

    #[test]
    fn generic_sync_dispatch_rejects_unadmitted_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: "docs".into(),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert!(matches!(
            reject_unadmitted_crdt_apply(&plan),
            Err(crate::Error::CrdtApplyRequiresAdmission)
        ));
    }
}
