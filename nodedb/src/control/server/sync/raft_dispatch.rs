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
use crate::control::state::SharedState;
use crate::control::wal_replication::{AsyncRaftProposer, ReplicatedEntry, to_replicated_entry};
use crate::event::EventSource;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, VShardId};

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
pub async fn propose_sync_write(
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
            Ok(p) => {
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
pub async fn dispatch_sync_response(
    state: &SharedState,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    event_source: EventSource,
) -> crate::Result<Response> {
    // The sync inbound envelope carries no session database yet (see
    // `dispatch_sync_bytes` below), so this path is scoped to the default
    // database, same as its local-fallback branch a few lines down.
    if let Some(proposer) = state.async_raft_proposer.get()
        && let Some(entry) = to_replicated_entry(tenant_id, DatabaseId::DEFAULT, vshard_id, &plan)
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
            write_set: Vec::new(),
        });
    }

    crate::control::server::dispatch_utils::dispatch_to_data_plane_with_source(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        vshard_id,
        plan,
        trace_id,
        event_source,
    )
    .await
}

/// Sync-path convenience over [`dispatch_sync_response`]: dispatches `plan`
/// tagged [`EventSource::CrdtSync`] (so AFTER triggers are not re-fired on
/// synced data) with a zero trace id, and returns just the apply-payload
/// bytes — which carry the zerompk-encoded [`SyncAckResult`] the per-engine
/// handlers decode for the gate verdict.
///
/// Every `SharedState*Dispatcher` funnels through here so the dispatch policy
/// (event source, trace id, payload extraction) lives in exactly one place.
pub async fn dispatch_sync_payload(
    state: &SharedState,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<Vec<u8>> {
    let response = dispatch_sync_response(
        state,
        tenant_id,
        vshard_id,
        plan,
        TraceId::ZERO,
        EventSource::CrdtSync,
    )
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
/// [`crate::control::server::shared::ddl::sync_dispatch::dispatch_async_with_source`].
pub async fn dispatch_sync_bytes(
    state: &SharedState,
    tenant_id: TenantId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
    event_source: EventSource,
) -> crate::Result<Vec<u8>> {
    // The sync inbound envelope carries no session database yet, so the Lite
    // sync path is scoped to the default database.
    dispatch_write_replicated(
        state,
        tenant_id,
        DatabaseId::DEFAULT,
        collection,
        plan,
        timeout,
        event_source,
    )
    .await
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
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
    event_source: EventSource,
) -> crate::Result<Vec<u8>> {
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);

    if let Some(proposer) = state.async_raft_proposer.get()
        && let Some(entry) = to_replicated_entry(tenant_id, database_id, vshard_id, &plan)
    {
        return propose_sync_write(state, entry, proposer).await;
    }

    let resp =
        crate::control::server::shared::ddl::sync_dispatch::dispatch_async_response_with_source(
            state,
            tenant_id,
            database_id,
            collection,
            plan,
            timeout,
            event_source,
        )
        .await?;

    if resp.status != Status::Ok {
        // Preserve the typed Data-Plane error code so the CRDT delta path can
        // build a precise compensation hint by type instead of substring-matching
        // a human message.
        return Err(match resp.error_code {
            Some(code) => crate::Error::DataPlane(code),
            None => crate::Error::Internal {
                detail: String::from_utf8_lossy(&resp.payload).into_owned(),
            },
        });
    }

    // Mirror `dispatch_async_with_source`: advance the tenant write-HLC on the
    // success path (the Response-returning core leaves this to the caller).
    state.advance_tenant_write_hlc(tenant_id.as_u64());
    Ok(resp.payload.to_vec())
}
