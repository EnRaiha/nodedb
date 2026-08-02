// SPDX-License-Identifier: BUSL-1.1

//! Sync dispatch that returns raw payload bytes, used by the CRDT delta path.

use std::time::Duration;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::control::wal_replication::to_replicated_entry;
use crate::event::EventSource;
use crate::types::{Lsn, VShardId};

use super::admission_guard::reject_unadmitted_crdt_apply;
use super::outcome::SyncDispatchOutcome;
use super::propose::propose_sync_write;

/// Dispatch a sync write and return the apply payload plus what the CRDT
/// admission measured about the delta on the way through.
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
) -> crate::Result<SyncDispatchOutcome> {
    // The sync inbound envelope carries no session database yet, so the Lite
    // sync path is scoped to the default database.
    if matches!(
        authorized.plan(),
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
        )
    ) {
        let outcome =
            crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted_outcome(
                state,
                crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
                    authorized,
                    collection,
                    timeout,
                    event_source,
                    policy,
                },
            )
            .await?;
        return Ok(SyncDispatchOutcome {
            payload: outcome.payload,
            trimmed_ops: outcome.trimmed_ops,
        });
    }
    // The CRDT delta path mints no redo of its own before this call — the
    // admitted-apply path and the Raft entry own its durability — so it supplies
    // no LSN.
    dispatch_write_replicated(state, collection, authorized, timeout, event_source, None)
        .await
        .map(SyncDispatchOutcome::untrimmed)
}

/// Dispatch a write so it is quorum-durable when the node is clustered.
///
/// Computes the vshard from `database_id` + `collection`, then applies the same
/// proposer-gate-then-local-fallback policy used by every write that must not be
/// lost on leader failover:
///
/// * Cluster path — when `async_raft_proposer` is set and `plan` maps to a
///   `ReplicatedEntry`, the write is proposed through Raft and blocks until the
///   entry is committed to a quorum and applied locally. This is what makes a
///   `crdt_apply` durable across replicas: without it the delta lands only on the
///   receiving node and is lost to every follower (and entirely on leader
///   failover).
/// * Single-node / non-replicated path — `async_raft_proposer` is `None` (or the
///   plan has no replicated form), so the write goes straight to the local Data
///   Plane and the tenant write-HLC is advanced on success.
///
/// `wal_lsn` names the redo record the caller appended for this write before
/// calling, or `None` when it appended none. The single-node path waits on it
/// before returning: the sync handlers turn this return value straight into the
/// peer's "applied" ack, and a peer that reads that retires the batch forever.
/// The Raft path does not need it — the committed entry is the durable record.
///
/// Returns the apply-payload bytes the caller can map to its own success shape.
pub async fn dispatch_write_replicated(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    timeout: Duration,
    event_source: EventSource,
    wal_lsn: Option<Lsn>,
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

    // Durable-at-ack barrier for the single-node path. The system-task dispatch
    // above hands the plan to the Data Plane directly, so the Control-Plane write
    // funnel's own barrier never runs for it; the record the caller appended is
    // still only buffered at this point. Every engine that reaches here rebuilds
    // its state by WAL replay alone, so returning without this fsync acks a write
    // a `kill -9` erases — and the peer, having seen "applied", never re-sends it.
    if let Some(lsn) = wal_lsn {
        state.wal.wait_durable(lsn).await?;
    }

    // Mirror `dispatch_system_with_source`: advance the tenant write-HLC on the
    // success path (the Response-returning core leaves this to the caller).
    state.advance_tenant_write_hlc(tenant_id.as_u64());
    Ok(resp.payload.to_vec())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::super::durability_test_support::{
        COLLECTION, append_buffered_record, authorized_write, fixture, respond_once,
    };
    use super::dispatch_write_replicated;
    use crate::event::EventSource;

    /// The timeseries and columnar sync handlers append their redo and then ack
    /// their peer off this return value. The single-node branch hands the plan
    /// to the Data Plane directly, so the write funnel's own barrier never runs
    /// for it — without the wait below the record is still only buffered when
    /// the peer is told "applied", and it never re-sends.
    #[tokio::test]
    async fn a_supplied_lsn_is_fsync_durable_before_the_payload_returns() {
        let (state, side, _directory) = fixture();
        let lsn = append_buffered_record(&state);
        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "the append must only buffer, or this test proves nothing"
        );
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_write_replicated(
            &state,
            COLLECTION,
            authorized,
            Duration::from_secs(5),
            EventSource::CrdtSync,
            Some(lsn),
        )
        .await
        .expect("replicated sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() >= lsn.as_u64(),
            "the supplied redo must be fsync-durable before the peer is acked"
        );
    }

    /// A caller that appended nothing has nothing to wait on, which is what
    /// makes the assertion above a statement about the threaded LSN.
    #[tokio::test]
    async fn no_supplied_lsn_leaves_an_unrelated_buffered_record_alone() {
        let (state, side, _directory) = fixture();
        let lsn = append_buffered_record(&state);
        let authorized = authorized_write(&state);

        let responder = tokio::spawn(respond_once(Arc::clone(&state), side));
        dispatch_write_replicated(
            &state,
            COLLECTION,
            authorized,
            Duration::from_secs(5),
            EventSource::CrdtSync,
            None,
        )
        .await
        .expect("replicated sync dispatch succeeds");
        responder.await.expect("responder completes");

        assert!(
            state.wal.durable_through() < lsn.as_u64(),
            "nothing appended by this dispatch means nothing to fsync"
        );
    }
}
