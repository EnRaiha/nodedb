// SPDX-License-Identifier: BUSL-1.1

//! Sync dispatch that returns raw payload bytes, used by the CRDT delta path.

use std::time::Duration;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicableWrite, to_replicated_entry};
use crate::event::EventSource;
use crate::types::{Lsn, VShardId};

use super::admission_guard::reject_unadmitted_crdt_apply;
use super::outcome::SyncDispatchOutcome;
use super::propose::propose_sync_write;

/// Dispatch a sync write and return the apply payload plus what CRDT admission
/// measured about the delta. Cluster path proposes through Raft; single-node
/// falls through to `sync_dispatch::dispatch_system_with_source`.
pub async fn dispatch_sync_bytes(
    state: &SharedState,
    collection: &str,
    authorized: AuthorizedTask,
    timeout: Duration,
    event_source: EventSource,
    policy: &dyn crate::control::crdt_admission::CrdtPostImagePolicy,
) -> crate::Result<SyncDispatchOutcome> {
    // Sync inbound envelope carries no session database, so scoped to the default database.
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
    // Mints no redo of its own — the admitted-apply path and Raft entry own durability.
    dispatch_write_replicated(state, collection, authorized, timeout, event_source, None)
        .await
        .map(SyncDispatchOutcome::untrimmed)
}

/// Dispatch a write so it is quorum-durable when the node is clustered.
///
/// Cluster path proposes through Raft and blocks until applied locally. Single-node
/// path waits on `wal_lsn` (the caller's already-appended redo) before returning.
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
        && let Some(entry) = to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &ReplicableWrite::decide_for_replication(&plan)?,
        )?
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
        // Preserve the typed error code so the CRDT delta path builds a precise
        // compensation hint instead of substring-matching a message.
        return Err(match resp.error_code {
            Some(code) => crate::Error::DataPlane(*code),
            None => crate::Error::Internal {
                detail: String::from_utf8_lossy(&resp.payload).into_owned(),
            },
        });
    }

    // System-task dispatch bypasses the write funnel's own durable-at-ack barrier —
    // without this fsync, `kill -9` erases an acked write.
    if let Some(lsn) = wal_lsn {
        state.wal.wait_durable(lsn).await?;
    }

    // Mirrors `dispatch_system_with_source`'s success-path write-HLC advance.
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

    /// Guards the single-node branch's durable-at-ack barrier: without the wait
    /// below, the record is still only buffered when the peer hears "applied".
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
