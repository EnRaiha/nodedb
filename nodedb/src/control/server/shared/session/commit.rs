// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral COMMIT orchestration shared by pgwire and native sessions.

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::gateway::RouteDecision;
use crate::control::planner::calvin::{DispatchClass, classify_dispatch, read_vshards_of};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::metering::meter_buffered_write;
use crate::control::server::shared::plan_util::extract_collection;
use crate::control::server::shared::sql::staging_predicates::is_stageable_write;
use crate::control::state::SharedState;
use nodedb_cluster::calvin::types::ReleaseReason;
use nodedb_physical::physical_plan::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::connection::SessionId;
use super::ddl_buffer;
use super::outcome::{AbortReason, CommitOutcome, TxnDataPlane};
use super::overlay_drop::drop_txn_overlay;
use super::read_set::ReadSetEntry;
use super::store::SessionStore;

/// Run the neutral COMMIT sequence for one collision-free session.
///
/// Returns [`CommitOutcome::Committed`] once every durable batch has flushed
/// and all post-commit side effects have fired, or [`CommitOutcome::Aborted`]
/// with the reason the transport maps to its wire error.
pub async fn run_commit(
    sessions: &SessionStore,
    session_id: SessionId,
    identity: &AuthenticatedIdentity,
    state: &SharedState,
    dp: &impl TxnDataPlane,
) -> CommitOutcome {
    let read_set = sessions.take_read_set(session_id);
    // Collections this transaction wrote itself. A read of a collection the
    // same transaction has written is a read-your-own-write, not a
    // serialization conflict — reading uncommitted own state (served from the
    // staging overlay, which reports no watermark) must not abort the commit.
    // The read-set is collection-granular, so exclusion is too.
    let written_collections = sessions.buffered_collections(session_id, |plan| {
        extract_collection(plan).map(String::from)
    });
    // Peek the buffered write tasks WITHOUT draining them or leaving the block.
    // The session stays `InBlock` through classification and dispatch; the
    // buffered batch is flushed to Calvin as the COMMIT finalization (see
    // `run_commit_calvin`), then `sessions.commit` below drains the buffer.
    let buffered = sessions.buffered_tasks(session_id);
    let tenant_id = identity.tenant_id;
    // The interactive-COMMIT read-set widens dispatch classification: a txn that
    // writes shard X but read shard Y participates in {X, Y} and must route
    // through Calvin with Y as a participant. Autocommit has no session read-set.
    let read_vshards = read_vshards_of(&read_set);

    // In-transaction `MERGE`, `UPDATE ... FROM <source>`, and `INSERT ... SELECT`
    // are resolved + staged into concrete, surrogate-carrying point writes
    // (`PointInsert` / `PointPut` / `PointDelete`) at STATEMENT time
    // (`session::expander_stage`), so by COMMIT the buffer already holds those
    // concrete point ops — no raw `Merge` / `UpdateFromJoin` / `InsertSelect`
    // plan remains to expand here, and COMMIT invokes no expander at all.

    if buffered.is_empty() {
        // Read-only interactive transaction: no writes to classify, but it can
        // still serialization-conflict against concurrent writers. Run the
        // single-shard SI validation only — classifying an empty buffer would
        // misread a lone cross-shard READ as `MultiShard` and wrongly reject it.
        if let Some(outcome) =
            si_conflict_abort(sessions, session_id, state, &read_set, &written_collections)
        {
            // Release read reservations (owner still set), then roll back.
            super::reservation_release::release_and_rollback(state, sessions, session_id).await;
            return outcome;
        }
    } else {
        match classify_dispatch(&buffered, &read_vshards) {
            DispatchClass::MultiShard { .. } => {
                // Flush the buffered cross-shard batch through Calvin's durable
                // Vote/Verdict barrier (`run_commit_calvin`), leader-routed. SI is
                // a single-shard validation and is intentionally NOT run here —
                // Calvin performs its own cross-shard OCC over `versioned_reads`
                // and returns a serialization abort (SQLSTATE 40001) on an ABORT
                // verdict.
                if let Some(reason) = super::commit_calvin::run_commit_calvin(
                    sessions, session_id, state, &buffered, tenant_id, &read_set,
                )
                .await
                {
                    super::reservation_release::release_and_rollback(state, sessions, session_id)
                        .await;
                    return CommitOutcome::Aborted { reason };
                }
            }
            DispatchClass::SingleShard { vshard: vshard_id } => {
                let leader =
                    crate::control::server::graph_dispatch::cluster_resolve::resolve_for_vshard(
                        state,
                        vshard_id.as_u32(),
                    );
                if !matches!(leader, RouteDecision::Local) {
                    // The interactive transaction WAL record belongs to this
                    // coordinator and cannot be forwarded as a bare remote
                    // Data-Plane LSN. Route a non-local single-shard commit
                    // through Calvin's replicated Vote/Verdict barrier instead;
                    // this gives it the same leader routing, OCC, durability,
                    // and apply ordering as any multi-participant commit.
                    if let Some(reason) = super::commit_calvin::run_commit_calvin(
                        sessions, session_id, state, &buffered, tenant_id, &read_set,
                    )
                    .await
                    {
                        super::reservation_release::release_and_rollback(
                            state, sessions, session_id,
                        )
                        .await;
                        return CommitOutcome::Aborted { reason };
                    }
                } else {
                    if let Some(outcome) = si_conflict_abort(
                        sessions,
                        session_id,
                        state,
                        &read_set,
                        &written_collections,
                    ) {
                        super::reservation_release::release_and_rollback(
                            state, sessions, session_id,
                        )
                        .await;
                        return outcome;
                    }
                    if let Some(reason) =
                        dispatch_single_shard(state, dp, &buffered, tenant_id, vshard_id).await
                    {
                        super::reservation_release::release_and_rollback(
                            state, sessions, session_id,
                        )
                        .await;
                        return CommitOutcome::Aborted { reason };
                    }
                }
            }
        }
    }

    // Every abort branch above has already returned, so every buffered write
    // just durably committed. Meter the non-stageable ("Buffered") writes now
    // — this is the first point their dispatch has actually happened. A
    // stageable ("Staged") write was already metered at STATEMENT time
    // (`staging_gate::stage_write`, when it applied to the per-transaction
    // overlay), and is re-identified and skipped here by the exact same
    // `is_stageable_write` predicate `route_in_tx_write` used to route it —
    // metering it again here would double-bill it, since it is buffered for
    // durable replay same as a non-stageable write. `buffered` still holds
    // the peeked (not yet drained) task list, so this reads the same tasks
    // `dispatch_single_shard` / `run_commit_calvin` just replayed above.
    meter_committed_buffered_writes(state, identity, &buffered);

    // Release this transaction's read reservations (belt-and-suspenders: the
    // Calvin batch's `on_txn_complete` already releases the owner for keys in the
    // batch — this covers reserved keys not in it) while the owner is still set,
    // before `sessions.commit` drains the session below.
    super::reservation_release::release_session_reservations(
        state,
        sessions,
        session_id,
        ReleaseReason::Commit,
    )
    .await;
    // Transition the session out of the block NOW — this drains the write buffer
    // and clears snapshot/txn state, moving the session to `Idle`. Keep the
    // aligned descriptor-lease scope holders owned here through every remaining
    // cleanup step and response construction below.
    let (_drained_tasks, _lease_scopes) = match sessions.commit(session_id) {
        Ok(drained) => drained,
        Err(_msg) => {
            return CommitOutcome::Aborted {
                reason: AbortReason::NoTransaction,
            };
        }
    };

    // Release the per-transaction staging overlay on every vShard that hosted a
    // staged write, now that the durable batch(es) have flushed. Uses the peeked
    // buffer (identical contents to the drained one). Guarded on a staged
    // (txn_id-carrying) buffer.
    if let Some(txn_id) = buffered.first().and_then(|t| t.txn_id) {
        let mut dropped = std::collections::HashSet::new();
        for task in &buffered {
            if dropped.insert(task.vshard_id) {
                // The transaction is already durable at this point; a teardown
                // failure (e.g. the vShard's leader moved and the drop can no
                // longer reach the overlay) cannot un-commit it, so it is
                // surfaced at ERROR and the remaining vShards are still reaped
                // rather than aborting a committed transaction. `drop_txn_overlay`
                // already retries a transient remote failure a bounded number of
                // times internally (see `retry_not_leader`); a drop that still
                // fails after that budget strands a bounded, invisible (the
                // `txn_id` is never reused) overlay on the unreachable former
                // leader, cleared on that node's restart and visible meanwhile
                // via `active_txn_overlays`.
                if let Err(e) = drop_txn_overlay(state, dp, tenant_id, task.vshard_id, txn_id).await
                {
                    tracing::error!(
                        vshard = task.vshard_id.as_u32(),
                        error = %e,
                        "failed to release per-transaction staging overlay after commit"
                    );
                }
            }
        }
    }

    // Flush pending offset commits (deferred from COMMIT OFFSET inside transaction).
    let pending_offsets = sessions.take_pending_offsets(session_id);
    for pending_offset in pending_offsets {
        if let Err(e) = state.offset_store.commit_offset(
            pending_offset.database_id,
            pending_offset.tenant_id,
            &pending_offset.stream,
            &pending_offset.group,
            pending_offset.partition_id,
            pending_offset.offset,
        ) {
            tracing::warn!(
                stream = %pending_offset.stream,
                group = %pending_offset.group,
                partition = pending_offset.partition_id,
                error = %e,
                "failed to commit deferred offset"
            );
        }
    }

    // Finalize GAP_FREE reservations (numbers become permanent).
    let reservations = sessions.take_pending_reservations(session_id);
    for handle in &reservations {
        state.sequence_registry.gap_free_manager().commit(handle);
        // Log to _system.sequence_log.
        {
            let catalog = state.credentials.catalog();
            crate::control::sequence::log::log_reservation(
                catalog,
                &crate::control::sequence::log::committed(
                    &handle.sequence_key,
                    handle.value,
                    &identity.username,
                    identity.tenant_id.as_u64(),
                ),
            );
        }
    }

    // Flush any buffered DDL entries as a single atomic batch.
    if let Some(reason) = ddl_buffer::flush(state) {
        return CommitOutcome::Aborted { reason };
    }

    // Close non-WITH-HOLD cursors on transaction end.
    sessions.close_non_hold_cursors(session_id);
    // Flush NOTIFY messages buffered during this transaction.
    sessions.flush_pending_notifies(session_id, identity.tenant_id, &state.notify_bus);
    CommitOutcome::Committed
}

/// Meter every non-stageable ("Buffered") write in `buffered`, once its
/// COMMIT-time durable replay has already succeeded.
///
/// Skips any task `is_stageable_write` still classifies as stageable: that
/// task was already metered at STATEMENT time when it staged into the
/// per-transaction overlay (`staging_gate::stage_write`) — it is buffered
/// here too (COMMIT replays every write, staged or not, from the one durable
/// batch), but billing it again here would double-count it. Re-deriving the
/// predicate here, rather than carrying a "was this staged" flag on
/// `PhysicalTask`, keeps this in lockstep with `route_in_tx_write`'s own
/// routing decision by construction — the two can never independently drift.
///
/// Each task metered independently (its own collection/engine, `rows: None`
/// — one unit per write, matching every other door's convention for a
/// dispatch whose response carries no row payload to count).
fn meter_committed_buffered_writes(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    buffered: &[PhysicalTask],
) {
    if !state.metering_config.enabled {
        return;
    }
    for task in buffered {
        if is_stageable_write(&task.plan) {
            continue;
        }
        let scope = RequestAuthScope::builder(identity, state.auth_stores())
            .with_session_database(Some(task.database_id))
            .build();
        meter_buffered_write(state, &scope, &task.plan);
    }
}

/// Snapshot-isolation write-conflict check for a single-shard interactive
/// COMMIT. If any read key's collection advanced past both the read LSN and the
/// transaction snapshot LSN — and the transaction did not write that collection
/// itself (read-your-own-write is excluded) — the WAL moved under the reader:
/// records the read-set hot-key aborts and returns a serialization abort. The
/// caller owns the session rollback (via `release_and_rollback`) so it can
/// first release the transaction's read reservations while the reservation owner
/// is still set — the rollback clears it. Returns `None` when there is no
/// conflict (or no snapshot, i.e. not in a transaction).
///
/// This is a single-shard validation: it compares against the global WAL
/// `next_lsn`, so it is only sound for a transaction whose participants are one
/// shard, and is run exclusively on the `SingleShard` / read-only paths.
fn si_conflict_abort(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
    read_set: &[ReadSetEntry],
    written_collections: &std::collections::HashSet<String>,
) -> Option<CommitOutcome> {
    let snapshot_lsn = sessions.snapshot_lsn(session_id)?;
    let current_lsn = state.wal.next_lsn();
    let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
    for entry in read_set {
        let collection = &entry.collection;
        let read_lsn = entry.read_lsn;
        if written_collections.contains(collection) {
            continue;
        }
        if current > read_lsn && current > snapshot_lsn {
            // WAL advanced past what we read — concurrent write detected. The
            // caller releases reservations and rolls the session back.
            super::hot_key::record_read_set_aborts(state, read_set);
            return Some(CommitOutcome::Aborted {
                reason: AbortReason::Serialization,
            });
        }
    }
    None
}

/// Single-shard commit: resolve the transaction's staged post-images into one
/// replayable `TransactionRedo` WAL record, then dispatch the buffered plans as
/// one atomic `TransactionBatch` stamped with that record's LSN. The redo
/// record restores restart durability for in-transaction writes into in-memory
/// secondary indexes (vector HNSW, FTS) that the base storage engine cannot
/// rebuild on its own. Returns `Some(reason)` on failure.
async fn dispatch_single_shard(
    state: &SharedState,
    dp: &impl TxnDataPlane,
    buffered: &[PhysicalTask],
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
) -> Option<AbortReason> {
    let plans: Vec<PhysicalPlan> = buffered.iter().map(|t| t.plan.clone()).collect();
    let database_id = buffered
        .first()
        .map_or(crate::types::DatabaseId::DEFAULT, |task| task.database_id);
    if buffered.iter().any(|task| task.database_id != database_id) {
        return Some(AbortReason::Dispatch(crate::Error::BadRequest {
            detail: "transaction spans multiple databases".to_owned(),
        }));
    }

    // txn_id is present for any staged commit (buffer_write stamps it).
    let Some(txn_id) = buffered.first().and_then(|t| t.txn_id) else {
        return Some(AbortReason::Dispatch(crate::Error::Internal {
            detail: "single-shard commit: buffered task carries no txn_id".into(),
        }));
    };

    // 1. Resolve the transaction's staged post-images into ONE replayable
    //    RedoRecord. Read-only: reads `txn_overlays[txn_id]` on the owning
    //    core, writes nothing.
    let resolve_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan: PhysicalPlan::Meta(MetaOp::ResolveTxn {
            txn_id,
            plans: plans.clone(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let resolve_resp = match dp.dispatch_no_wal(resolve_task, None).await {
        Ok(r) if r.status == Status::Ok => r,
        Ok(r) => {
            return Some(AbortReason::BatchRejected {
                code: r.error_code.as_deref().cloned(),
            });
        }
        Err(e) => return Some(AbortReason::Dispatch(e)),
    };
    let redo = match crate::wal::RedoRecord::from_bytes(resolve_resp.payload.as_bytes()) {
        Ok(r) => r,
        Err(e) => {
            return Some(AbortReason::Dispatch(crate::Error::Internal {
                detail: format!("single-shard commit: resolve redo decode failed: {e}"),
            }));
        }
    };

    // Re-verify local vShard ownership immediately before the durable WAL
    // append. `run_commit` resolved this vShard as `Local`, but a leadership
    // handoff can land during the `ResolveTxn` await above. Without this
    // re-check the transaction redo would be appended to a WAL this node no
    // longer owns, and the batch dispatch below (which re-resolves leadership)
    // would then reject the now-non-local commit — leaving an orphaned durable
    // redo record behind while the client is told the commit aborted. Aborting
    // here, BEFORE any durable write, keeps the failure side-effect-free and
    // retryable: the client's retry re-enters `run_commit`, sees the vShard is
    // non-local, and routes the commit through Calvin's replicated barrier.
    if !matches!(
        crate::control::server::graph_dispatch::cluster_resolve::resolve_for_vshard(
            state,
            vshard_id.as_u32(),
        ),
        RouteDecision::Local
    ) {
        return Some(AbortReason::Serialization);
    }

    // 2. Write-ahead the transaction as ONE replayable `TransactionRedo` record
    //    (each sub-op keeps its real engine `record_type`). `None` when the txn
    //    has no durable writes (all reads / CRDT / text). Its LSN stamps the
    //    batch install so the Data Plane records the committed write version for
    //    every key in the batch.
    let wal_lsn = if redo.ops.is_empty() {
        None
    } else {
        match state
            .wal
            .append_transaction_redo(tenant_id, vshard_id, database_id, &redo)
        {
            Ok(lsn) => Some(lsn),
            Err(e) => {
                return Some(AbortReason::Dispatch(crate::Error::Internal {
                    detail: format!("single-shard commit: transaction redo WAL append failed: {e}"),
                }));
            }
        }
    };
    let batch_task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan: PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans,
            // Reuse the resolve-time bitemporal stamps recorded in this
            // transaction's staging overlay so a `bitemporal=true` document put
            // installs on the same version key the redo (WAL-appended just
            // above) carries — otherwise a normal restart writes a second
            // version of the row.
            txn_id: Some(txn_id),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    classify_batch_dispatch(dp.dispatch_no_wal(batch_task, wal_lsn).await)
}

/// Convert a transaction-batch dispatch result into a commit abort reason, if
/// any. `dispatch_no_wal` returns `Ok(Response { status: Error, .. })` for a
/// failed batch rather than a Rust `Err` — the status must be checked
/// explicitly or a failed sub-plan reports as COMMIT success.
pub(super) fn classify_batch_dispatch(result: crate::Result<Response>) -> Option<AbortReason> {
    match result {
        Err(e) => {
            tracing::warn!(error = %e, "transaction batch dispatch failed");
            Some(AbortReason::Dispatch(e))
        }
        Ok(resp) if resp.status != Status::Ok => Some(AbortReason::BatchRejected {
            code: resp.error_code.as_deref().cloned(),
        }),
        Ok(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::KvOp;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::WalManager;

    use super::*;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, dir)
    }

    fn enable_metering(state: &mut Arc<SharedState>) {
        Arc::get_mut(state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
    }

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            1,
            "regular-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    fn buffered_task(plan: PhysicalPlan) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::from_collection_in_database(DatabaseId::DEFAULT, "widgets"),
            database_id: DatabaseId::DEFAULT,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// `KvOp::Put` is on `is_stageable_write`'s allow-list — a real
    /// `Staged` route would already have billed it at STATEMENT time
    /// (`staging_gate::stage_write`), so a task shaped like this must be
    /// skipped here or COMMIT would double-bill it.
    fn stageable_task() -> PhysicalTask {
        buffered_task(PhysicalPlan::Kv(KvOp::Put {
            collection: "widgets".into(),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        }))
    }

    /// `KvOp::Get` is not on `is_stageable_write`'s allow-list, so this
    /// stands in for the non-stageable ("Buffered") route's shape — the one
    /// `meter_committed_buffered_writes` must bill.
    fn non_stageable_task() -> PhysicalTask {
        buffered_task(PhysicalPlan::Kv(KvOp::Get {
            collection: "widgets".into(),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        }))
    }

    #[test]
    fn meters_only_non_stageable_tasks() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = identity();
        let buffered = vec![stageable_task(), non_stageable_task()];

        meter_committed_buffered_writes(&state, &identity, &buffered);

        let events = state.usage_counter.drain();
        assert_eq!(
            events.len(),
            1,
            "the stageable task was already billed at statement time and must be skipped here"
        );
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
    }

    #[test]
    fn records_nothing_when_metering_disabled() {
        let (state, _dir) = test_state();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let identity = identity();
        let buffered = vec![non_stageable_task()];

        meter_committed_buffered_writes(&state, &identity, &buffered);

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn records_nothing_for_an_empty_buffer() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = identity();

        meter_committed_buffered_writes(&state, &identity, &[]);

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }
}
