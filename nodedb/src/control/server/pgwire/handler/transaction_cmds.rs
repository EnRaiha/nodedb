// SPDX-License-Identifier: BUSL-1.1

//! Transaction command handlers: BEGIN, COMMIT, ROLLBACK, SAVEPOINT.
//!
//! Extracted from `sql_exec.rs` — handles all transactional state management
//! including snapshot isolation conflict detection, WAL transaction batching,
//! GAP_FREE sequence reservation lifecycle, and deferred offset commits.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::calvin::{
    DispatchClass, DispatchOutcome, classify_dispatch, dispatch_calvin_or_fast,
};
use crate::control::security::identity::AuthenticatedIdentity;
use nodedb_cluster::calvin::{AttemptOutcome, TxnId};

use super::core::NodeDbPgHandler;
use crate::control::server::pgwire::types::error_to_sqlstate;

/// Builds the canonical SQLSTATE 57014 error emitted when a Calvin coordinator
/// channel is closed (coordinator task dropped due to deadline expiry).  Both
/// the assignment-recv and completion-recv arms use this constructor so the
/// mapping is defined exactly once and the tests exercise the production path.
fn calvin_cancelled_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "57014".to_owned(),
        "Calvin coordinator cancelled (deadline exceeded)".to_owned(),
    )))
}

/// Converts a batch-dispatch result into a COMMIT-time error, if any.
/// `dispatch_task_no_wal` returns `Ok(Response { status: Error, .. })` for a
/// failed batch rather than a Rust `Err` — callers must check `status`
/// explicitly or a failed sub-plan reports as COMMIT success.
fn batch_dispatch_to_commit_error(
    result: crate::Result<crate::bridge::envelope::Response>,
) -> Result<(), PgWireError> {
    match result {
        Err(e) => {
            tracing::warn!(error = %e, "transaction batch dispatch failed");
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "40001".to_owned(),
                format!("transaction commit failed: {e}"),
            ))))
        }
        Ok(resp) if resp.status != crate::bridge::envelope::Status::Ok => {
            let code = resp.error_code.clone().unwrap_or(
                crate::bridge::envelope::ErrorCode::RejectedPrevalidation {
                    reason: "transaction commit failed".to_owned(),
                },
            );
            let (severity, sqlstate, message) =
                crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code);
            tracing::warn!(
                sqlstate = sqlstate,
                message = %message,
                "transaction batch reported error status"
            );
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                sqlstate.to_owned(),
                message,
            ))))
        }
        Ok(_) => Ok(()),
    }
}

impl NodeDbPgHandler {
    /// Release the per-transaction staging overlay on the vShard that hosted
    /// the transaction's staged point writes.
    ///
    /// Best-effort cleanup dispatched AFTER the durable resolution (COMMIT
    /// batch flush / ROLLBACK). A failure here leaks in-memory overlay state
    /// on that core but does not affect the already-resolved transaction, so it
    /// is logged rather than surfaced to the client.
    async fn dispatch_drop_txn_overlay(
        &self,
        tenant_id: crate::types::TenantId,
        vshard_id: crate::types::VShardId,
        txn_id: crate::types::TxnId,
    ) {
        let task = nodedb_physical::physical_task::PhysicalTask {
            tenant_id,
            vshard_id,
            database_id: crate::types::DatabaseId::DEFAULT,
            plan: crate::bridge::envelope::PhysicalPlan::Meta(
                nodedb_physical::physical_plan::MetaOp::DropTxnOverlay { txn_id },
            ),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        if let Err(e) = self.dispatch_task_no_wal(task, None).await {
            tracing::warn!(error = %e, "failed to drop per-transaction staging overlay");
        }
    }

    /// Handle BEGIN / START TRANSACTION.
    pub(super) fn handle_begin(&self, addr: &std::net::SocketAddr) -> PgWireResult<Vec<Response>> {
        let snapshot_lsn = {
            let next = self.state.wal.next_lsn();
            crate::types::Lsn::new(next.as_u64().saturating_sub(1))
        };
        crate::control::server::shared::session::ddl_buffer::activate();
        self.sessions.begin(addr, snapshot_lsn).map_err(|msg| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                msg.to_owned(),
            )))
        })?;
        Ok(vec![Response::Execution(Tag::new("BEGIN"))])
    }

    /// Handle COMMIT / END / END TRANSACTION.
    ///
    /// Performs snapshot isolation conflict detection, WAL transaction batching,
    /// GAP_FREE sequence finalization, and deferred offset commits.
    pub(super) async fn handle_commit(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        // Snapshot isolation: check for write conflicts before committing.
        let read_set = self.sessions.take_read_set(addr);
        // Collections this transaction wrote itself. A read of a collection the
        // same transaction has written is a read-your-own-write, not a
        // serialization conflict — reading uncommitted own state (served from
        // the staging overlay, which reports no watermark) must not abort the
        // commit. The read-set is collection-granular, so exclusion is too.
        let written_collections = self.sessions.buffered_collections(addr, |plan| {
            super::plan::extract_collection(plan).map(String::from)
        });
        if let Some(snapshot_lsn) = self.sessions.snapshot_lsn(addr) {
            let current_lsn = self.state.wal.next_lsn();
            let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
            for (_collection, _doc_id, read_lsn) in &read_set {
                if written_collections.contains(_collection) {
                    continue;
                }
                if current > *read_lsn && current > snapshot_lsn {
                    // WAL advanced past what we read — concurrent write detected.
                    if let Ok(reservations) = self.sessions.rollback(addr) {
                        for handle in &reservations {
                            let key = handle.sequence_key.clone();
                            let registry = &self.state.sequence_registry;
                            registry.gap_free_manager().rollback(handle, || {
                                let map = registry.sequences_read();
                                if let Some(h) = map.get(&key) {
                                    h.rollback_one();
                                }
                            });
                        }
                    }
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "40001".to_owned(),
                        "could not serialize access due to concurrent update".to_owned(),
                    ))));
                }
            }
        }

        let buffered = self.sessions.commit(addr).map_err(|msg| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25000".to_owned(),
                msg.to_owned(),
            )))
        })?;

        if !buffered.is_empty() {
            let tenant_id = identity.tenant_id;

            match classify_dispatch(&buffered) {
                DispatchClass::SingleShard { vshard: vshard_id } => {
                    // Single-shard path: WAL + TransactionBatch dispatch.
                    let mut sub_records: Vec<(u16, Vec<u8>)> = Vec::with_capacity(buffered.len());
                    for task in &buffered {
                        if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                            task.tenant_id,
                            task.vshard_id,
                            &task.plan,
                        ) {
                            let bytes = entry.to_bytes();
                            sub_records.push((nodedb_wal::record::RecordType::Put as u16, bytes));
                        }
                    }

                    if !sub_records.is_empty() {
                        let tx_payload = zerompk::to_msgpack_vec(&sub_records).map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "XX000".to_owned(),
                                format!("transaction WAL serialization failed: {e}"),
                            )))
                        })?;
                        self.state
                            .wal
                            .append_transaction(
                                tenant_id,
                                vshard_id,
                                crate::types::DatabaseId::DEFAULT,
                                &tx_payload,
                            )
                            .map_err(|e| {
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "XX000".to_owned(),
                                    format!("transaction WAL append failed: {e}"),
                                )))
                            })?;
                    }

                    let plans: Vec<crate::bridge::envelope::PhysicalPlan> =
                        buffered.iter().map(|t| t.plan.clone()).collect();
                    let batch_task = nodedb_physical::physical_task::PhysicalTask {
                        tenant_id,
                        vshard_id,
                        database_id: crate::types::DatabaseId::DEFAULT,
                        plan: crate::bridge::envelope::PhysicalPlan::Meta(
                            nodedb_physical::physical_plan::MetaOp::TransactionBatch { plans },
                        ),
                        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
                        txn_id: None,
                    };
                    batch_dispatch_to_commit_error(
                        self.dispatch_task_no_wal(batch_task, None).await,
                    )?;
                }
                DispatchClass::MultiShard { .. } => {
                    // Multi-shard path: route through the Calvin sequencer.
                    let cross_shard_mode = self.sessions.cross_shard_txn_mode(addr);
                    let tx_state = self.sessions.transaction_state(addr);

                    let inbox = self.state.sequencer_inbox.get();
                    let orchestrator = self.state.ollp_orchestrator.get();
                    let registry =
                        self.state.calvin_completion_registry.get().ok_or_else(|| {
                            let (severity, code, message) =
                                error_to_sqlstate(&crate::Error::SequencerUnavailable);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?;

                    let dispatch = dispatch_calvin_or_fast(
                        &buffered,
                        cross_shard_mode,
                        tx_state,
                        inbox,
                        orchestrator,
                        tenant_id,
                    )
                    .await
                    .map_err(|e| {
                        let (severity, code, message) = error_to_sqlstate(&e);
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        )))
                    })?;

                    match dispatch {
                        DispatchOutcome::CalvinStatic { inbox_seq }
                        | DispatchOutcome::CalvinDependent { inbox_seq } => {
                            let timeout = std::time::Duration::from_secs(
                                self.state.tuning.network.default_deadline_secs,
                            );
                            let assignment_rx = registry.register_submission(inbox_seq);
                            let (epoch, position, participants) =
                                tokio::time::timeout(timeout, assignment_rx)
                                    .await
                                    .map_err(|_| {
                                        PgWireError::UserError(Box::new(ErrorInfo::new(
                                            "ERROR".to_owned(),
                                            "57014".to_owned(),
                                            "timed out waiting for Calvin sequencer assignment"
                                                .to_owned(),
                                        )))
                                    })?
                                    .map_err(|_| calvin_cancelled_error())?;

                            let completion_rx = registry
                                .register_completion(TxnId::new(epoch, position), participants);
                            let outcome = tokio::time::timeout(timeout, completion_rx)
                                .await
                                .map_err(|_| {
                                    PgWireError::UserError(Box::new(ErrorInfo::new(
                                        "ERROR".to_owned(),
                                        "57014".to_owned(),
                                        "timed out waiting for Calvin transaction completion"
                                            .to_owned(),
                                    )))
                                })?
                                .map_err(|_| calvin_cancelled_error())?;
                            // The static (non-dependent) Calvin path never
                            // produces an OLLP mismatch — `note_ollp_mismatch`
                            // only fires on the dependent-predicate retry path —
                            // so this branch is unreachable at runtime today. It
                            // is kept as a typed error (never a panic) so any
                            // future mismatch signal surfaces deterministically
                            // instead of crashing.
                            if outcome == AttemptOutcome::Mismatch {
                                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                                    "ERROR".to_owned(),
                                    "XX000".to_owned(),
                                    "OLLP mismatch outcome on non-dependent Calvin path".to_owned(),
                                ))));
                            }
                        }
                        DispatchOutcome::SingleShard | DispatchOutcome::BestEffortNonAtomic => {
                            // BestEffortNonAtomic: dispatch each vshard's sub-batch independently.
                            // Group buffered tasks by vshard and dispatch per-vshard TransactionBatches.
                            let mut by_vshard: std::collections::BTreeMap<
                                u32,
                                Vec<crate::bridge::envelope::PhysicalPlan>,
                            > = std::collections::BTreeMap::new();
                            for task in &buffered {
                                by_vshard
                                    .entry(task.vshard_id.as_u32())
                                    .or_default()
                                    .push(task.plan.clone());
                            }
                            for (vshard_u32, plans) in by_vshard {
                                let vshard_id = nodedb_types::id::VShardId::new(vshard_u32);
                                let batch_task = nodedb_physical::physical_task::PhysicalTask {
                                    tenant_id,
                                    vshard_id,
                                    database_id: crate::types::DatabaseId::DEFAULT,
                                    plan: crate::bridge::envelope::PhysicalPlan::Meta(
                                        nodedb_physical::physical_plan::MetaOp::TransactionBatch {
                                            plans,
                                        },
                                    ),
                                    post_set_op: nodedb_physical::physical_task::PostSetOp::None,
                                    txn_id: None,
                                };
                                batch_dispatch_to_commit_error(
                                    self.dispatch_task_no_wal(batch_task, None).await,
                                )?;
                            }
                        }
                    }
                }
            }

            // Release the per-transaction staging overlay on every vShard that
            // hosted a staged write, now that the durable batch(es) have
            // flushed. Guarded on a staged (txn_id-carrying) buffer.
            if let Some(txn_id) = buffered[0].txn_id {
                let mut dropped = std::collections::HashSet::new();
                for task in &buffered {
                    if dropped.insert(task.vshard_id) {
                        self.dispatch_drop_txn_overlay(tenant_id, task.vshard_id, txn_id)
                            .await;
                    }
                }
            }
        }

        // Flush pending offset commits (deferred from COMMIT OFFSET inside transaction).
        let pending_offsets = self.sessions.take_pending_offsets(addr);
        for (tid, stream, group, partition_id, lsn) in pending_offsets {
            if let Err(e) =
                self.state
                    .offset_store
                    .commit_offset(tid, &stream, &group, partition_id, lsn)
            {
                tracing::warn!(
                    stream = %stream,
                    group = %group,
                    partition = partition_id,
                    error = %e,
                    "failed to commit deferred offset"
                );
            }
        }

        // Finalize GAP_FREE reservations (numbers become permanent).
        let reservations = self.sessions.take_pending_reservations(addr);
        for handle in &reservations {
            self.state
                .sequence_registry
                .gap_free_manager()
                .commit(handle);
            // Log to _system.sequence_log.
            if let Some(catalog) = self.state.credentials.catalog() {
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
        if let Some(payloads) = crate::control::server::shared::session::ddl_buffer::take()
            && !payloads.is_empty()
        {
            use nodedb_cluster::{MetadataEntry, encode_entry};
            // Each buffered entry carries the audit context captured
            // at its own statement boundary (not COMMIT time). Map to
            // `CatalogDdlAudited` when present so every sub-DDL gets
            // its own audit record on every replica.
            let sub_entries: Vec<MetadataEntry> = payloads
                .into_iter()
                .map(|e| match e.audit {
                    Some(ctx) => MetadataEntry::CatalogDdlAudited {
                        payload: e.payload,
                        auth_user_id: ctx.auth_user_id,
                        auth_user_name: ctx.auth_user_name,
                        sql_text: ctx.sql_text,
                    },
                    None => MetadataEntry::CatalogDdl { payload: e.payload },
                })
                .collect();
            let batch = MetadataEntry::Batch {
                entries: sub_entries,
            };
            if let Some(handle) = self.state.metadata_raft.get() {
                let raw = encode_entry(&batch).map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("DDL batch encode: {e}"),
                    )))
                })?;
                handle.propose(raw).map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("DDL batch propose: {e}"),
                    )))
                })?;
            }
        }
        // Close non-WITH-HOLD cursors on transaction end.
        self.sessions.close_non_hold_cursors(addr);
        // Flush NOTIFY messages buffered during this transaction.
        self.sessions
            .flush_pending_notifies(addr, identity.tenant_id, &self.state.notify_bus);
        Ok(vec![Response::Execution(Tag::new("COMMIT"))])
    }

    /// Handle ROLLBACK / ABORT.
    pub(super) async fn handle_rollback(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        crate::control::server::shared::session::ddl_buffer::discard();
        // Snapshot the overlay identity BEFORE `rollback()` clears session
        // state, so the staging overlay can be released on its home vShard.
        let (overlay_txn_id, overlay_vshard) = self.sessions.txn_identity(addr);
        let reservations = self.sessions.rollback(addr).unwrap_or_default();
        for handle in &reservations {
            let key = &handle.sequence_key;
            let registry = &self.state.sequence_registry;
            registry.gap_free_manager().rollback(handle, || {
                let map = registry.sequences_read();
                if let Some(h) = map.get(key.as_str()) {
                    h.rollback_one();
                }
            });
            if let Some(catalog) = self.state.credentials.catalog() {
                crate::control::sequence::log::log_reservation(
                    catalog,
                    &crate::control::sequence::log::rolled_back(
                        key,
                        handle.value,
                        &identity.username,
                        identity.tenant_id.as_u64(),
                    ),
                );
            }
        }
        self.sessions.close_non_hold_cursors(addr);
        // Discard NOTIFY messages buffered during this transaction.
        self.sessions.discard_pending_notifies(addr);

        // Release any staging overlay populated by statement-time point writes.
        if let (Some(txn_id), Some(vshard_id)) = (overlay_txn_id, overlay_vshard) {
            self.dispatch_drop_txn_overlay(identity.tenant_id, vshard_id, txn_id)
                .await;
        }
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }
}

#[cfg(test)]
mod tests {
    use super::calvin_cancelled_error;
    use pgwire::error::{ErrorInfo, PgWireError};

    #[test]
    fn calvin_assignment_channel_closed_returns_57014() {
        // Exercises the production constructor directly — no replicated mapping.
        let err = calvin_cancelled_error();
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(
                    info.code, "57014",
                    "expected SQLSTATE 57014 (query_canceled) for Calvin channel-closed, got {}",
                    info.code
                );
                assert_ne!(
                    info.code, "XX000",
                    "must not surface XX000 (internal_error) for a coordinator deadline cancel"
                );
            }
            other => panic!("expected PgWireError::UserError, got {other:?}"),
        }
    }

    #[test]
    fn calvin_completion_channel_closed_returns_57014() {
        // Completion arm uses the same production constructor — assert it here too.
        let err = calvin_cancelled_error();
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(
                    info.code, "57014",
                    "expected SQLSTATE 57014 for Calvin completion channel-closed, got {}",
                    info.code
                );
            }
            other => panic!("expected PgWireError::UserError, got {other:?}"),
        }
    }

    #[test]
    fn ollp_mismatch_is_not_57014() {
        // Verify the OLLP mismatch arm (a genuine invariant violation) stays
        // XX000 — a distinct code path from coordinator deadline cancellation.
        let err = PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            "OLLP mismatch outcome on non-dependent Calvin path".to_owned(),
        )));
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(
                    info.code, "XX000",
                    "OLLP mismatch must stay XX000 (internal_error)"
                );
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
