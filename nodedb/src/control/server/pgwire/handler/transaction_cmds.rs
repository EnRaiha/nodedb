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

impl NodeDbPgHandler {
    /// Handle BEGIN / START TRANSACTION.
    pub(super) fn handle_begin(&self, addr: &std::net::SocketAddr) -> PgWireResult<Vec<Response>> {
        let snapshot_lsn = {
            let next = self.state.wal.next_lsn();
            crate::types::Lsn::new(next.as_u64().saturating_sub(1))
        };
        crate::control::server::pgwire::session::ddl_buffer::activate();
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
        if let Some(snapshot_lsn) = self.sessions.snapshot_lsn(addr) {
            let current_lsn = self.state.wal.next_lsn();
            let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
            for (_collection, _doc_id, read_lsn) in &read_set {
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
                    };
                    if let Err(e) = self.dispatch_task_no_wal(batch_task, None).await {
                        tracing::warn!(error = %e, "transaction batch dispatch failed");
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "40001".to_owned(),
                            format!("transaction commit failed: {e}"),
                        ))));
                    }
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
                                    .map_err(|_| {
                                        PgWireError::UserError(Box::new(ErrorInfo::new(
                                            "ERROR".to_owned(),
                                            "XX000".to_owned(),
                                            "Calvin sequencer assignment channel closed".to_owned(),
                                        )))
                                    })?;

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
                                .map_err(|_| {
                                    PgWireError::UserError(Box::new(ErrorInfo::new(
                                        "ERROR".to_owned(),
                                        "XX000".to_owned(),
                                        "Calvin completion channel closed".to_owned(),
                                    )))
                                })?;
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
                                };
                                if let Err(e) = self.dispatch_task_no_wal(batch_task, None).await {
                                    tracing::warn!(
                                        error = %e,
                                        "best-effort non-atomic multi-shard batch dispatch failed"
                                    );
                                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                                        "ERROR".to_owned(),
                                        "40001".to_owned(),
                                        format!("transaction commit failed: {e}"),
                                    ))));
                                }
                            }
                        }
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
        if let Some(payloads) = crate::control::server::pgwire::session::ddl_buffer::take()
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
    pub(super) fn handle_rollback(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        crate::control::server::pgwire::session::ddl_buffer::discard();
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
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }
}
