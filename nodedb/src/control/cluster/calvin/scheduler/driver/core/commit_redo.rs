// SPDX-License-Identifier: BUSL-1.1

//! Redo-record resolution for a committed staged static Calvin transaction.
//!
//! A committed static Calvin dispatch stages its transaction on the Data
//! Plane (validate the read-set + buffer the plans, no base mutation). Once
//! the local commit vote is known (`resolve_staged_commit`), this module
//! drives the resolve step: dispatch `MetaOp::CalvinResolve` to reconstitute
//! the staged post-images as one replayable `RedoRecord`, WAL-append that
//! record (restoring restart durability for this vShard's slice of the
//! commit), then hand off to `dispatch_commit_resolution` for the flush that
//! `finish_resolved_commit` / `commit_apply_tail` complete.

use std::time::Instant;

use super::super::types::CommitState;
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;
use crate::types::{DatabaseId, ReadConsistency, VShardId};
use crate::wal::{CalvinStamp, RedoRecord};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

impl Scheduler {
    /// Handle the `MetaOp::CalvinResolve` response: decode the resolved
    /// `RedoRecord`, WAL-append it (unless its op set is empty), then dispatch
    /// the flush stamped with that record's LSN.
    ///
    /// A non-`Ok` response, a decode failure, or a WAL-append failure is a
    /// loud infra abort — never a silent fall-through to a non-durable flush.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_redo_resolve(
        &mut self,
        txn_id: TxnId,
        response: Response,
    ) {
        if response.status != Status::Ok {
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: CalvinResolve response was not Ok; locks NOT released (shard degraded)"
            );
            self.abort_redo_resolve_infra_error(txn_id);
            return;
        }

        let mut redo = match RedoRecord::from_bytes(response.payload.as_bytes()) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    error = %e,
                    "calvin: CalvinResolve redo record decode failed"
                );
                self.abort_redo_resolve_infra_error(txn_id);
                return;
            }
        };
        redo.calvin_stamp = Some(CalvinStamp {
            epoch: txn_id.epoch,
            position: txn_id.position,
            vshard_id: self.vshard_id,
        });

        let Some(pending) = self.pending.get(&txn_id) else {
            // Txn state was reclaimed out from under us (should not happen —
            // locks are held until `on_txn_complete`); complete defensively.
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;

        let redo_lsn = if redo.ops.is_empty() {
            None
        } else {
            match self.shared.wal.append_transaction_redo(
                tenant_id,
                VShardId::new(self.vshard_id),
                DatabaseId::DEFAULT,
                &redo,
            ) {
                Ok(lsn) => Some(lsn),
                Err(e) => {
                    tracing::error!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        error = %e,
                        "calvin: TransactionRedo WAL append failed"
                    );
                    self.abort_redo_resolve_infra_error(txn_id);
                    return;
                }
            }
        };

        if !self.dispatch_commit_resolution(txn_id, true, redo_lsn) {
            // `dispatch_commit_resolution` already logged the dispatch failure.
            self.abort_redo_resolve_infra_error(txn_id);
            return;
        }

        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = Some(CommitState::AwaitingResolve {
                committed: true,
                redo_lsn,
            });
        }
    }

    /// Complete `txn_id` as an infra error: releases its locks so the epoch
    /// advances rather than stalling. Shared by every `finish_redo_resolve`
    /// failure branch.
    fn abort_redo_resolve_infra_error(&mut self, txn_id: TxnId) {
        self.metrics.record_executor_error();
        self.metrics
            .record_infra_abort(infra_abort_reason::IO_ERROR);
        self.metrics.record_completed();
        self.on_txn_complete(txn_id);
    }

    /// Dispatch `MetaOp::CalvinResolve` to this vShard's core, registering a
    /// response bridge so the resolve response re-enters the completion loop
    /// under `CommitState::AwaitingRedoResolve`.
    ///
    /// Mirrors `dispatch_commit_resolution`'s exempt, no-WAL-LSN dispatch
    /// shape — a resolve reads the staged overlay and writes nothing.
    ///
    /// Returns `false` if the dispatch failed (the caller then completes the
    /// txn as an infra error).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_calvin_resolve(
        &mut self,
        txn_id: TxnId,
    ) -> bool {
        let Some(pending) = self.pending.get(&txn_id) else {
            return false;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let epoch = txn_id.epoch;
        let position = txn_id.position;

        let request_id = self.next_request_id();
        // no-determinism: request deadline is ephemeral, not written to WAL
        let deadline = Instant::now()
            + std::time::Duration::from_millis(
                self.config.epoch_duration_ms * u64::from(self.config.txn_deadline_multiplier),
            );
        let request = Request {
            request_id,
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(self.vshard_id),
            plan: PhysicalPlan::Meta(MetaOp::CalvinResolve { epoch, position }),
            deadline,
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId([0u8; 16]),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            // A resolve reads the staged overlay only; it writes no WAL record
            // itself, so no committed LSN rides on this envelope.
            wal_lsn: None,
            // Calvin resolves TTL instants via `epoch_system_ms`, not this
            // field — see `resolved_now_ms` precedence in the KV write handlers.
            resolved_now_ms: None,
            // The scheduler already holds this transaction's locks, so the
            // write-admission gate must not re-acquire — Exempt.
            admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
        };

        let resp_rx = self.shared.tracker.register(request_id);
        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };
        if let Err(e) = dispatch_result {
            self.shared.tracker.cancel(&request_id);
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin: CalvinResolve dispatch failed"
            );
            return false;
        }

        // The resolve response re-enters the completion loop under the SAME
        // txn_id, now in `AwaitingRedoResolve`, where `finish_redo_resolve` runs.
        self.spawn_response_bridge(txn_id, request_id, resp_rx);
        true
    }
}
