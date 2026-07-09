// SPDX-License-Identifier: BUSL-1.1

//! Verdict-driven commit resolution for staged static Calvin transactions.
//!
//! A static Calvin dispatch STAGES its transaction on the Data Plane (validate
//! the read-set + buffer the plans, no base mutation). Its executor response
//! carries the local commit vote on `read_set_valid`. This module drives the
//! second step: dispatch a flush (commit) or drop (abort) of the staged buffer,
//! wait for its response, then run the commit tail (deposit applied result,
//! append `CalvinApplied` WAL + record write versions, propose `CompletionAck`)
//! for a flush, or ack-only for a drop.

use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_cluster::calvin::SequencerEntry;

use super::super::types::CommitState;
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;
use crate::types::{DatabaseId, ReadConsistency, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

impl Scheduler {
    /// Resolve a staged transaction's local commit vote into a flush or drop.
    ///
    /// The staged executor response is validate-only: its `read_set_valid` is
    /// the local commit vote (`Some(true)` => commit, `Some(false)` => abort; a
    /// defensive `None` is treated as commit). Dispatches the corresponding
    /// flush/drop back to the same core, moves the txn to
    /// [`CommitState::AwaitingResolve`], and bumps the flushed / dropped
    /// counter. The commit tail runs later, in [`Self::finish_resolved_commit`],
    /// once the flush/drop response arrives.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resolve_staged_commit(
        &mut self,
        txn_id: TxnId,
        staged_response: &Response,
    ) {
        let committed = staged_response.read_set_valid != Some(false);
        if !committed {
            // The staged slice's read-set was no longer current: observe it, the
            // same node-global signal the direct-apply path records.
            self.shared
                .calvin_counters
                .read_set_validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }

        if !self.dispatch_commit_resolution(txn_id, committed) {
            // Flush/drop dispatch failed: complete the txn as an infra error so
            // its locks release and the epoch advances rather than stalling. The
            // staged buffer is reclaimed by a later drop or on core teardown.
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        }

        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.commit_state = Some(CommitState::AwaitingResolve { committed });
        }

        if committed {
            self.shared
                .calvin_counters
                .commits_flushed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.shared
                .calvin_counters
                .commits_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Run the commit tail once a flush/drop response has returned.
    ///
    /// On a successful flush the full commit tail runs (deposit applied result,
    /// `CalvinApplied` WAL + write-version recording, `CompletionAck`). On a
    /// successful drop only the `CompletionAck` is proposed — the coordinator's
    /// completion waiter still fires and the epoch advances, but nothing was
    /// written so there is no result to deposit, no apply LSN, and no versions
    /// to record. A non-`Ok` resolve response is treated as an executor error.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_resolved_commit(
        &mut self,
        txn_id: TxnId,
        response: Response,
        committed: bool,
    ) {
        if response.status == Status::Ok {
            if committed {
                self.commit_apply_tail(txn_id, response);
            } else {
                self.propose_sequencer_entry(
                    SequencerEntry::CompletionAck {
                        epoch: txn_id.epoch,
                        position: txn_id.position,
                        vshard_id: self.vshard_id,
                    },
                    txn_id,
                    "completion ack (dropped)",
                );
            }
        } else {
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                committed,
                "calvin: flush/drop response was not Ok; locks NOT released (shard degraded)"
            );
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
        }

        self.metrics.record_completed();
        self.on_txn_complete(txn_id);
    }

    /// Deposit the applied result, append the `CalvinApplied` WAL record +
    /// record the apply's write versions, and propose the `CompletionAck`.
    ///
    /// Shared by the flush-completion path and the direct-apply (dependent /
    /// active) apply path.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn commit_apply_tail(
        &mut self,
        txn_id: TxnId,
        response: Response,
    ) {
        // Deposit the FULL applied Response (affected-count + watermark + any
        // RETURNING rows) into the local sidecar BEFORE proposing the replicated
        // CompletionAck. The ack fires the coordinator's completion oneshot on
        // every sequencer member, so depositing first guarantees the result is
        // present by the time the coordinator drains it — no lost result, no
        // race.
        //
        // Gated on the PRIMARY-WRITE participant: the sole participant whose
        // slice carries the user's non-edge DML (Document/KV/Vector/etc.), as
        // opposed to the implicit graph-edge cleanup that dual-homes alongside
        // it. Exactly one participant carries the primary write for a
        // single-collection user DML (+ its edges), so the edge participants
        // never clobber the entry; the `CalvinApplyResult::{Single,Conflict}`
        // guard stays as belt-and-suspenders. Results travel via this in-process
        // sidecar only — never the sequencer Raft log.
        let has_primary_write = self
            .pending
            .get(&txn_id)
            .map(|p| p.has_primary_write)
            .unwrap_or(false);
        if has_primary_write {
            use std::collections::hash_map::Entry;

            use crate::control::state::CalvinApplyResult;

            let key = nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position);
            let mut results = self
                .shared
                .calvin_apply_results
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match results.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert(CalvinApplyResult::Single(response));
                }
                Entry::Occupied(mut slot) => {
                    // A second RETURNING-bearing participant for one Calvin txn
                    // means a cross-shard RETURNING union, which is unsupported.
                    // Record Conflict so the coordinator fails the statement
                    // loudly rather than returning one shard's rows. Unreachable
                    // under collection-level sharding today.
                    tracing::error!(
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        vshard = self.vshard_id,
                        "multiple RETURNING participants for one Calvin txn — cross-shard \
                         RETURNING union unsupported"
                    );
                    slot.insert(CalvinApplyResult::Conflict);
                }
            }
        }
        match self.shared.wal.append_calvin_applied(
            crate::types::VShardId::new(self.vshard_id),
            txn_id.epoch,
            txn_id.position,
        ) {
            // The CalvinApplied WAL LSN is the committed write-LSN for this apply
            // — the SAME shard-local WAL-LSN space fast-path writes and read
            // watermarks use. Record the apply's per-key write versions at it now
            // that it exists (it did not at dispatch time).
            Ok(applied_lsn) => self.record_calvin_write_versions(txn_id, applied_lsn),
            Err(e) => {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    error = %e,
                    "calvin: failed to write CalvinApplied WAL record"
                );
            }
        }
        self.propose_sequencer_entry(
            SequencerEntry::CompletionAck {
                epoch: txn_id.epoch,
                position: txn_id.position,
                vshard_id: self.vshard_id,
            },
            txn_id,
            "completion ack",
        );
    }

    /// Dispatch a flush (`committed = true`) or drop (`committed = false`) of a
    /// staged transaction's commit-pending buffer back to its core, registering
    /// a response bridge so the resolve response re-enters the completion loop.
    ///
    /// Returns `false` if the dispatch failed (the caller then completes the txn
    /// as an infra error). Mirrors the exempt, no-WAL-LSN dispatch shape of the
    /// static/active dispatch and the write-version record op.
    fn dispatch_commit_resolution(&mut self, txn_id: TxnId, committed: bool) -> bool {
        let Some(pending) = self.pending.get(&txn_id) else {
            return false;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let epoch = txn_id.epoch;
        let position = txn_id.position;

        let plan = if committed {
            PhysicalPlan::Meta(MetaOp::CalvinFlush { epoch, position })
        } else {
            PhysicalPlan::Meta(MetaOp::CalvinDrop { epoch, position })
        };

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
            plan,
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
            // A flush allocates its CalvinApplied WAL LSN post-apply (in
            // `commit_apply_tail`), so no committed LSN is known here.
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
                committed,
                error = %e,
                "calvin: flush/drop dispatch failed"
            );
            return false;
        }

        // The resolve response re-enters the completion loop under the SAME
        // txn_id, now in `AwaitingResolve`, where `finish_resolved_commit` runs.
        self.spawn_response_bridge(txn_id, request_id, resp_rx);
        true
    }
}
