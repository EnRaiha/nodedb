// SPDX-License-Identifier: BUSL-1.1

//! Verdict-driven commit resolution for staged static Calvin transactions.
//!
//! A static Calvin dispatch STAGES its transaction on the Data Plane (validate
//! the read-set + buffer the plans, no base mutation). Its executor response
//! carries the local commit vote on `read_set_valid`. This module drives the
//! final step: dispatch a flush (commit, after `commit_redo` has WAL-appended
//! the resolved `TransactionRedo`) or drop (abort) of the staged buffer, wait
//! for its response, then run the commit tail (deposit applied result, record
//! write versions — plus a `CalvinApplied` WAL fallback when no redo record
//! was appended — propose `CompletionAck`) for a flush, or ack-only for a
//! drop.

use std::sync::atomic::Ordering;
use std::time::Instant;

use nodedb_cluster::calvin::{SequencerEntry, VerdictSignal};

use super::super::types::CommitState;
use super::scheduler::Scheduler;
use super::staged_vote::{StagedVote, staged_commit_vote};
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;

impl Scheduler {
    /// Cast this participant's local commit vote for a staged transaction, then
    /// PARK it on the cross-shard commit barrier awaiting the durable GLOBAL
    /// verdict — it does NOT self-decide flush-or-drop on its local vote.
    ///
    /// The staged executor response is validate-only: its `read_set_valid` is
    /// this shard's local commit vote (`Some(true)` => commit, `Some(false)` =>
    /// abort; a `None` from the active/dependent path is treated as commit). The
    /// leader proposes that vote via the sequencer Raft group; the sequencer
    /// aggregates all participants' votes into a single authoritative
    /// `SequencerEntry::Verdict`, applied on every replica.
    ///
    /// This method moves the txn to [`CommitState::AwaitingVerdict`] WITHOUT
    /// dispatching a resolve or drop, then immediately probes
    /// `registry.verdict(txn)`: if the verdict is already durable (replay, or a
    /// push we raced) it resumes at once via [`Self::resume_on_verdict`];
    /// otherwise it stays parked, holding locks and its staged buffer, until the
    /// verdict push, a later probe, or the stall re-probe sweep delivers the
    /// verdict. Resuming (in `resume_on_verdict`) is where the flush/drop is
    /// dispatched and the flushed/dropped counters bump — using the GLOBAL
    /// verdict, never the local vote.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resolve_staged_commit(
        &mut self,
        txn_id: TxnId,
        staged_response: &Response,
    ) {
        // A staged error is always an abort vote. Only successful staged
        // responses may use `None` for the dependent-read path; accepting an
        // error-plus-None as commit would let a failed participant flush after
        // its peers received a global commit verdict.
        let vote = staged_commit_vote(staged_response);

        // Durably propose this participant's commit vote via the sequencer
        // Raft group, leader-guarded like `OllpMismatch`: only the data-group
        // leader ran read-set validation, so only a leader's vote is
        // authoritative. The sequencer aggregates every participant's vote into
        // the single global verdict this txn parks on below. An abort travels as
        // `AbortVote` so its cause survives to the coordinator.
        if self.is_group_leader() {
            let entry = match vote.abort_reason() {
                Some(reason) => SequencerEntry::AbortVote {
                    epoch: txn_id.epoch,
                    position: txn_id.position,
                    vshard: self.vshard_id,
                    reason,
                },
                None => SequencerEntry::Vote {
                    epoch: txn_id.epoch,
                    position: txn_id.position,
                    vshard: self.vshard_id,
                    commit: true,
                },
            };
            self.propose_sequencer_entry(entry, txn_id, "commit vote");
        }

        if vote == StagedVote::SerializationConflict {
            // The staged slice's read-set was no longer current: observe it, the
            // same node-global signal the direct-apply path records. A
            // participant error never validated a read-set, so it must not count
            // here.
            self.shared
                .calvin_counters
                .read_set_validation_failures
                .fetch_add(1, Ordering::Relaxed);
        }

        // PARK on the barrier: transition to `AwaitingVerdict` and arm the stall
        // deadline. Do NOT dispatch resolve/drop here — the GLOBAL verdict, not
        // this local vote, decides. If the txn already vanished (torn down
        // elsewhere), there is nothing to park.
        match self.pending.get_mut(&txn_id) {
            Some(pending) => {
                pending.commit_state = Some(CommitState::AwaitingVerdict);
                // no-determinism: local stall-warning deadline only; the global replicated verdict, not this wall-clock, decides commit/abort.
                pending.verdict_deadline = Some(Instant::now() + self.config.verdict_stall_warn());
            }
            None => return,
        }

        // PROBE on park (correctness backstop): the verdict may already be
        // durable — on replay, or a push that raced ahead of this park. Resume
        // immediately if so; the double-resume guard in `resume_on_verdict`
        // makes a later duplicate push/probe a no-op.
        if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
            txn_id.epoch,
            txn_id.position,
        )) {
            self.resume_on_verdict(txn_id, verdict);
        }
    }

    /// Resume a txn parked in [`CommitState::AwaitingVerdict`] once the durable
    /// GLOBAL verdict is known: dispatch its flush (commit) or drop (abort).
    ///
    /// `committed` is the authoritative cross-shard verdict — NOT this shard's
    /// local vote. On commit, dispatches `MetaOp::CalvinResolve` and moves the
    /// txn to [`CommitState::AwaitingRedoResolve`] (the resolved redo is
    /// WAL-appended and the flush dispatched from [`Self::finish_redo_resolve`]).
    /// On abort, dispatches the drop directly and moves the txn to
    /// [`CommitState::AwaitingResolve`]. Bumps the flushed / dropped counter. The
    /// commit tail runs later in [`Self::finish_resolved_commit`], once the
    /// flush/drop response arrives.
    ///
    /// Double-resume guard: the verdict push and the probe-on-park (and the
    /// stall re-probe sweep) can all fire for one txn, so this first confirms the
    /// txn is still `Some(AwaitingVerdict)` — if it already transitioned out
    /// (resolve/drop dispatched, or completed), this is a no-op. This guarantees
    /// the flush/drop is dispatched exactly once.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn resume_on_verdict(
        &mut self,
        txn_id: TxnId,
        committed: bool,
    ) {
        // Guard: only a still-parked txn resumes. Mirrors `handle_completion`'s
        // state-match so a duplicate push/probe/timeout is idempotent.
        if !matches!(
            self.pending.get(&txn_id).and_then(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        ) {
            return;
        }

        let dispatched = if committed {
            // Resolve the staged post-images into a replayable `RedoRecord`
            // first; the redo is WAL-appended (in `finish_redo_resolve`) before
            // the flush is dispatched, restoring restart durability for this
            // vShard's slice of a multi-shard Calvin commit.
            self.dispatch_calvin_resolve(txn_id)
        } else {
            self.dispatch_commit_resolution(txn_id, false, None)
        };
        if !dispatched {
            // Resolve/drop dispatch failed: complete the txn as an infra error so
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
            pending.commit_state = Some(if committed {
                CommitState::AwaitingRedoResolve
            } else {
                CommitState::AwaitingResolve {
                    committed: false,
                    redo_lsn: None,
                }
            });
            // No longer parked: clear the stall deadline.
            pending.verdict_deadline = None;
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

    /// Handle a pushed [`VerdictSignal`] from this node's completion registry.
    ///
    /// Matches the signal to the parked txn by `(epoch, position)` and resumes
    /// it. A signal for a txn this scheduler does not host, or one that already
    /// resumed, is a harmless no-op (the double-resume guard covers the latter).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn handle_verdict_signal(
        &mut self,
        signal: VerdictSignal,
    ) {
        let txn_id = TxnId::new(signal.epoch, signal.position);
        self.resume_on_verdict(txn_id, signal.verdict.is_commit());
    }

    /// Sweep parked `AwaitingVerdict` txns whose stall deadline has passed.
    ///
    /// For each stalled txn, RE-PROBE the durable verdict: if it is now known,
    /// resume (a push we dropped on a full channel, or a verdict that landed
    /// after the last probe). If it is STILL unknown, KEEP WAITING — hold locks,
    /// emit a stall metric + warning, and re-arm the deadline so the warning is
    /// rate-limited rather than per-iteration. It NEVER releases locks and NEVER
    /// unilaterally aborts: a participant cannot know whether a peer already
    /// flushed a COMMIT, so aborting one side while a peer committed would tear
    /// the transaction. The verdict is guaranteed to arrive eventually — a
    /// post-failover leader re-aggregates the replicated votes (seeded on every
    /// replica) into the same verdict — so waiting is always the safe action.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn check_awaiting_verdict_stalls(
        &mut self,
    ) {
        // no-determinism: stall-detection clock drives warnings/metrics only; this path holds locks and never aborts, so it cannot affect the replicated outcome.
        let now = Instant::now();
        let stalled: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(_, p)| matches!(p.commit_state, Some(CommitState::AwaitingVerdict)))
            .filter(|(_, p)| p.verdict_deadline.is_some_and(|d| now >= d))
            .map(|(id, _)| *id)
            .collect();

        for txn_id in stalled {
            if let Some(verdict) = self.registry.verdict(nodedb_cluster::calvin::TxnId::new(
                txn_id.epoch,
                txn_id.position,
            )) {
                self.resume_on_verdict(txn_id, verdict);
                continue;
            }

            // Verdict still unknown: keep waiting, hold locks, never abort.
            self.metrics.record_verdict_stall();
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: staged txn still awaiting the cross-shard verdict past its stall \
                 deadline; HOLDING locks and waiting (never aborting — a peer may have already \
                 flushed a commit). The verdict is guaranteed to arrive."
            );
            if let Some(pending) = self.pending.get_mut(&txn_id) {
                pending.verdict_deadline = Some(now + self.config.verdict_stall_warn());
            }
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
        redo_lsn: Option<crate::types::Lsn>,
    ) {
        let completed = if response.status == Status::Ok {
            if committed {
                self.commit_apply_tail(txn_id, response, redo_lsn)
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
                true
            }
        } else {
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                committed,
                "calvin: flush/drop response was not Ok while applying an already-committed \
                 verdict; forcing infra-abort completion so locks release and the epoch advances"
            );
            false
        };

        if completed {
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
        } else {
            // The cross-shard verdict is already globally durable, and a commit's
            // resolved redo was WAL-appended before this flush — so recovery
            // re-applies the write. A local flush/apply or WAL-marker failure is
            // therefore an infrastructure event, NOT an outcome change. It must
            // never leave the txn parked: holding its locks forever wedges every
            // txn queued behind those keys and freezes this vShard's epoch
            // watermark (which anchors cross-shard BEGIN snapshots), and nothing
            // re-drives a non-`AwaitingVerdict` pending entry. Surface the infra
            // abort and force completion — the same forward-progress contract the
            // resolve/drop dispatch-failure path in `resume_on_verdict` follows.
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
        }
    }

    /// Deposit the applied result, durably mark the apply, record the apply's
    /// write versions, and propose the `CompletionAck`.
    ///
    /// Shared by the flush-completion path and the direct-apply (dependent /
    /// active) apply path.
    ///
    /// `redo_lsn` is `Some(lsn)` when a `TransactionRedo` record was already
    /// WAL-appended for this commit's non-empty write set (`finish_redo_resolve`)
    /// — that record already IS the durable applied marker, so only write
    /// versions are recorded at it. `None` (a drop, an empty-ops staged commit,
    /// or the direct-apply dependent/active path, which carries no redo record)
    /// falls back to appending a `CalvinApplied` marker here, exactly as before
    /// this record existed.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn commit_apply_tail(
        &mut self,
        txn_id: TxnId,
        response: Response,
        redo_lsn: Option<crate::types::Lsn>,
    ) -> bool {
        // Deposit the FULL applied Response (affected-count + watermark + any
        // RETURNING rows) into the local sidecar BEFORE proposing the replicated
        // CompletionAck. The ack fires the coordinator's completion oneshot on
        // every sequencer member, so depositing first guarantees the result is
        // present by the time the coordinator drains it — no lost result, no
        // race.
        //
        // Gated on the PRIMARY-WRITE participant: any participant whose slice
        // carries the user's non-edge DML (Document/KV/Vector/etc.), as opposed
        // to the implicit graph-edge cleanup that dual-homes alongside it. A
        // multi-collection cross-shard COMMIT has MANY primary-write
        // participants — each a plain affected-count write — and they coalesce:
        // the first applied response stands for the coordinator (which discards
        // it for a COMMIT tag anyway), and the plain-write siblings do not
        // conflict. Only a genuine cross-shard RETURNING union — two
        // participants each carrying RETURNING rows — records `Conflict`.
        // Results travel via this in-process sidecar only — never the sequencer
        // Raft log.
        let (has_primary_write, has_returning) = self
            .pending
            .get(&txn_id)
            .map(|p| (p.has_primary_write, p.has_returning))
            .unwrap_or((false, false));
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
                    slot.insert(CalvinApplyResult::Single {
                        response,
                        has_returning,
                    });
                }
                Entry::Occupied(mut slot) => {
                    // Derive both facts from the existing entry BEFORE any
                    // insert, so the immutable borrow does not outlive the
                    // mutable one.
                    let existing_returning = matches!(
                        slot.get(),
                        CalvinApplyResult::Single {
                            has_returning: true,
                            ..
                        }
                    );
                    let already_conflict = matches!(slot.get(), CalvinApplyResult::Conflict);

                    if already_conflict {
                        // A RETURNING union was already recorded; stays Conflict.
                    } else if has_returning && existing_returning {
                        // Two RETURNING-bearing participants for one Calvin txn:
                        // a cross-shard RETURNING union, which is unsupported.
                        // Record Conflict so the coordinator fails the statement
                        // loudly rather than returning one shard's partial rows.
                        tracing::error!(
                            epoch = txn_id.epoch,
                            position = txn_id.position,
                            vshard = self.vshard_id,
                            "two RETURNING-bearing participants for one Calvin txn — cross-shard \
                             RETURNING union unsupported"
                        );
                        slot.insert(CalvinApplyResult::Conflict);
                    } else if has_returning {
                        // The incoming participant carries the rows; the existing
                        // entry was a plain affected-count sibling. Rows win.
                        slot.insert(CalvinApplyResult::Single {
                            response,
                            has_returning: true,
                        });
                    } else {
                        // Incoming is a plain write; keep the existing entry — a
                        // multi-collection cross-shard COMMIT coalesces (the
                        // coordinator discards it for a COMMIT tag anyway).
                    }
                }
            }
        }
        let applied_lsn = match redo_lsn {
            // The TransactionRedo record already durably marks this apply — the
            // SAME shard-local WAL-LSN space fast-path writes and read
            // watermarks use. Record the apply's per-key write versions at it;
            // no second (CalvinApplied) marker is written.
            Some(lsn) => {
                self.record_calvin_write_versions(txn_id, lsn);
                Some(lsn)
            }
            None => match self.shared.wal.append_calvin_applied(
                crate::types::VShardId::new(self.vshard_id),
                txn_id.epoch,
                txn_id.position,
            ) {
                // The CalvinApplied WAL LSN is the committed write-LSN for this
                // apply — the SAME shard-local WAL-LSN space fast-path writes and
                // read watermarks use. Record the apply's per-key write versions
                // at it now that it exists (it did not at dispatch time).
                Ok(applied_lsn) => {
                    self.record_calvin_write_versions(txn_id, applied_lsn);
                    Some(applied_lsn)
                }
                Err(e) => {
                    tracing::error!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        error = %e,
                        "calvin: failed to write CalvinApplied WAL record"
                    );
                    None
                }
            },
        };
        let Some(lsn) = applied_lsn else {
            // The apply cannot be acknowledged without a durable participant
            // LSN: CDC and write-version consumers would otherwise observe a
            // successful commit with no authoritative ordering point.
            return false;
        };
        // Control change-stream events are distinct from Data-Plane
        // WriteEvents. Publish the participant-local logical manifests once,
        // from the data-group leader, at the authoritative committed LSN.
        if self.is_group_leader()
            && let Some(pending) = self.pending.get_mut(&txn_id)
        {
            let tenant_id = pending.txn.tx_class.tenant_id;
            let database_id = pending.txn.tx_class.database_id;
            for change_set in std::mem::take(&mut pending.change_sets) {
                crate::control::server::dispatch_utils::publish_change_set_with_lsn(
                    &self.shared,
                    tenant_id,
                    database_id,
                    change_set,
                    lsn,
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
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use nodedb_cluster::MultiRaft;
    use nodedb_cluster::RoutingTable;
    use nodedb_cluster::calvin::types::{
        EngineKeySet, ReadWriteSet, SequencedTxn, SortedVec, TxClass, VersionedReadSet,
    };
    use nodedb_cluster::calvin::{
        AbortReason, CalvinCompletionRegistry, ParticipantVote, SequencerStateMachine,
        VerdictOutcome,
    };
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;
    use nodedb_types::TenantId;

    use super::super::scheduler::{Scheduler, SchedulerParams};
    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::envelope::Payload;
    use crate::control::cluster::calvin::scheduler::driver::types::PendingTxn;
    use crate::control::cluster::calvin::scheduler::lock_manager::LockManager;
    use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
    use crate::control::cluster::calvin::scheduler::{NOT_YET_APPLIED_EPOCH, SchedulerConfig};
    use crate::control::state::SharedState;
    use crate::types::{Lsn, RequestId};
    use crate::wal::WalManager;

    /// Same minimal scheduler fixture as the process/catch_up driver tests,
    /// retaining its Data-Plane request receiver for tests that must observe
    /// scheduler dispatches.
    fn build_test_scheduler_with_data_side(
        vshard_id: u32,
        registry: Arc<CalvinCompletionRegistry>,
    ) -> (
        Scheduler,
        tempfile::TempDir,
        crate::bridge::dispatch::CoreChannelDataSide,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
        let (dispatcher, mut data_sides) = Dispatcher::new(1, 64);
        let data_side = data_sides
            .pop()
            .expect("one configured core has one data side");
        let shared = SharedState::new(dispatcher, wal).unwrap();

        let rt = RoutingTable::uniform(1, &[1], 1);
        let multi_raft = Arc::new(Mutex::new(MultiRaft::new(1, rt, dir.path().to_path_buf())));

        let sequencer_state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
            HashMap::new(),
            Arc::clone(&registry),
        )));

        let (_tx, receiver) = tokio::sync::mpsc::channel(16);
        let (_rr_tx, read_result_rx) = tokio::sync::mpsc::channel(16);
        let (_prom_tx, promotion_rx) = tokio::sync::mpsc::unbounded_channel();
        let (verdict_tx, verdict_rx) = tokio::sync::mpsc::channel(16);
        registry.register_verdict_signal_sender(vshard_id, verdict_tx);

        let lock_manager = Arc::new(Mutex::new(LockManager::new()));

        let scheduler = Scheduler::new(SchedulerParams {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            sequencer_state_machine,
            fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
            applied_tail: std::collections::BTreeSet::new(),
            rebuild_target_epoch: 0,
            config: SchedulerConfig::default(),
            metrics: SchedulerMetrics::new(),
            read_result_rx,
            lock_manager,
            promotion_rx,
            registry,
            verdict_rx,
        });
        (scheduler, dir, data_side)
    }

    /// Build a static-write `SequencedTxn` at `(epoch, position)`.
    fn staged_pending(txn: SequencedTxn, txn_id: TxnId) -> PendingTxn {
        PendingTxn {
            txn,
            lock_owner: txn_id,
            dispatch_time: std::time::Instant::now(),
            has_primary_write: true,
            has_returning: false,
            change_sets: Vec::new(),
            commit_state: Some(CommitState::Staged),
            verdict_deadline: None,
        }
    }

    fn staged_response(status: Status, read_set_valid: Option<bool>) -> Response {
        Response {
            request_id: RequestId::new(1),
            status,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn make_sequenced_txn(epoch: u64, position: u32) -> SequencedTxn {
        let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: "test_coll".to_string(),
            surrogates: SortedVec::new(vec![1]),
        }]);
        let tx_class = TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        SequencedTxn {
            epoch,
            position,
            tx_class,
            epoch_system_ms: 1_700_000_000_000,
            epoch_vshard_txn_count: 1,
            lock_owner: None,
        }
    }

    /// A false vote from either participant makes the only global verdict abort;
    /// applying that durable verdict broadcasts the abort to every parked local
    /// participant. The scheduler's `resume_on_verdict(false)` then dispatches a
    /// drop, never a resolve/flush, on each recipient.
    #[tokio::test]
    async fn two_participant_false_vote_broadcasts_global_abort_to_every_scheduler() {
        let registry = CalvinCompletionRegistry::new_detached();
        let txn = nodedb_cluster::calvin::TxnId::new(14, 2);
        let txn_id = TxnId::new(14, 2);
        let (mut first_scheduler, _first_dir, mut first_data) =
            build_test_scheduler_with_data_side(7, Arc::clone(&registry));
        let (mut second_scheduler, _second_dir, mut second_data) =
            build_test_scheduler_with_data_side(9, Arc::clone(&registry));
        first_scheduler
            .pending
            .insert(txn_id, staged_pending(make_sequenced_txn(14, 2), txn_id));
        second_scheduler
            .pending
            .insert(txn_id, staged_pending(make_sequenced_txn(14, 2), txn_id));

        // Local staging votes only park their own staged slices; neither the
        // affirmative nor the failed participant may resolve or drop unilaterally.
        first_scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Ok, Some(true)));
        second_scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Error, None));
        for (scheduler, data_side) in [
            (&first_scheduler, &mut first_data),
            (&second_scheduler, &mut second_data),
        ] {
            assert!(matches!(
                scheduler
                    .pending
                    .get(&txn_id)
                    .and_then(|pending| pending.commit_state),
                Some(CommitState::AwaitingVerdict)
            ));
            assert!(data_side.request_rx.try_pop().is_err());
        }

        // Model the replicated vote entries and their resulting durable verdict.
        // The shared registry sends each scheduler's actual registered channel.
        registry.seed_expected(txn, 2);
        registry.note_vote(txn, 7, ParticipantVote::Commit);
        assert!(registry.drain_unproposed_verdicts().is_empty());
        registry.note_vote(
            txn,
            9,
            ParticipantVote::Abort(Some(AbortReason::SerializationConflict)),
        );
        assert_eq!(
            registry.drain_unproposed_verdicts(),
            vec![(
                txn,
                VerdictOutcome::Abort(Some(AbortReason::SerializationConflict))
            )]
        );
        registry.note_verdict(
            txn,
            VerdictOutcome::Abort(Some(AbortReason::SerializationConflict)),
        );
        assert_eq!(registry.verdict(txn), Some(false));

        let first_signal = first_scheduler
            .verdict_rx
            .try_recv()
            .expect("registry must signal the first registered scheduler");
        let second_signal = second_scheduler
            .verdict_rx
            .try_recv()
            .expect("registry must signal the second registered scheduler");
        first_scheduler.handle_verdict_signal(first_signal);
        second_scheduler.handle_verdict_signal(second_signal);

        for (scheduler, data_side) in [
            (&first_scheduler, &mut first_data),
            (&second_scheduler, &mut second_data),
        ] {
            assert!(matches!(
                scheduler
                    .pending
                    .get(&txn_id)
                    .and_then(|pending| pending.commit_state),
                Some(CommitState::AwaitingResolve {
                    committed: false,
                    redo_lsn: None
                })
            ));
            let request = data_side
                .request_rx
                .try_pop()
                .expect("global abort must dispatch a drop to every participant");
            assert!(matches!(
                request.inner.plan,
                PhysicalPlan::Meta(MetaOp::CalvinDrop {
                    epoch: 14,
                    position: 2
                })
            ));
            assert!(data_side.request_rx.try_pop().is_err());
        }
    }
}
