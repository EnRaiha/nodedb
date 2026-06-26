// SPDX-License-Identifier: BUSL-1.1

//! `Scheduler` struct definition, constructor, and main run loop.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tracing::info;

use nodedb_cluster::MultiRaft;
use nodedb_cluster::calvin::types::SequencedTxn;
use nodedb_cluster::calvin::{SEQUENCER_GROUP_ID, SequencerEntry};

use super::super::barrier::{PendingDependentBarrier, ReadResultEvent};
use super::super::config::SchedulerConfig;
use super::super::types::{BlockedTxn, PendingTxn};
use crate::bridge::envelope::Response;
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
#[allow(unused_imports)]
use crate::control::cluster::calvin::scheduler::recovery::read_last_applied_epoch;
use crate::control::shutdown::ShutdownReceiver;
use crate::control::state::SharedState;
use crate::types::RequestId;

/// Outcome of an executor response bridge task.
///
/// `None` means the executor response channel was closed before a response
/// arrived (infra error).
pub(in crate::control::cluster::calvin::scheduler::driver::core) type CompletionItem =
    (TxnId, RequestId, Option<Response>);

/// The Calvin scheduler for one vshard.
///
/// Owns the in-memory lock table and orchestrates lock acquisition, dispatch,
/// and response handling for both static-set and dependent-read transactions.
///
/// `Send` — runs as a Tokio task on the Control Plane.
pub struct Scheduler {
    /// Vshard this scheduler is responsible for.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) vshard_id: u32,
    /// Incoming sequenced transactions from the sequencer fan-out.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) receiver:
        mpsc::Receiver<SequencedTxn>,
    /// Shared control-plane state used for dispatch, response tracking, WAL,
    /// and request-id allocation.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) shared: Arc<SharedState>,
    /// Handle to MultiRaft so completion acknowledgements can be proposed to
    /// the sequencer group.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) multi_raft:
        Arc<Mutex<MultiRaft>>,
    /// Deterministic lock manager for this vshard.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) lock_manager: LockManager,
    /// In-flight static/active transactions awaiting executor response.
    /// `BTreeMap` ensures deterministic iteration order.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) pending:
        BTreeMap<TxnId, PendingTxn>,
    /// Blocked transactions awaiting lock release.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) blocked:
        BTreeMap<TxnId, BlockedTxn>,
    /// Dependent-read barriers awaiting passive read results.
    /// `BTreeMap` for determinism.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) dependent_barrier:
        BTreeMap<TxnId, PendingDependentBarrier>,
    /// Channel receiving `CalvinReadResult` Raft apply events from the
    /// per-vshard data Raft apply loop. Bounded.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) read_result_rx:
        mpsc::Receiver<ReadResultEvent>,
    /// Highest epoch applied so far.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) last_applied_epoch: u64,
    /// Rebuild target epoch (from initial recovery scan).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) rebuild_target_epoch: u64,
    /// Scheduler configuration.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) config: SchedulerConfig,
    /// Metrics.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) metrics: Arc<SchedulerMetrics>,
    /// Fan-in receiver for executor responses.
    ///
    /// Each dispatched transaction spawns a lightweight bridge task that
    /// awaits the per-request `mpsc::Receiver<Response>` and forwards the
    /// result here as a [`CompletionItem`]. The scheduler's `select!` loop
    /// includes this channel as a first-class arm so it wakes the moment
    /// any executor response is ready — no polling, no sleep.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) completion_rx:
        mpsc::Receiver<CompletionItem>,
    /// Sender half of the completion fan-in channel, cloned per dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) completion_tx:
        mpsc::Sender<CompletionItem>,
}

impl Scheduler {
    /// Construct a scheduler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vshard_id: u32,
        receiver: mpsc::Receiver<SequencedTxn>,
        shared: Arc<SharedState>,
        multi_raft: Arc<Mutex<MultiRaft>>,
        last_applied_epoch: u64,
        rebuild_target_epoch: u64,
        config: SchedulerConfig,
        metrics: Arc<SchedulerMetrics>,
        read_result_rx: mpsc::Receiver<ReadResultEvent>,
    ) -> Self {
        // Capacity: at most one completion per inflight txn. Use the incoming
        // channel capacity as a proxy for the max concurrent pending count.
        let completion_cap = config.channel_capacity;
        let (completion_tx, completion_rx) = mpsc::channel(completion_cap);

        Self {
            vshard_id,
            receiver,
            shared,
            multi_raft,
            lock_manager: LockManager::new(),
            pending: BTreeMap::new(),
            blocked: BTreeMap::new(),
            dependent_barrier: BTreeMap::new(),
            read_result_rx,
            last_applied_epoch,
            rebuild_target_epoch,
            config,
            metrics,
            completion_rx,
            completion_tx,
        }
    }

    /// Whether the scheduler has caught up to the rebuild target epoch.
    pub fn is_caught_up(&self) -> bool {
        self.last_applied_epoch >= self.rebuild_target_epoch
    }

    /// Spawn a bridge task that awaits a single executor response and forwards
    /// it to the scheduler's fan-in completion channel.
    ///
    /// The bridge task is cancel-safe: it holds only a cloned sender and the
    /// per-request receiver. Dropping the scheduler's `completion_rx` causes
    /// the bridge's `send` to fail silently, which is fine on shutdown.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn spawn_response_bridge(
        &self,
        txn_id: TxnId,
        request_id: RequestId,
        mut response_rx: mpsc::Receiver<Response>,
    ) {
        let tx = self.completion_tx.clone();
        tokio::spawn(async move {
            let result = response_rx.recv().await;
            // Ignore send error: scheduler has shut down.
            let _ = tx.send((txn_id, request_id, result)).await;
        });
    }

    /// Run the scheduler event loop until shutdown is signaled.
    pub async fn run(mut self, mut shutdown: ShutdownReceiver) {
        info!(
            vshard_id = self.vshard_id,
            last_applied_epoch = self.last_applied_epoch,
            rebuild_target_epoch = self.rebuild_target_epoch,
            "calvin scheduler starting"
        );

        loop {
            self.check_dependent_barrier_timeouts();

            tokio::select! {
                biased;

                _ = shutdown.wait_cancelled() => {
                    info!(vshard_id = self.vshard_id, "calvin scheduler shutting down");
                    break;
                }

                maybe_completion = self.completion_rx.recv() => {
                    if let Some((txn_id, request_id, resp_opt)) = maybe_completion {
                        self.handle_completion(txn_id, request_id, resp_opt);
                    }
                }

                maybe_event = self.read_result_rx.recv() => {
                    if let Some(event) = maybe_event {
                        self.handle_read_result(event);
                    }
                }

                maybe_txn = self.receiver.recv() => {
                    match maybe_txn {
                        Some(txn) => self.process_new_txn(txn),
                        None => {
                            info!(
                                vshard_id = self.vshard_id,
                                "calvin scheduler: receiver channel closed; exiting"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Process a completed executor response (or disconnected channel).
    ///
    /// Called from the `completion_rx` arm of the main `select!` loop.
    fn handle_completion(
        &mut self,
        txn_id: TxnId,
        request_id: RequestId,
        resp_opt: Option<Response>,
    ) {
        let response = match resp_opt {
            Some(r) => r,
            None => {
                // Bridge task observed a closed channel before any response.
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    request_id = request_id.as_u64(),
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: executor response channel disconnected"
                );
                self.metrics.record_executor_error();
                self.metrics.record_infra_abort(
                    crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
                );
                self.metrics.record_completed();
                self.on_txn_complete(txn_id);
                return;
            }
        };

        let elapsed_ms = self
            .pending
            .get(&txn_id)
            .map(|p| p.dispatch_time.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.metrics.record_executor_txn_duration_ms(elapsed_ms);

        // OLLP mismatch: the active executor detected predicate drift and returned
        // OllpRetryRequired without writing. The retry loop is now COORDINATOR-owned
        // (`run_dependent_with_retry`): the scheduler must NOT re-submit a stale
        // prediction. Instead it (1) releases the aborted attempt's locks and
        // (2) signals the coordinator's completion waiter via the registry so it
        // can run a FRESH reconnaissance and resubmit.
        if response.status == crate::bridge::envelope::Status::Error
            && response.error_code == Some(crate::bridge::envelope::ErrorCode::OllpRetryRequired)
        {
            // A mismatch is a normal OLLP retry signal, not a failure: the executor
            // correctly detected predicate drift and declined to write. Count it as
            // a received executor response, but NOT as an executor error or infra
            // abort — those would inflate failure metrics on every routine retry.
            self.metrics.record_completed();
            // (1) Release the aborted attempt's locks and clean up pending state.
            // This fixes the lock-leak: the old `schedule_ollp_retry` re-submitted
            // without ever releasing the aborted attempt's locks.
            self.on_txn_complete(txn_id);
            // OLLP mismatch broadcast is LEADER-ONLY. The optimistic-lock
            // verification runs only on the data-group leader (the data plane
            // skips it on followers — see the `ollp_is_group_leader` gate in the
            // bulk-DML handlers), so by construction only a leader's executor can
            // return `OllpRetryRequired`. This guard is defense-in-depth: a
            // non-leader scheduler must never broadcast a mismatch — a lagging
            // follower could otherwise poison an attempt the leader already
            // completed, exhausting retries on a static dataset. A non-leader
            // simply releases locks (done above) and returns; the leader owns the
            // single mismatch signal that the completion registry observes.
            if !self.is_group_leader() {
                tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    "calvin: OllpRetryRequired observed on non-leader; NOT broadcasting mismatch \
                     (leader owns the verification decision)"
                );
                return;
            }
            // (2) Broadcast the mismatch signal via the sequencer-group Raft so that
            // the coordinator's CalvinCompletionRegistry fires on EVERY replica,
            // including remote nodes. The SequencerStateMachine::apply() arm for
            // OllpMismatch calls note_ollp_mismatch, waking the coordinator's
            // retry-loop waiter wherever it is — mirrors how CompletionAck is
            // delivered to remote coordinators.
            self.propose_sequencer_entry(
                SequencerEntry::OllpMismatch {
                    epoch: txn_id.epoch,
                    position: txn_id.position,
                },
                txn_id,
                "OLLP mismatch signal",
            );
            return;
        }

        if response.status == crate::bridge::envelope::Status::Ok {
            if let Err(e) = self.shared.wal.append_calvin_applied(
                crate::types::VShardId::new(self.vshard_id),
                txn_id.epoch,
                txn_id.position,
            ) {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    error = %e,
                    "calvin: failed to write CalvinApplied WAL record"
                );
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
        } else {
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: executor response was not Ok; locks NOT released (shard degraded)"
            );
            self.metrics.record_executor_error();
            self.metrics.record_infra_abort(
                crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason::IO_ERROR,
            );
        }

        self.metrics.record_completed();
        self.on_txn_complete(txn_id);
    }

    /// Encode `entry` as MessagePack and propose it to the sequencer Raft group.
    ///
    /// Logs a warning on encode failure or propose failure; never panics.
    /// `op_name` is a short human-readable label used in warning messages
    /// (e.g. `"completion ack"`, `"OLLP mismatch signal"`).
    fn propose_sequencer_entry(&self, entry: SequencerEntry, txn_id: TxnId, op_name: &str) {
        match zerompk::to_msgpack_vec(&entry) {
            Ok(bytes) => {
                if let Err(e) = self
                    .multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .propose_to_group(SEQUENCER_GROUP_ID, bytes)
                {
                    tracing::warn!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        error = %e,
                        "calvin: failed to propose {op_name}",
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    error = %e,
                    "calvin: failed to encode {op_name}",
                );
            }
        }
    }

    /// Allocate a fresh request ID for a dispatch.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn next_request_id(
        &self,
    ) -> RequestId {
        self.shared.next_request_id()
    }
}
