// SPDX-License-Identifier: BUSL-1.1

//! Static and active (dependent-read) txn dispatch to the Data Plane.

use std::time::Instant;

use tracing::error;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::routing::{PlanRouting, plan_vshard};
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Priority, Request};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockKey, TxnId};
use crate::types::{DatabaseId, ReadConsistency, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

/// Whether this vShard's slice carries a PRIMARY user data write — the write
/// whose applied `Response` (affected-count + any RETURNING rows) the
/// coordinator surfaces.
///
/// A primary write is a Document / KV / Vector / Timeseries / Columnar / Array
/// write — NOT the implicit graph-edge cleanup (`EdgePut` / `EdgeDelete`) that
/// dual-homes alongside a document delete/update. For a single-collection user
/// DML (plus its implicit edges) exactly ONE participant carries the primary
/// write, so only it deposits the applied `Response` into the coordinator's
/// sidecar and the edge participants never clobber the entry.
///
/// This gate subsumes the RETURNING case (a RETURNING write IS a primary write,
/// so its rows are still deposited) while ALSO carrying the affected-count of a
/// plain (non-RETURNING) write — which a RETURNING-only gate dropped, making a
/// routed plain write report zero rows affected.
pub(crate) fn plans_have_primary_write(plans: &[PhysicalPlan]) -> bool {
    plans.iter().any(|plan| {
        crate::control::planner::calvin::is_write_plan(plan)
            && !matches!(plan, PhysicalPlan::Graph(_))
    })
}

impl Scheduler {
    /// Whether THIS node is currently the leader of the data-group owning this
    /// scheduler's vshard.
    ///
    /// Stamped into the `CalvinExecute{Static,Active}` MetaOp at dispatch time
    /// so the Data Plane runs the OLLP optimistic-lock verification (and emits
    /// `OllpRetryRequired`) ONLY on the leader, while every replica applies the
    /// carried predicted write-set verbatim — preserving Calvin determinism.
    ///
    /// Resolved via the existing routing → group-role check (no new election).
    /// On a poisoned lock the inner guard is recovered; a momentarily-unknown
    /// leadership (e.g. mid-election) resolves to `false`, i.e. follower-style
    /// apply against the predicted set, which is always determinism-safe.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_group_leader(
        &self,
    ) -> bool {
        let mr = match self.multi_raft.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        mr.vshard_role_is_leader(self.vshard_id)
    }

    /// Broadcast a terminal, NON-retryable routing-failure signal via the
    /// sequencer-group Raft so every replica's `CalvinCompletionRegistry`
    /// fires `note_routing_failed`, waking the coordinator's completion
    /// waiter immediately with the reason instead of leaving it to burn the
    /// full deadline and report a generic timeout. Mirrors the OllpMismatch
    /// broadcast in `handle_executor_response`. Shared by `dispatch_txn` and
    /// `dispatch_active_txn`.
    fn propose_routing_failure(
        &self,
        epoch: u64,
        position: u32,
        txn_id: TxnId,
        err: &crate::Error,
    ) {
        self.propose_sequencer_entry(
            nodedb_cluster::calvin::SequencerEntry::TxnRoutingFailed {
                epoch,
                position,
                detail: err.to_string(),
            },
            txn_id,
            "txn routing-failure signal",
        );
    }

    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn local_calvin_plans(
        &self,
        plans: Vec<PhysicalPlan>,
        epoch: u64,
        position: u32,
    ) -> crate::Result<Vec<PhysicalPlan>> {
        let mut local = Vec::new();
        for plan in plans {
            match plan_vshard(&plan) {
                PlanRouting::Vshards(vshards) => {
                    if vshards.iter().any(|v| v.as_u32() == self.vshard_id) {
                        local.push(plan);
                    }
                }
                PlanRouting::ControlPlaneOnly => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "calvin txn {epoch}/{position} for vshard {} carries a \
                             control-plane-only plan that must never reach the Data \
                             Plane: {plan:?}",
                            self.vshard_id
                        ),
                    });
                }
                PlanRouting::Unroutable(reason) => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "calvin txn {epoch}/{position} for vshard {} contains an \
                             unroutable plan ({reason}): {plan:?}",
                            self.vshard_id
                        ),
                    });
                }
                PlanRouting::NotAWrite => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "calvin txn {epoch}/{position} for vshard {} contains a \
                             non-write (read/DDL) plan inside a Calvin write \
                             transaction: {plan:?}",
                            self.vshard_id
                        ),
                    });
                }
            }
        }

        if local.is_empty() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "calvin txn {epoch}/{position} contains no local plans for vshard {}",
                    self.vshard_id
                ),
            });
        }

        Ok(local)
    }

    /// Dispatch a static-set ready transaction to the Data Plane executor.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_txn(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        keys: std::collections::BTreeSet<LockKey>,
        lock_acquired_time: Instant,
    ) {
        let request_id = self.next_request_id();
        let tenant_id = txn.tx_class.tenant_id;
        let vshard_id = VShardId::new(self.vshard_id);
        let epoch = txn.epoch;
        let position = txn.position;

        let plans = match super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: plan decode failed; releasing locks and skipping txn"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plans = match self.local_calvin_plans(plans, epoch, position) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: static txn routing failed; releasing locks"
                );
                self.propose_routing_failure(epoch, position, txn_id, &e);
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let has_primary_write = plans_have_primary_write(&plans);
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic {
            epoch,
            position,
            tenant_id,
            plans,
            epoch_system_ms: txn.epoch_system_ms,
            is_group_leader: self.is_group_leader(),
            // The replicated read-set travels to the apply core so each
            // participant can check, at apply, whether its slice of the reads was
            // still current. Empty for pure-write / autocommit transactions.
            versioned_reads: txn.tx_class.versioned_reads.as_slice().to_vec(),
        });

        // no-determinism: request deadline is ephemeral, not written to WAL
        let deadline = Instant::now()
            + std::time::Duration::from_millis(
                self.config.epoch_duration_ms * u64::from(self.config.txn_deadline_multiplier),
            );

        let request = Request {
            request_id,
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id,
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
            // Calvin allocates the CalvinApplied WAL LSN post-apply (in the
            // scheduler's response handler), so no committed LSN is known at
            // dispatch time to stamp here.
            wal_lsn: None,
            // Calvin resolves TTL instants via `epoch_system_ms`, not this
            // field — see `resolved_now_ms` precedence in the KV write handlers.
            resolved_now_ms: None,
            // Calvin-scheduled apply: the deterministic scheduler already holds
            // this transaction's locks, so it does not re-acquire at the
            // write-admission gate — Exempt.
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::AlreadyOrdered,
            ),
        };

        let resp_rx = self.shared.tracker.register(request_id);

        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: dispatch failed; releasing locks"
            );
            self.on_txn_complete(txn_id);
            return;
        }

        self.metrics.record_dispatch();

        // no-determinism: executor latency observability, off-WAL path
        let dispatch_instant = Instant::now();

        self.spawn_response_bridge(txn_id, request_id, resp_rx);

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                keys,
                request_id,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: dispatch_instant,
                lock_acquired_time,
                has_primary_write,
                // This dispatch STAGED the txn (validate + buffer, no apply);
                // its response carries the local commit vote that drives the
                // subsequent flush-or-drop.
                commit_state: Some(super::super::types::CommitState::Staged),
            },
        );
    }

    /// Dispatch an active dependent-read txn once all passive results are in.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_active_txn(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        keys: std::collections::BTreeSet<LockKey>,
        lock_acquired_time: Instant,
        injected_reads: std::collections::BTreeMap<
            nodedb_physical::physical_plan::meta::PassiveReadKeyId,
            nodedb_types::Value,
        >,
    ) {
        let request_id = self.next_request_id();
        let tenant_id = txn.tx_class.tenant_id;
        let vshard_id = VShardId::new(self.vshard_id);
        let epoch = txn.epoch;
        let position = txn.position;

        let plans = match super::super::helpers::decode_plans(&txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active plan decode failed; releasing locks"
                );
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let plans = match self.local_calvin_plans(plans, epoch, position) {
            Ok(p) => p,
            Err(e) => {
                error!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin scheduler: active txn routing failed; releasing locks"
                );
                self.propose_routing_failure(epoch, position, txn_id, &e);
                self.on_txn_complete(txn_id);
                return;
            }
        };
        let has_primary_write = plans_have_primary_write(&plans);
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecuteActive {
            epoch,
            position,
            tenant_id,
            plans,
            injected_reads,
            epoch_system_ms: txn.epoch_system_ms,
            is_group_leader: self.is_group_leader(),
        });

        // no-determinism: request deadline is ephemeral, not written to WAL
        let deadline = Instant::now()
            + std::time::Duration::from_millis(
                self.config.epoch_duration_ms * u64::from(self.config.txn_deadline_multiplier),
            );

        let request = Request {
            request_id,
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id,
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
            // Calvin allocates the CalvinApplied WAL LSN post-apply (in the
            // scheduler's response handler), so no committed LSN is known at
            // dispatch time to stamp here.
            wal_lsn: None,
            // Calvin resolves TTL instants via `epoch_system_ms`, not this
            // field — see `resolved_now_ms` precedence in the KV write handlers.
            resolved_now_ms: None,
            // Calvin-scheduled apply: the deterministic scheduler already holds
            // this transaction's locks, so it does not re-acquire at the
            // write-admission gate — Exempt.
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::AlreadyOrdered,
            ),
        };

        let resp_rx = self.shared.tracker.register(request_id);

        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            error!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin scheduler: active dispatch failed; releasing locks"
            );
            self.on_txn_complete(txn_id);
            return;
        }

        self.metrics.record_dispatch();

        // no-determinism: executor latency observability, off-WAL path
        let dispatch_instant = Instant::now();

        self.spawn_response_bridge(txn_id, request_id, resp_rx);

        self.pending.insert(
            txn_id,
            super::super::types::PendingTxn {
                txn,
                keys,
                request_id,
                // no-determinism: dispatch_time is scheduler observability, not Calvin WAL data
                dispatch_time: dispatch_instant,
                lock_acquired_time,
                has_primary_write,
                // The dependent-read active path applies directly (it resolves
                // its reads via injected values, not a staged buffer).
                commit_state: None,
            },
        );
    }
}
