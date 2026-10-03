// SPDX-License-Identifier: BUSL-1.1

//! Dispatch of staged Calvin flush/drop resolution operations.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::commit_redo::missing_pending_error;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::scheduler::Scheduler;
use crate::bridge::dispatch::JournalGroup;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::Lsn;

/// How a staged transaction resolves on this vShard.
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum CommitResolution {
    /// Install the committed redo record the scheduler appended at
    /// `redo_lsn`. The flush scope holds its bytes. Both are empty when the
    /// transaction wrote nothing on this vShard.
    Flush { redo_lsn: Option<Lsn> },
    /// Discard the staged state under an abort verdict.
    Drop,
}

impl Scheduler {
    /// Dispatch a flush or drop of a staged transaction's commit-pending buffer.
    ///
    /// A flush carries the redo record, the collections the local plans
    /// write, and their materialized-sum targets, so the Data Plane installs
    /// the record the way every committed transaction installs.
    ///
    /// A capacity refusal returns [`DispatchOutcome::Deferred`]: the flush or
    /// drop is parked for re-send and the txn stays in flight. A txn with no
    /// `pending` entry returns [`DispatchOutcome::Failed`]. The flush takes its
    /// collections and sum targets from the scope derived at stage time. A
    /// flush whose gates a halt released takes them back first, and fails when
    /// a collection it writes no longer holds its planned incarnation.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn dispatch_commit_resolution(
        &mut self,
        txn_id: TxnId,
        resolution: CommitResolution,
    ) -> DispatchOutcome {
        // A flush writes the collections: it holds their gates while it runs.
        if matches!(resolution, CommitResolution::Flush { .. })
            && let Err(error) = self.regate(txn_id)
        {
            return DispatchOutcome::Failed(error);
        }
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return DispatchOutcome::Failed(missing_pending_error(txn_id));
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let event_source =
            super::request::slice_event_source(&pending.txn.tx_class, &pending.flush_scope);
        let commit_hlc = self
            .cut_floors
            .commit_hlc(pending.txn.epoch, pending.txn.epoch_system_ms);
        let epoch = txn_id.epoch;
        let position = txn_id.position;
        let (plan, step, wal_lsn, journal) = match resolution {
            CommitResolution::Flush { redo_lsn } => {
                pending.flush_scope.sends = pending.flush_scope.sends.saturating_add(1);
                let scope = &pending.flush_scope;
                // The core stores the flush's append inputs beside its
                // effects for boot. The record is whole at append, so no part
                // follows it.
                let journal = redo_lsn.map(|origin| JournalGroup {
                    origin,
                    collection: scope.collections.first().cloned().unwrap_or_default(),
                    apply_key: crate::wal::manager::NO_APPLY_KEY,
                    commit_hlc: Some(commit_hlc),
                    change_position: None,
                });
                (
                    PhysicalPlan::Meta(MetaOp::CalvinFlush {
                        epoch,
                        position,
                        redo: scope.redo.clone(),
                        collections: scope.collections.clone(),
                        sum_targets: scope.sum_targets.clone(),
                    }),
                    DispatchStep::Flush,
                    redo_lsn,
                    journal,
                )
            }
            CommitResolution::Drop => (
                PhysicalPlan::Meta(MetaOp::CalvinDrop { epoch, position }),
                DispatchStep::Drop,
                None,
                None,
            ),
        };
        let request_id = self.next_request_id();
        let mut request =
            self.build_exempt_request(request_id, tenant_id, database_id, plan, wal_lsn);
        // The flush emits the transaction's events, under its own source and
        // dated by its commit HLC.
        request.event_source = event_source;
        request.commit_hlc = Some(commit_hlc);
        self.dispatch_sequenced_journalled(txn_id, step, request, journal)
    }
}
