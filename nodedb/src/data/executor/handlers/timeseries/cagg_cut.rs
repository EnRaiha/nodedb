// SPDX-License-Identifier: BUSL-1.1

//! Refresh cuts of continuous aggregates on the Data Plane.
//!
//! A cut arrives as a replicated write and applies with nothing else of its
//! data group in flight. The apply flushes the root's memtable, records the
//! partitions the aggregate still has to fold, and writes the image before
//! it answers. The maintenance tick folds those partitions under the
//! per-tick budget.
//!
//! `REFRESH CONTINUOUS AGGREGATE` then waits for its cut. The core holds the
//! wait request and answers it from the tick once the cut is fully folded,
//! so the fold never blocks the core. The request deadline bounds the wait.

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, RefreshCut};
use nodedb_types::timeseries::PartitionState;

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::continuous_agg::{CutOutcome, CutStatus};
use crate::types::{DatabaseId, TenantId};

/// A `REFRESH CONTINUOUS AGGREGATE` waiting for its cut.
pub struct CaggRefreshWaiter {
    task: ExecutionTask,
    name: String,
    refresh_id: u64,
}

impl CoreLoop {
    /// Apply refresh cut `cut` of an aggregate rooted at `collection`.
    pub(in crate::data::executor) fn execute_cagg_refresh_cut(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        cut: &RefreshCut,
    ) -> Response {
        let now_ms = self.ingest_now_ms();
        match self.apply_cagg_refresh_cut(
            task.request.database_id,
            task.request.tenant_id,
            collection,
            cut,
            now_ms,
        ) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(task, e),
        }
    }

    fn apply_cagg_refresh_cut(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        root: &str,
        cut: &RefreshCut,
        now_ms: i64,
    ) -> crate::Result<()> {
        let (db, tid) = (database_id.as_u64(), tenant_id.as_u64());
        self.ensure_ts_registry(tenant_id, database_id, root)?;
        self.load_cagg_images(db, tid, root);
        // Every row of every record before the cut lands in a partition, and
        // no row after it can: nothing else of the group is in flight.
        self.flush_ts_collection(tenant_id, database_id, root, now_ms)?;
        let mut live: Vec<String> = self
            .ts_registries
            .get(&(database_id, tenant_id, root.to_string()))
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, entry)| entry.meta.state != PartitionState::Deleted)
                    .map(|(_, entry)| entry.dir_name.clone())
                    .collect()
            })
            .unwrap_or_default();
        live.sort_unstable();
        let outcome = self.continuous_agg_mgr.apply_cut(db, tid, root, cut, &live);
        tracing::debug!(
            core = self.core_id,
            collection = %root,
            aggregate = %cut.aggregate,
            refresh_id = cut.refresh_id,
            ?outcome,
            "continuous aggregate refresh cut"
        );
        if matches!(outcome, CutOutcome::Duplicate | CutOutcome::Superseded)
            || self.continuous_agg_mgr.is_quarantined(db, tid, root)
        {
            return Ok(());
        }
        // The image holds the cut before the apply answers, so the group's
        // applied index never passes a cut a restart would not find.
        if let Err(e) = self.persist_cagg_image(db, tid, root) {
            self.continuous_agg_mgr.mark_image_dirty(db, tid, root);
            crate::diag::continuous_aggregate_state_fault(&e, "image_write", db, tid, root);
            return Err(e);
        }
        Ok(())
    }

    /// Hold a refresh wait until its cut is fully folded. Returns the task
    /// unchanged when it is anything else.
    pub(in crate::data::executor) fn hold_cagg_refresh_await(
        &mut self,
        task: ExecutionTask,
    ) -> Option<ExecutionTask> {
        let wait = match task.plan() {
            PhysicalPlan::Meta(MetaOp::AwaitContinuousAggregateRefresh {
                name,
                refresh_id,
                ..
            }) => Some((name.clone(), *refresh_id)),
            _ => None,
        };
        let Some((name, refresh_id)) = wait else {
            return Some(task);
        };
        self.maintenance
            .cagg_refresh_waiters
            .push(CaggRefreshWaiter {
                task,
                name,
                refresh_id,
            });
        // A cut with nothing to fold answers on this tick.
        self.answer_cagg_refresh_waiters();
        None
    }

    /// The answer to a refresh wait now, or `None` while its cut folds.
    pub(in crate::data::executor) fn cagg_refresh_answer(
        &self,
        task: &ExecutionTask,
        name: &str,
        refresh_id: u64,
    ) -> Option<Response> {
        let status = self.continuous_agg_mgr.cut_status(
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            name,
            refresh_id,
        );
        match status {
            CutStatus::Complete => Some(self.response_ok(task)),
            CutStatus::Stale => Some(self.response_error(
                task,
                crate::Error::ObjectNotInPrerequisiteState {
                    object: format!("continuous aggregate '{name}'"),
                    detail: "it is stale and takes no fold; drop and re-create it".into(),
                },
            )),
            // no-determinism: the deadline bounds only when the answer is
            // sent; the cut and its fold do not depend on it.
            CutStatus::Waiting if std::time::Instant::now() > task.request.deadline => {
                Some(self.response_error(task, ErrorCode::DeadlineExceeded))
            }
            CutStatus::Waiting => None,
        }
    }

    /// Answer every refresh wait whose cut completed, whose aggregate is
    /// stale, or whose deadline passed. Called from `tick()`.
    pub fn answer_cagg_refresh_waiters(&mut self) {
        if self.maintenance.cagg_refresh_waiters.is_empty() {
            return;
        }
        let waiters = std::mem::take(&mut self.maintenance.cagg_refresh_waiters);
        for waiter in waiters {
            match self.cagg_refresh_answer(&waiter.task, &waiter.name, waiter.refresh_id) {
                Some(response) => self.send_cagg_refresh_answer(response),
                None => self.maintenance.cagg_refresh_waiters.push(waiter),
            }
        }
    }

    fn send_cagg_refresh_answer(&mut self, response: Response) {
        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "failed to send a REFRESH CONTINUOUS AGGREGATE answer: response queue full"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::cagg_test_support::{
        COLLECTION, ROWS_A, ROWS_B, TENANT, flush, host_def, ingest, register, task_at, totals,
    };
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::continuous_agg::{CutStatus, RefreshPolicy};
    use nodedb_physical::physical_plan::RefreshCut;

    const SUM_A: i64 = 111;
    const SUM_AB: i64 = 11_111;

    fn cut(refresh_id: u64) -> RefreshCut {
        RefreshCut {
            aggregate: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::Manual).incarnation,
            refresh_id,
            periodic: false,
        }
    }

    fn apply(core: &mut crate::data::executor::core_loop::CoreLoop, refresh_id: u64) -> Status {
        core.execute_cagg_refresh_cut(&task_at(9), COLLECTION, &cut(refresh_id))
            .status
    }

    #[test]
    fn a_manual_aggregate_folds_only_at_its_cut() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::Manual));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (0, 0), "no fold before a cut");

        // The cut flushes the memtable, so rows still in it are inside it.
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        assert_eq!(apply(&mut core, 1), Status::Ok);
        assert_eq!(
            core.continuous_agg_mgr.cut_status(0, TENANT, "by_host", 1),
            CutStatus::Waiting
        );
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
        assert_eq!(
            core.continuous_agg_mgr.cut_status(0, TENANT, "by_host", 1),
            CutStatus::Complete
        );
    }

    #[test]
    fn rows_after_a_cut_wait_for_the_next_cut() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::Manual));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        assert_eq!(apply(&mut core, 1), Status::Ok);
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        flush(&mut core);
        core.fold_pending_cagg_partitions(0);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (3, SUM_A));
        assert_eq!(apply(&mut core, 2), Status::Ok);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
    }

    /// The cut is in the image before the apply answers: a restart before
    /// any fold still folds exactly the rows before the cut.
    #[test]
    fn a_pending_cut_survives_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            register(&mut core, host_def("by_host", RefreshPolicy::Manual));
            core.fold_pending_cagg_partitions(0);
            assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
            assert_eq!(apply(&mut core, 1), Status::Ok);
            assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
            flush(&mut core);
        }
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.load_ts_registries().expect("load registries");
        register(&mut core, host_def("by_host", RefreshPolicy::Manual));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (3, SUM_A));
        assert_eq!(
            core.continuous_agg_mgr.cut_status(0, TENANT, "by_host", 1),
            CutStatus::Complete
        );
        assert_eq!(
            apply(&mut core, 1),
            Status::Ok,
            "a re-delivered cut is a no-op"
        );
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (3, SUM_A));
    }

    /// The fold budget spreads a cut over ticks. The wait answers only once
    /// the last partition folded.
    #[test]
    fn the_wait_answers_when_the_cut_is_folded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.ts_tuning.cagg_fold_partitions_per_tick = 1;
        register(&mut core, host_def("by_host", RefreshPolicy::Manual));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        assert_eq!(apply(&mut core, 4), Status::Ok);

        let task = task_at(10);
        assert!(core.cagg_refresh_answer(&task, "by_host", 4).is_none());
        core.fold_pending_cagg_partitions(0);
        assert!(core.cagg_refresh_answer(&task, "by_host", 4).is_none());
        core.fold_pending_cagg_partitions(0);
        let answer = core
            .cagg_refresh_answer(&task, "by_host", 4)
            .expect("the cut is folded");
        assert_eq!(answer.status, Status::Ok);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
    }
}
