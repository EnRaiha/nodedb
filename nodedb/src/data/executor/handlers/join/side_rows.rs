// SPDX-License-Identifier: BUSL-1.1

//! The rows of one side of a nested-loop or sort-merge join: the side's
//! resolved input when the coordinator set one, else a local scan of the
//! side's collection.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// One join side as the plan names it.
pub(super) struct JoinSideSource<'a> {
    pub collection: &'a str,
    /// The coordinator-resolved rows, set inside a transaction so the side
    /// reads its staged writes. The child carries the side's read policy.
    pub input: Option<&'a nodedb_physical::physical_plan::PhysicalPlan>,
    /// The side's read policy, applied to locally scanned rows.
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    /// Materialize one join side, up to `scan_limit` locally scanned rows.
    ///
    /// A child error response is returned unchanged.
    pub(super) fn join_side_rows(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        side: JoinSideSource<'_>,
        scan_limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, Box<Response>> {
        if let Some(input) = side.input {
            let docs = self.input_docs(task, input)?;
            return self
                .retain_rows_matching(docs, side.rls_filters, "RLS filter (join side)")
                .map_err(|e| Box::new(self.response_error(task, ErrorCode::from(e))));
        }
        let did = task.request.database_id.as_u64();
        // A timeseries side reads its flushed partitions through a registry
        // that loads lazily.
        if let Err(e) = self.prepare_collection_scan(did, tid, side.collection) {
            return Err(Box::new(self.response_error(task, ErrorCode::from(e))));
        }
        self.scan_collection_with_rls(did, tid, side.collection, scan_limit, side.rls_filters)
            .map_err(|e| Box::new(self.response_error(task, ErrorCode::from(e))))
    }
}
