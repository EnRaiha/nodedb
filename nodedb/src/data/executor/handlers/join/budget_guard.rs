// SPDX-License-Identifier: BUSL-1.1

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Returns a `ResourcesExhausted` error response if the materialized join
    /// side `docs` exceeds the per-query byte `budget` (0 = unlimited), else
    /// `None`. Centralizes the per-side memory bound shared by the hash,
    /// sort-merge, and nested-loop join handlers so an unbounded join surfaces
    /// a deterministic error instead of OOMing — never silently truncating.
    pub(super) fn join_side_over_budget(
        &self,
        task: &ExecutionTask,
        docs: &[(String, Vec<u8>)],
        budget: usize,
    ) -> Option<Response> {
        if crate::data::executor::handlers::scan_budget::scan_bytes_exceeded(docs, budget) {
            Some(self.response_error(task, ErrorCode::ResourcesExhausted))
        } else {
            None
        }
    }
}
