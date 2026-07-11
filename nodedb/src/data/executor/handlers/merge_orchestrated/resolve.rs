// SPDX-License-Identifier: BUSL-1.1

//! MERGE RESOLVE pass: classify without writing, returning the NOT-MATCHED
//! insert rows for Control-Plane surrogate assignment.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::super::merge::MergeParams;

impl CoreLoop {
    /// RESOLVE pass: return the NOT-MATCHED insert rows without writing.
    ///
    /// Response payload is msgpack `Vec<(join_key, body_msgpack)>` — the
    /// orchestrator assigns a fresh, registered surrogate per row.
    pub(in crate::data::executor) fn execute_merge_resolve(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        let plan = match self.collect_merge_plan(task.request.database_id.as_u64(), tid, &params) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };
        let pairs: Vec<(String, Vec<u8>)> = plan
            .inserts
            .into_iter()
            .map(|i| (i.join_key, i.body))
            .collect();
        match zerompk::to_msgpack_vec(&pairs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("merge resolve encode: {e}"),
                },
            ),
        }
    }
}
