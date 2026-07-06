// SPDX-License-Identifier: BUSL-1.1

//! `TextOp::SetAnalyzer` handler: binds a collection's per-collection FTS
//! analyzer. Called by `dispatch_text` — see `CREATE SEARCH INDEX ...
//! ANALYZER '<name>'`.

use tracing::warn;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Persist `analyzer_name` as `collection`'s configured FTS analyzer.
    /// Every subsequent tokenization of the collection's text — forward
    /// indexing, the in-transaction staged-write overlay, and query-time
    /// scoring — resolves through `InvertedIndex::analyze_for_collection`,
    /// which reads this same binding.
    pub(in crate::data::executor) fn execute_set_analyzer(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        analyzer_name: &str,
    ) -> Response {
        let tenant_id = nodedb_types::TenantId::new(tid);
        let database_id = task.request.database_id.as_u64();
        match self.inverted.set_collection_analyzer(
            database_id,
            tenant_id,
            collection,
            analyzer_name,
        ) {
            Ok(()) => self.response_ok(task),
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    analyzer = analyzer_name,
                    error = %e,
                    "SetAnalyzer: analyzer binding failed"
                );
                self.response_error(task, e)
            }
        }
    }
}
