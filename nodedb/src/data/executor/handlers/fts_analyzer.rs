// SPDX-License-Identifier: BUSL-1.1

//! `TextOp::SetTextConfig` handler: binds a collection's per-collection FTS
//! analyzer and its default fuzzy-matching behaviour. Called by
//! `dispatch_text` — see `CREATE SEARCH INDEX ... ANALYZER '<name>' FUZZY <b>`.

use tracing::warn;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Persist `collection`'s FTS configuration. `None` for either setting
    /// leaves the stored value alone, so a statement that specifies one
    /// property does not silently reset the other.
    ///
    /// Every subsequent tokenization of the collection's text — forward
    /// indexing, the in-transaction staged-write overlay, and query-time
    /// scoring — resolves the analyzer through
    /// `InvertedIndex::analyze_for_collection`, and every search path ORs in
    /// the fuzzy default inside `FtsIndex::search`; both read what is written
    /// here.
    pub(in crate::data::executor) fn execute_set_text_config(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        analyzer_name: Option<&str>,
        fuzzy_default: Option<bool>,
    ) -> Response {
        let tenant_id = nodedb_types::TenantId::new(tid);
        let database_id = task.request.database_id.as_u64();

        if let Some(analyzer_name) = analyzer_name
            && let Err(e) = self.inverted.set_collection_analyzer(
                database_id,
                tenant_id,
                collection,
                analyzer_name,
            )
        {
            warn!(
                core = self.core_id,
                %collection,
                analyzer = analyzer_name,
                error = %e,
                "SetTextConfig: analyzer binding failed"
            );
            return self.response_error(task, e);
        }

        if let Some(fuzzy) = fuzzy_default
            && let Err(e) =
                self.inverted
                    .set_collection_fuzzy(database_id, tenant_id, collection, fuzzy)
        {
            warn!(
                core = self.core_id,
                %collection,
                fuzzy,
                error = %e,
                "SetTextConfig: fuzzy default binding failed"
            );
            return self.response_error(task, e);
        }

        self.response_ok(task)
    }
}
