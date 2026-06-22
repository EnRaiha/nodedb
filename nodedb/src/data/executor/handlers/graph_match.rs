// SPDX-License-Identifier: BUSL-1.1

//! Graph pattern matching handler — executes MATCH queries on the Data Plane.

use tracing::{debug, warn};

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::pattern::ast::MatchQuery;

impl CoreLoop {
    /// Encode a `MatchOutcome`'s rows to MessagePack and build the appropriate
    /// response envelope (`partial` if the outcome was truncated, normal otherwise).
    ///
    /// Shared by [`execute_graph_match`] and [`execute_graph_match_continuation`]
    /// to avoid duplicating the encode → response tail.
    fn match_outcome_response(
        &self,
        task: &ExecutionTask,
        outcome: crate::engine::graph::pattern::executor::MatchOutcome,
    ) -> Response {
        match crate::engine::graph::pattern::executor::rows_to_msgpack(&outcome.rows) {
            Ok(payload) => {
                if outcome.truncated {
                    self.response_partial(task, payload)
                } else {
                    self.response_with_payload(task, payload)
                }
            }
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Return an empty MATCH result payload for a tenant that has no graph
    /// state on this shard.  An absent CSR partition is not an error.
    ///
    /// Shared by [`execute_graph_match`] and [`execute_graph_match_continuation`].
    fn match_empty_partition_response(&self, task: &ExecutionTask) -> Response {
        match crate::engine::graph::pattern::executor::rows_to_msgpack(&[]) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    pub(in crate::data::executor) fn execute_graph_match(
        &self,
        task: &ExecutionTask,
        tid: u64,
        query_bytes: &[u8],
        frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    ) -> Response {
        debug!(core = self.core_id, tid, "graph match execution");
        let database_id = task.request.database_id.as_u64();

        // Deserialize the MatchQuery from MessagePack.
        let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
            Ok(q) => q,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize MatchQuery");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid match query: {e}"),
                    },
                );
            }
        };

        // Execute the pattern match on the caller's CSR partition +
        // EdgeStore. An absent partition means "this tenant has no
        // graph state" — return the empty row set rather than error.
        let partition = match self.csr_partition(database_id, tid) {
            Some(p) => p,
            None => return self.match_empty_partition_response(task),
        };
        match crate::engine::graph::pattern::executor::execute(
            &query,
            partition,
            &self.edge_store,
            frontier_bitmap,
            None, // single-node path: all nodes are local, no frontier entries
        ) {
            Ok(outcome) => self.match_outcome_response(task, outcome),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Cross-shard MATCH continuation: resume a pattern on THIS shard.
    ///
    /// Deserializes the already-optimized `MatchQuery` and the accumulated
    /// `partial_row`, seeds the source binding (`source_binding -> source_node`)
    /// on top of the partial bindings, then resumes expansion from
    /// `resume_triple_idx` against this shard's CSR partition.
    ///
    /// Phase A scope: returns ROWS ONLY — identical response format to
    /// `execute_graph_match`. `is_remote_node` is `None`, so this resume emits
    /// no further frontier (Phase B will wire recursive scatter).
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_graph_match_continuation(
        &self,
        task: &ExecutionTask,
        tid: u64,
        query_bytes: &[u8],
        resume_triple_idx: usize,
        partial_row_bytes: &[u8],
        source_node: &str,
        source_binding: &str,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid, "graph match continuation execution"
        );
        let database_id = task.request.database_id.as_u64();

        // Deserialize the already-optimized MatchQuery.
        let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
            Ok(q) => q,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize MatchQuery");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid match query: {e}"),
                    },
                );
            }
        };

        // Deserialize the accumulated partial bindings.
        let mut seed_row: HashMap<String, String> = match zerompk::from_msgpack(partial_row_bytes) {
            Ok(r) => r,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize partial_row");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid continuation partial_row: {e}"),
                    },
                );
            }
        };
        // Seed the source binding so the resumed triple resolves its source
        // from the bound variable rather than free-ranging.
        seed_row.insert(source_binding.to_string(), source_node.to_string());

        // An absent partition means this tenant has no graph state on this
        // shard — return the empty row set rather than error.
        let partition = match self.csr_partition(database_id, tid) {
            Some(p) => p,
            None => return self.match_empty_partition_response(task),
        };

        match crate::engine::graph::pattern::executor::execute_continuation(
            &query,
            partition,
            &self.edge_store,
            None, // Phase A: no anchor prefilter on the resume path
            None, // Phase A: emit no further frontier from the continuation
            resume_triple_idx,
            seed_row,
        ) {
            Ok(outcome) => self.match_outcome_response(task, outcome),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
