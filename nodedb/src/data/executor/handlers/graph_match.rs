// SPDX-License-Identifier: BUSL-1.1

//! Graph pattern matching handler — executes MATCH queries on the Data Plane.

use tracing::{debug, warn};

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::pattern::ast::MatchQuery;
use crate::engine::graph::pattern::executor::{BindingRow, UnresolvedExpansion, rows_to_msgpack};

/// Map key carrying the binding-rows msgpack array in the MATCH envelope.
pub(crate) const MATCH_ENVELOPE_ROWS_KEY: &str = "rows";
/// Map key carrying the cross-shard frontier msgpack array in the MATCH envelope.
pub(crate) const MATCH_ENVELOPE_FRONTIER_KEY: &str = "frontier";

/// Encode a MATCH result into the DP→CP `{rows, frontier}` msgpack envelope.
///
/// `rows` are serialized exactly as [`rows_to_msgpack`] produces (an unchanged
/// bare msgpack array) and embedded as the `rows` map value. The
/// `unresolved_frontier` is zerompk-encoded (a msgpack array of
/// [`UnresolvedExpansion`]) and embedded as the `frontier` map value. Both are
/// already-valid msgpack values, so they are spliced in via `write_kv_raw`
/// without re-encoding.
pub(crate) fn encode_match_envelope(
    rows: &[BindingRow],
    frontier: &[UnresolvedExpansion],
) -> Result<Vec<u8>, crate::Error> {
    use nodedb_query::msgpack_scan::writer::{write_kv_raw, write_map_header};

    let rows_bytes = rows_to_msgpack(rows)?;
    let frontier_bytes =
        zerompk::to_msgpack_vec(&frontier.to_vec()).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("match frontier serialization: {e}"),
        })?;

    let mut buf = Vec::with_capacity(rows_bytes.len() + frontier_bytes.len() + 16);
    write_map_header(&mut buf, 2);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_ROWS_KEY, &rows_bytes);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_FRONTIER_KEY, &frontier_bytes);
    Ok(buf)
}

/// Build the `{rows, frontier}` envelope from an ALREADY-encoded bare rows
/// msgpack array plus frontier entries.
///
/// Mirrors [`encode_match_envelope`] but accepts pre-merged rows bytes (as
/// produced by `broadcast_match_to_all_cores`) instead of `&[BindingRow]`,
/// avoiding a redundant decode+re-encode round-trip.  The output is byte-
/// identical to what `encode_match_envelope` would produce for the same rows.
///
/// Called by `execute_plan_all_local_cores` in the MATCH branch to reconstruct
/// the single-shard envelope shape from a node-level `MatchBroadcastOutcome`.
pub(crate) fn encode_match_envelope_raw(
    rows_array: &[u8],
    frontier: &[UnresolvedExpansion],
) -> Result<Vec<u8>, crate::Error> {
    use nodedb_query::msgpack_scan::writer::{write_kv_raw, write_map_header};

    let frontier_bytes =
        zerompk::to_msgpack_vec(&frontier.to_vec()).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("match frontier serialization: {e}"),
        })?;

    let mut buf = Vec::with_capacity(rows_array.len() + frontier_bytes.len() + 16);
    write_map_header(&mut buf, 2);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_ROWS_KEY, rows_array);
    write_kv_raw(&mut buf, MATCH_ENVELOPE_FRONTIER_KEY, &frontier_bytes);
    Ok(buf)
}

impl CoreLoop {
    /// Encode a `MatchOutcome` into the DP→CP MATCH envelope and build the
    /// appropriate response (`partial` if the outcome was truncated, normal
    /// otherwise).
    ///
    /// The envelope is a 2-field msgpack map carrying BOTH the binding rows
    /// (exactly as [`rows_to_msgpack`] produces them — a bare msgpack array)
    /// AND the cross-shard `unresolved_frontier` (a zerompk-encoded array of
    /// [`UnresolvedExpansion`]):
    ///
    /// ```text
    /// { "rows": <rows msgpack array>, "frontier": <frontier msgpack array> }
    /// ```
    ///
    /// The Control Plane's `broadcast_match_to_all_cores` unwraps this map:
    /// it merges the `rows` subfields across cores back into the SAME bare
    /// array shape `match_payload_to_response` already expects, and unions the
    /// `frontier` entries for cross-shard continuation dispatch (B2). On a
    /// fully-local CSR the frontier array is empty, so single-node client
    /// behaviour after the unwrap is byte-identical to the prior bare-array
    /// response.
    ///
    /// Shared by [`execute_graph_match`] and [`execute_graph_match_continuation`]
    /// to avoid duplicating the encode → response tail.
    fn match_outcome_response(
        &self,
        task: &ExecutionTask,
        outcome: crate::engine::graph::pattern::executor::MatchOutcome,
    ) -> Response {
        match encode_match_envelope(&outcome.rows, &outcome.unresolved_frontier) {
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
        match encode_match_envelope(&[], &[]) {
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
        cluster_mode: bool,
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
        // In cluster mode the Data Plane has no routing knowledge, so it
        // cannot pre-filter which bound zero-degree sources are genuinely
        // remote. It emits ALL of them as frontier candidates (predicate
        // returns `true` for every node) and the Control Plane filters them
        // precisely via routing in B2. In single-node mode (`false`) no
        // predicate is supplied, so the frontier stays empty and the
        // response is byte-identical to today.
        let all_remote = |_: &str| true;
        let is_remote_node: Option<&dyn Fn(&str) -> bool> = if cluster_mode {
            Some(&all_remote)
        } else {
            None
        };
        match crate::engine::graph::pattern::executor::execute(
            &query,
            partition,
            &self.edge_store,
            frontier_bitmap,
            is_remote_node,
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
    /// A `MatchContinuation` ALWAYS runs cross-shard (it only exists because
    /// another shard emitted an `UnresolvedExpansion` routed here), so its
    /// remaining-pattern expansion MUST surface its OWN unresolved frontier:
    /// `is_remote_node = Some(&|_| true)`. This is what makes multi-round
    /// continuation work — deeper hops that again leave this shard's CSR are
    /// re-emitted as frontier entries for the Control-Plane coordinator to
    /// dispatch onward. The Control Plane filters them precisely via routing
    /// (dropping true local leaves), exactly as it does for the round-0
    /// `Match` frontier. No `cluster_mode` field is needed on
    /// `MatchContinuation` — the predicate is unconditionally `true` here.
    /// The response already envelopes `{rows, frontier}` via
    /// `match_outcome_response`.
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

        // A continuation only ever runs cross-shard, so it must surface its
        // own unresolved frontier (every bound zero-degree source becomes a
        // candidate; the Control Plane filters precisely via routing). This is
        // what enables multi-round continuation across >1 shard boundary.
        let all_remote = |_: &str| true;
        let is_remote_node: Option<&dyn Fn(&str) -> bool> = Some(&all_remote);
        match crate::engine::graph::pattern::executor::execute_continuation(
            &query,
            partition,
            &self.edge_store,
            None, // no anchor prefilter on the resume path
            is_remote_node,
            resume_triple_idx,
            seed_row,
        ) {
            Ok(outcome) => self.match_outcome_response(task, outcome),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
