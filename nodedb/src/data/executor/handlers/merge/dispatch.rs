// SPDX-License-Identifier: BUSL-1.1

//! The MERGE request shape and the choice of which pass runs it.
//!
//! Its own file because the mode selection is the one thing every MERGE path
//! shares and none of them owns: [`MergeParams`] is filled in by the Data-Plane
//! dispatcher, read by the orchestrated RESOLVE and APPLY passes in
//! `merge_orchestrated`, and consumed by the legacy walk here. Keeping the
//! struct and the three-way branch apart from any one pass is what stops a
//! future field from being interpreted differently by whichever pass happens to
//! declare it.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::document::merge_types::MergeClauseOp;

/// Parameters for `execute_merge`.
pub(in crate::data::executor) struct MergeParams<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub clauses: &'a [MergeClauseOp],
    /// RESOLVE-ONLY read pass (orchestrator phase 1): classify without writing
    /// and return the NOT-MATCHED insert rows.
    pub resolve_only: bool,
    /// Control-Plane-pre-assigned surrogates for the NOT-MATCHED insert rows,
    /// keyed by source join value (orchestrator phase 3). `Some` selects the
    /// atomic verify-and-apply path; `None` (with `resolve_only == false`)
    /// selects the legacy per-row apply, retained only as a fallback — the
    /// in-transaction MERGE that once used it is now resolved + staged at
    /// statement time into concrete point ops
    /// (`control::server::shared::session::expander_stage`).
    pub resolved_inserts: Option<&'a [(String, u32)]>,
    /// Control-Plane-shipped source rows for cross-core MERGE. When `Some`, the
    /// source join-map is built from these pre-scanned
    /// `(source_doc_id, raw_stored_source_bytes)` rows instead of a local read
    /// of the source collection (whose vShard may live on a different core).
    /// `None` selects the legacy local-storage read (co-resident / in-txn
    /// buffered replay).
    pub source_rows: Option<&'a [(String, Vec<u8>)]>,
    /// Projection for a `MERGE ... RETURNING`. Honoured only by the orchestrated
    /// apply pass — the only pass that walks every arm's row bodies. `None`
    /// selects the affected-count payload.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled RLS read policy of the TARGET collection, gating the
    /// `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy of the TARGET collection, gating the PERSIST.
    /// Every arm writes a target row, decided against the image it stores: the
    /// post-image for an UPDATE or INSERT arm, the pre-image for a DELETE arm.
    /// A separate slot from `rls_filters`: that one bounds what may be shown
    /// back, this one bounds what may be written. Empty = no write policy.
    pub rls_write_check: &'a [u8],
}

impl CoreLoop {
    /// Execute a MERGE statement.
    ///
    /// Three modes, selected by [`MergeParams`]:
    /// - `resolve_only` → [`Self::execute_merge_resolve`]: a read pass that
    ///   returns the NOT-MATCHED insert rows for Control-Plane surrogate
    ///   assignment (no writes).
    /// - `resolved_inserts.is_some()` → [`Self::execute_merge_apply`]: the
    ///   atomic apply with CP-assigned surrogates + resolve→apply drift verify.
    /// - otherwise → `execute_merge_legacy`: the per-row apply retained only as
    ///   a fallback (in-transaction MERGE is now expanded at COMMIT into
    ///   concrete point ops before it could reach this path).
    pub(in crate::data::executor) fn execute_merge(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        if params.resolve_only {
            return self.execute_merge_resolve(task, tid, params);
        }
        if params.resolved_inserts.is_some() {
            return self.execute_merge_apply(task, tid, params);
        }
        self.execute_merge_legacy(task, tid, params)
    }
}
