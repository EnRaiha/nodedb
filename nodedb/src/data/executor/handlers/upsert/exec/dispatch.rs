// SPDX-License-Identifier: BUSL-1.1

//! The upsert handler entry point: probe for an existing row and dispatch to
//! the overwrite branch ([`overwrite`]) or the insert branch ([`insert`]).
//!
//! Works for schemaless and strict collections. All internal transport
//! uses nodedb_types::Value + zerompk (msgpack). No JSON roundtrips.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook::HookCtx;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::Surrogate;

use super::insert::InsertCtx;
use super::overwrite::OverwriteCtx;

/// Parameters for `execute_upsert`.
pub(in crate::data::executor) struct UpsertParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    pub on_conflict_updates: &'a [(String, nodedb_physical::physical_plan::UpdateValue)],
    /// Compiled RLS write policy gating the PERSIST, decided against whichever
    /// body this call actually stores — the merged row on the conflict branch,
    /// the incoming body on the insert branch. Empty = no write policy.
    pub rls_write_check: &'a [u8],
    /// When `Some`, project the STORED post-image per spec: the merged row on
    /// the conflict branch, the inserted row otherwise. Never the submitted
    /// body — on a conflict the caller's values are only part of the result.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled read policy bounding which of those rows may be shown back.
    pub rls_filters: &'a [u8],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this upsert may touch, resolved on the Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Upsert: insert if absent, merge fields if present.
    ///
    /// If a document with `document_id` exists, merges `value` fields into the
    /// existing document (preserving fields not in `value`). If it doesn't exist,
    /// inserts as a new document (identical to PointPut).
    ///
    /// `value` is msgpack-encoded (zerompk). Strict collections decode binary
    /// tuples for existing docs, merge, and re-encode via `apply_point_put`.
    pub(in crate::data::executor) fn execute_upsert(
        &mut self,
        task: &ExecutionTask,
        params: UpsertParams<'_>,
    ) -> Response {
        let UpsertParams {
            tid,
            collection,
            document_id,
            surrogate,
            value,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
            resolved_sum_targets,
        } = params;
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            has_on_conflict = !on_conflict_updates.is_empty(),
            "upsert"
        );

        let database_id = task.request.database_id.as_u64();
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn: task.wal_lsn(),
        };

        // Detect strict storage mode for this collection.
        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let strict_schema = self.doc_configs.get(&config_key).and_then(|config| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

        // Check if document already exists. Bitemporal collections consult
        // the versioned table's current-state view (reverse-scan to newest
        // non-tombstone); non-bitemporal collections use the legacy point
        // lookup.
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        // Computed once for the whole statement: the schemaless half of this
        // check is an unindexed `vector_params` scan, so it must not be paid
        // per branch. Gates the live HNSW re-index + the post-apply redo
        // write-set below; a non-vector collection pays neither.
        let has_vectors = self.collection_has_vectors(database_id, tid, collection);
        let existing = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, row_key)
        } else {
            self.sparse.get(database_id, tid, collection, row_key)
        };

        match existing {
            Ok(Some(current_bytes)) => self.execute_upsert_overwrite(
                task,
                OverwriteCtx {
                    tid,
                    collection,
                    document_id,
                    surrogate,
                    row_key,
                    value,
                    on_conflict_updates,
                    rls_write_check,
                    returning,
                    rls_filters,
                    database_id,
                    hook_ctx: &hook_ctx,
                    has_vectors,
                    strict_schema: strict_schema.as_ref(),
                    current_bytes,
                },
            ),
            Ok(None) => self.execute_upsert_insert(
                task,
                InsertCtx {
                    tid,
                    collection,
                    document_id,
                    surrogate,
                    row_key,
                    value,
                    rls_write_check,
                    returning,
                    rls_filters,
                    database_id,
                    hook_ctx: &hook_ctx,
                    has_vectors,
                    strict_schema: strict_schema.as_ref(),
                },
            ),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
