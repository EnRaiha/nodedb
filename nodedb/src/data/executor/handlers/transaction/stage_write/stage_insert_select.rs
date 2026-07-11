// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for `INSERT INTO <target> SELECT ... FROM <source>
//! WHERE <predicate>` (`DocumentOp::InsertSelect`) inside a transaction.
//!
//! Mirrors the predicate `UPDATE` / `DELETE` staging in `stage_bulk_update.rs`
//! / `stage_bulk_delete.rs`: the SOURCE collection's current BASE ∪ OVERLAY
//! matching set is resolved via the shared [`CoreLoop::stage_bulk_base_rows`]
//! base scan plus [`CoreLoop::merge_overlay_into_scan`], so a source row
//! staged earlier in the same transaction is copied too. Each matched row is
//! recorded into the TARGET collection's overlay as a `Put`, keyed by the
//! SAME surrogate/doc_id the source row carries — an exact copy, not a fresh
//! insert. Nothing is written durably here; COMMIT's buffered plan replay
//! remains the sole durable apply.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Routing identity + payload for one staged `InsertSelect`, bundled to keep
/// the entry point within the argument-count budget.
pub(in crate::data::executor) struct StageInsertSelectParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub filter_bytes: &'a [u8],
    pub source_limit: usize,
}

impl CoreLoop {
    /// Stage `INSERT ... SELECT` at statement time: resolve the SOURCE
    /// collection's current BASE ∪ OVERLAY matching set, truncate to the
    /// SELECT's limit, and copy each matched row into the TARGET collection's
    /// overlay under the source row's own surrogate/doc_id. Returns
    /// `{"inserted": N}` in the same shape the Control-Plane `INSERT ... SELECT`
    /// orchestrator returns for the autocommit path.
    pub(in crate::data::executor) fn stage_insert_select(
        &mut self,
        params: StageInsertSelectParams<'_>,
    ) -> Response {
        let StageInsertSelectParams {
            task,
            tid,
            txn_id,
            target_collection,
            source_collection,
            filter_bytes,
            source_limit,
        } = params;
        let database_id = task.request.database_id;
        let source_coll_key: (DatabaseId, TenantId, String) = (
            database_id,
            TenantId::new(tid),
            source_collection.to_string(),
        );
        let target_coll_key: (DatabaseId, TenantId, String) = (
            database_id,
            TenantId::new(tid),
            target_collection.to_string(),
        );

        let filters: Vec<ScanFilter> = if filter_bytes.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filter_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("deserialize source filters: {e}"),
                        },
                    );
                }
            }
        };

        // SOURCE matching set: the same scan-and-filter primitive the bulk
        // predicate staging paths use, folded with the transaction's own
        // staged writes so a row inserted/updated earlier in this same
        // transaction is visible to the copy.
        let mut rows = match self.stage_bulk_base_rows(
            task,
            database_id.as_u64(),
            tid,
            source_collection,
            &filters,
        ) {
            Ok(rows) => rows,
            Err(resp) => return resp,
        };

        {
            let matches = self.strict_aware_matcher(tid, source_collection, &filters);
            self.merge_overlay_into_scan(txn_id, &source_coll_key, &mut rows, &matches);
        }
        rows.truncate(source_limit);

        let mut inserted = 0u64;
        for (row_key, body) in &rows {
            let Ok(surrogate) = u32::from_str_radix(row_key, 16) else {
                continue;
            };
            if let Err(e) = self.stage_bulk_put_capped(
                txn_id,
                &target_coll_key,
                surrogate,
                row_key,
                body.clone(),
            ) {
                return self.response_error(task, e);
            }
            inserted += 1;
        }

        match response_codec::encode_json(&serde_json::json!({ "inserted": inserted })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
