// SPDX-License-Identifier: BUSL-1.1

//! The rows a predicate document write matches: the base rows its filters
//! match, and, inside a transaction, that transaction's own staged writes.

use std::cell::Cell;

use crate::bridge::envelope::ErrorCode;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::StorageKey;
use crate::types::TenantId;

use super::context::DocResolveCtx;

impl CoreLoop {
    /// Every `(row key, current body)` that `filters` match in `collection`.
    ///
    /// A request that carries a transaction id reads its own writes: a
    /// staged delete drops its row, a staged put is matched again against
    /// `filters`, and a row only the transaction wrote joins the set.
    pub(super) fn resolve_matched_rows(
        &self,
        task: &ExecutionTask,
        ctx: &DocResolveCtx,
        collection: &str,
        filters: &[ScanFilter],
    ) -> Result<Vec<(StorageKey, Vec<u8>)>, ErrorCode> {
        let keys = self
            .scan_matching_documents(ctx.database_id, ctx.tid, collection, filters)
            .map_err(ErrorCode::from)?;
        let mut rows = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(stored) = self.doc_resolve_read(ctx, collection, &key)? {
                rows.push((key, stored));
            }
        }
        let Some(txn_id) = task.request.txn_id else {
            return Ok(rows);
        };

        let coll_key = (
            task.request.database_id,
            TenantId::new(ctx.tid),
            collection.to_string(),
        );
        let raw_matches = self.strict_aware_matcher(ctx.database_id, ctx.tid, collection, filters);
        // The merge takes an infallible predicate, so an evaluation error
        // (division by zero) is held here and raised once the merge returns.
        let predicate_err: Cell<Option<nodedb_query::EvalError>> = Cell::new(None);
        let matches = |row_key: &StorageKey, body: &[u8]| match raw_matches(row_key, body) {
            Ok(matched) => matched,
            Err(e) => {
                predicate_err.set(Some(e));
                false
            }
        };
        self.merge_overlay_into_scan(txn_id, &coll_key, &mut rows, &matches);
        if let Some(e) = predicate_err.take() {
            return Err(ErrorCode::from(crate::Error::from(e)));
        }
        Ok(rows)
    }
}
