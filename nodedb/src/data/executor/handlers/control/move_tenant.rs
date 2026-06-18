// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `MetaOp::RenameCollection`.
//!
//! Called after `MoveTenantCutover` applies so that physical data is
//! accessible under the new database context.  Re-keys all documents and
//! secondary indexes in the sparse engine (document / strict-document engines)
//! and the KV engine from the old db-qualified collection name to the new one.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `MetaOp::RenameCollection`: re-key all documents and secondary
    /// indexes from `old_collection` to `new_collection` for `tenant_id` in
    /// every engine that uses db-qualified collection names for keying.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_rename_collection(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        old_database_id: u64,
        new_database_id: u64,
        old_collection: &str,
        new_collection: &str,
    ) -> Response {
        // Sparse engine (document schemaless + document strict).
        if let Err(e) = self
            .sparse
            .rename_collection(tenant_id, old_collection, new_collection)
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "rename_collection sparse ({old_collection} -> {new_collection}): {e}"
                    ),
                },
            );
        }

        // KV engine.
        self.kv_engine.rename_collection(
            old_database_id,
            new_database_id,
            tenant_id,
            old_collection,
            new_collection,
        );

        self.response_ok(task)
    }
}
