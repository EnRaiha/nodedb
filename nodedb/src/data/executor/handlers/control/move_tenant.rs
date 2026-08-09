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

/// Parameters for [`CoreLoop::execute_rename_collection`].
pub(in crate::data::executor) struct RenameCollectionParams<'a> {
    pub tenant_id: u64,
    pub old_database_id: u64,
    pub new_database_id: u64,
    pub old_collection: &'a str,
    pub new_collection: &'a str,
}

impl CoreLoop {
    /// Handle `MetaOp::RenameCollection`: re-key all documents and secondary
    /// indexes from `old_collection` to `new_collection` for `tenant_id` in
    /// every engine that uses db-qualified collection names for keying.
    pub(in crate::data::executor) fn execute_rename_collection(
        &mut self,
        task: &ExecutionTask,
        params: RenameCollectionParams<'_>,
    ) -> Response {
        let RenameCollectionParams {
            tenant_id,
            old_database_id,
            new_database_id,
            old_collection,
            new_collection,
        } = params;
        // Sparse engine (document schemaless + document strict).
        if let Err(e) = self.sparse.rename_collection(
            old_database_id,
            new_database_id,
            tenant_id,
            old_collection,
            new_collection,
        ) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "rename_collection sparse ({old_collection} -> {new_collection}): {e}"
                    ),
                },
            );
        }

        // The sparse rename moved this collection's persisted hash-chain head
        // along with its rows; move the in-memory head in lockstep so the two
        // never disagree and the next INSERT under the new name chains from the
        // row that preceded it instead of restarting at genesis.
        let old_chain_key = (
            crate::types::DatabaseId::new(old_database_id),
            crate::types::TenantId::new(tenant_id),
            old_collection.to_string(),
        );
        if let Some(head) = self.chain_hashes.remove(&old_chain_key) {
            self.chain_hashes.insert(
                (
                    crate::types::DatabaseId::new(new_database_id),
                    crate::types::TenantId::new(tenant_id),
                    new_collection.to_string(),
                ),
                head,
            );
        }

        // KV engine.
        self.kv_engine
            .rename_collection(crate::engine::kv::RenameCollectionParams {
                old_database_id,
                new_database_id,
                tenant_id,
                old_collection,
                new_collection,
            });

        self.response_ok(task)
    }
}
