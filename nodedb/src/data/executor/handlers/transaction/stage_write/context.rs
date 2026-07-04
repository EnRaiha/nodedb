// SPDX-License-Identifier: BUSL-1.1

//! Shared routing context for a single staged point write.

use nodedb_types::Surrogate;

use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Collection overlay key: `(database, tenant, collection)`.
pub(super) type CollKey = (DatabaseId, TenantId, String);

/// The invariant routing identity of one staged point write, bundled so the
/// per-op helpers stay within a sane argument count.
pub(super) struct StageCtx<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub database_id: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub coll_key: CollKey,
}

impl<'a> StageCtx<'a> {
    pub(super) fn new(
        task: &'a ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &'a str,
        document_id: &'a str,
        surrogate: Surrogate,
    ) -> Self {
        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        Self {
            task,
            tid,
            database_id: task.request.database_id.as_u64(),
            txn_id,
            collection,
            document_id,
            surrogate,
            coll_key,
        }
    }
}
