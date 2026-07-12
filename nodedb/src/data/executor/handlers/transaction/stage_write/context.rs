// SPDX-License-Identifier: BUSL-1.1

//! Shared routing context for a single staged point write.

use std::borrow::Cow;

use nodedb_types::Surrogate;

use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Collection overlay key: `(database, tenant, collection)`.
pub(super) type CollKey = (DatabaseId, TenantId, String);

/// The invariant routing identity of one staged point write, bundled so the
/// per-op helpers stay within a sane argument count.
///
/// `document_id` is the overlay's doc-id key: for Document ops it borrows
/// the plan's own document id; for KV ops (which have no document id) it
/// owns the [`hex_key`](super::stage_kv::hex_key)-encoded KV key instead --
/// `Cow` lets both engines share this one context type without allocating
/// on the Document path or leaking on the KV path.
pub(in crate::data::executor) struct StageCtx<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub database_id: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub document_id: Cow<'a, str>,
    pub surrogate: Surrogate,
    pub coll_key: CollKey,
}

impl<'a> StageCtx<'a> {
    pub(in crate::data::executor) fn new(
        task: &'a ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &'a str,
        document_id: impl Into<Cow<'a, str>>,
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
            document_id: document_id.into(),
            surrogate,
            coll_key,
        }
    }
}
