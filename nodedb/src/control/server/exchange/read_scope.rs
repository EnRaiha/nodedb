// SPDX-License-Identifier: BUSL-1.1

//! Request scope threaded through plan resolution and every gather it runs.

use crate::types::{DatabaseId, TenantId, TraceId, TxnId};

/// Who a distributed read runs for, and what it must observe.
#[derive(Debug, Clone, Copy)]
pub struct ReadScope {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub trace_id: TraceId,
    /// The session transaction the read runs in, for its staging overlay.
    /// `None` for autocommit and internal reads.
    pub txn_id: Option<TxnId>,
    /// Every leg observes all writes committed before the read began: the
    /// node serving each leg confirms the leg's group first (see
    /// `control::cluster::linearizable_read`).
    pub linearizable: bool,
}
