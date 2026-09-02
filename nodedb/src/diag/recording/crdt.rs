// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for CRDT history work that never reached the Data Plane.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report the per-node oplog compaction that failed while applying a
/// committed `CompactHistory`. Called from the post-apply arm, which
/// cannot propagate: `stage` names which part of the work was lost.
pub fn history_compaction_not_applied(
    err: &crate::Error,
    stage: &'static str,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) {
    let class = error_class(err);
    let ctx = context::HistoryCompactionNotApplied {
        stage,
        database_id,
        tenant_id,
        collection,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "history post-apply: the committed COMPACT HISTORY never reached this node's \
         Data Plane",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
