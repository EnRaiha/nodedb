// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for vector-index work that never reached the Data Plane.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report per-node vector-index work that failed while applying a committed
/// catalog entry. Called from the vector post-apply arms, which cannot
/// propagate: `stage` names which of the WAL append, fsync, or Data Plane
/// dispatch was lost.
pub fn vector_index_not_applied(
    err: &crate::Error,
    stage: &'static str,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    field_name: &str,
) {
    let class = error_class(err);
    let ctx = context::VectorIndexNotApplied {
        stage,
        database_id,
        tenant_id,
        collection,
        field_name,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "vector index post-apply: the committed index change never reached this node's \
         Data Plane",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
