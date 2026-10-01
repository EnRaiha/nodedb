// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for continuous-aggregate changes that never reached every
//! core on this node.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a committed continuous-aggregate change that did not reach every
/// core. Called from the post-apply arm, which cannot propagate: `stage`
/// names which part of the work was lost.
pub fn continuous_aggregate_not_applied(
    err: &crate::Error,
    stage: &'static str,
    database_id: u64,
    tenant_id: u64,
    aggregate: &str,
) {
    let class = error_class(err);
    let ctx = context::ContinuousAggregateNotApplied {
        stage,
        database_id,
        tenant_id,
        aggregate,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "continuous aggregate post-apply: the committed change never reached every core \
         on this node",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
