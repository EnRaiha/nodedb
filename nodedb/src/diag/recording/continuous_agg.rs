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

/// Report continuous-aggregate state that could not move between memory
/// and its image, or a tier watermark payload that does not decode. Called
/// from the site that detects it; the caller handles the error as the
/// `stage` describes.
pub fn continuous_aggregate_state_fault(
    err: &dyn std::error::Error,
    stage: &'static str,
    database_id: u64,
    tenant_id: u64,
    subject: &str,
) {
    let class = error_class(err);
    let ctx = context::ContinuousAggregateStateFault {
        stage,
        database_id,
        tenant_id,
        subject,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "continuous aggregate state did not move between memory and disk",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
