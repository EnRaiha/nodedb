// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for retention policies whose auto-wired state outlived them.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report auto-wired tier aggregates that outlived the retention policy that
/// created them. Called from the DROP arm that logs and continues.
pub fn retention_autowire_orphaned(
    err: &crate::Error,
    database_id: u64,
    tenant_id: u64,
    policy: &str,
    collection: &str,
) {
    let class = error_class(err);
    let ctx = context::RetentionAutowireOrphaned {
        database_id,
        tenant_id,
        policy,
        collection,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "retention drop: tier aggregates were not unregistered, so they outlive the policy",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
