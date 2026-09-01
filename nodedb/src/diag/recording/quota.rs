// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for quota rows that never reached live enforcement.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a persisted quota row whose stored value did not decode, so boot
/// replay left its scope uncapped. Called from the lossy listings' bad-key
/// arms; `scope` is `database` or `tenant`.
pub fn quota_row_undecodable(scope: &'static str, database_id: u64, tenant_id: Option<u64>) {
    let ctx = context::QuotaRowNotInstalled {
        cause: "undecodable",
        scope,
        database_id,
        tenant_id,
        detail: "stored value did not decode as a quota record",
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "quota replay: a persisted row did not decode, so its scope runs uncapped",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a persisted quota row that failed validation, so boot replay left
/// its scope uncapped. Called from the replay validation arm.
pub fn quota_row_invalid(
    err: &(dyn std::error::Error + 'static),
    scope: &'static str,
    database_id: u64,
    tenant_id: Option<u64>,
) {
    let detail = err.to_string();
    let ctx = context::QuotaRowNotInstalled {
        cause: "invalid_record",
        scope,
        database_id,
        tenant_id,
        detail: &detail,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "quota replay: a persisted row broke its invariants, so its scope runs uncapped",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a replicated per-scope token quota whose stored enforcement mode
/// this build cannot parse, so post-apply skips installing it. Called from
/// the post-apply `put` arm that logs and continues.
pub fn scope_quota_not_installed(err: &crate::Error, scope_name: &str, enforcement: &str) {
    let detail = err.to_string();
    let ctx = context::ScopeQuotaNotInstalled {
        scope_name,
        enforcement,
        detail: &detail,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "scope quota post-apply: enforcement mode did not parse, so the cap does not install",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a boot quota replay that could not list its catalog table, so no
/// row of that scope was installed. Called from each listing's error arm.
pub fn quota_scope_replay_aborted(err: &crate::Error, scope: &'static str) {
    let class = error_class(err);
    let ctx = context::QuotaScopeReplayAborted {
        scope,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "quota replay: catalog read failed, so every scope of this kind runs uncapped",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a quota row write or delete refused while applying a committed
/// catalog entry. Called from each apply arm that logs and continues.
pub fn quota_row_write_failed(
    err: &crate::Error,
    operation: &'static str,
    database_id: u64,
    tenant_id: Option<u64>,
) {
    let class = error_class(err);
    let ctx = context::QuotaRowWriteFailed {
        operation,
        database_id,
        tenant_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "quota apply: a committed row write failed, so this node diverges from consensus",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a dropped scope whose tenant quota rows could not be scanned, so
/// some survive the drop. Called from each purge scan's error arm.
pub fn quota_scope_purge_incomplete(
    err: &crate::Error,
    scope: &'static str,
    database_id: Option<u64>,
    tenant_id: Option<u64>,
) {
    let class = error_class(err);
    let ctx = context::QuotaScopePurgeIncomplete {
        scope,
        database_id,
        tenant_id,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "quota purge: tenant row scan failed, so rows of a dropped scope survive",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
