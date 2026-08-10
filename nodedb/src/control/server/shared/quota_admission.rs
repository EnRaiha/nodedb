// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch quota admission: refuse a caller who has already spent the
//! cap on an entitlement that covers this request.
//!
//! This is the enforcing half of the metering pair. Charging
//! ([`meter_dispatch`](super::metering::meter_dispatch)) happens AFTER a task
//! succeeds, because a denied or errored request performed no billable work —
//! which means it can never be the place a `QuotaEnforcement::Hard` cap
//! refuses. By the time it runs, the work it would have refused is done.
//!
//! So the refusal lives here, immediately before dispatch, and applies exactly
//! the same coverage rule the charge does: only a held scope whose grants
//! cover this request's `(permission, collection)` is consulted. Holding an
//! exhausted `vector:heavy` entitlement must not block an unrelated KV
//! point-get.
//!
//! The check asks "is this grantee already over the cap?" rather than
//! projecting this request's exact token cost. The cost depends on the row
//! count, which is not known until the task has run — and a cap is a statement
//! about consumption already incurred, not a reservation system.

use crate::control::security::metering::quota::QuotaStatus;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::state::SharedState;

// The coverage rule is shared with the charging path rather than restated
// here: if the two ever disagreed, a caller could be billed against a ledger
// that never refuses them, or refused by one that never bills them.
use super::metering::{PlanMeteringInfo, scope_covers_request};

/// Refuse the request when a covering scope's `Hard` quota is already spent.
///
/// Returns `Ok(())` when metering is disabled, when the caller is an internal
/// service (WAL replay, triggers, the scheduler, CRDT sync — server-owned work
/// is never billed and so is never capped), when the plan attributes to no
/// collection, or when no covering scope has an exhausted hard quota.
///
/// Non-`Hard` modes never refuse here: `Soft`, `Throttle`, and `Overage` are
/// deliberately permissive, and `check_quota` already emits their warning and
/// accounting side effects.
pub(crate) fn admit_quota_for_dispatch(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    info: &PlanMeteringInfo,
) -> crate::Result<()> {
    if !state.metering_config.enabled {
        return Ok(());
    }
    if scope.identity().is_internal_service() {
        return Ok(());
    }
    let Some(collection) = info.collection() else {
        return Ok(());
    };

    let auth = scope.auth();
    let now_secs = crate::control::security::time::now_secs();
    let effective = state.scope_grants.effective_scopes(&auth.id, &auth.org_ids);

    for scope_name in &effective {
        if !scope_covers_request(state, scope_name, info.permission(), collection) {
            continue;
        }
        // A scope with no definition returns `Ok` — `check_quota`'s own
        // "no quota defined → allow" path — so this loop costs nothing for
        // the overwhelmingly common uncapped scope.
        if let Err(status) = state
            .quota_manager
            .check_quota(scope_name, &auth.id, 0, now_secs)
        {
            return Err(quota_exceeded(&status));
        }
    }

    Ok(())
}

/// Build the refusal for an exhausted hard quota.
///
/// The message names the scope, the cap, and the consumption, because the
/// caller's only remedy is to know which entitlement ran out — "quota
/// exceeded" alone leaves an operator to guess among every scope they hold.
fn quota_exceeded(status: &QuotaStatus) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!(
            "quota exceeded on scope '{}': {} of {} tokens used this period",
            status.scope_name, status.used_tokens, status.max_tokens
        ),
    }
}
