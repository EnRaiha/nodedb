// SPDX-License-Identifier: BUSL-1.1

//! Apply one DDL side-effect in the Data Plane and surface its verdict.
//!
//! A Data-Plane rejection arrives as an `Ok(Response)` carrying a non-`Ok`
//! status, so a caller that only handles the transport `Result` reports a
//! refused configuration as a successful statement. Index DDL routes its
//! engine registration through here so the refusal always reaches the client.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::result::DdlError;
use super::sync_dispatch::{SystemReason, SystemTask, dispatch_system};

/// Dispatch `plan` for `collection` and translate any Data-Plane refusal into
/// a [`DdlError`] carrying `sqlstate` and `context`.
pub(crate) async fn apply_in_engine(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    sqlstate: &str,
    context: &str,
) -> Result<(), DdlError> {
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    dispatch_system(
        state,
        SystemTask::new(
            SystemReason::DdlApply,
            tenant_id,
            database_id,
            collection,
            plan,
        ),
        timeout,
    )
    .await
    .map(|_| ())
    .map_err(|e| DdlError {
        sqlstate: sqlstate.to_string(),
        message: format!("{context}: {e}"),
    })
}
