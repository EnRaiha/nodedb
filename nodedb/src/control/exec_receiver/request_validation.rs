// SPDX-License-Identifier: BUSL-1.1

//! Validates an incoming `ExecuteRequest` before it is decoded: deadline
//! budget and per-collection descriptor version agreement with the caller.

use std::time::Duration;

use nodedb_cluster::rpc_codec::{ExecuteRequest, TypedClusterError};

use crate::control::gateway::version_check::{DescriptorCheckError, check_descriptor_versions};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::support::PLAN_DECODE_FAILED;

/// Validates the RPC deadline and descriptor versions carried on `req`.
///
/// Returns the clamped local deadline and the request's `DatabaseId` on
/// success, or a typed cluster error to surface to the caller.
pub(super) fn validate_request(
    state: &SharedState,
    req: &ExecuteRequest,
) -> Result<(Duration, DatabaseId), TypedClusterError> {
    // ── 1. Deadline check ─────────────────────────────────────────────────
    if req.deadline_remaining_ms == 0 {
        return Err(TypedClusterError::DeadlineExceeded { elapsed_ms: 0 });
    }

    let deadline = Duration::from_millis(req.deadline_remaining_ms).min(Duration::from_secs(
        state.tuning.network.default_deadline_secs,
    ));

    let database_id = DatabaseId::from(req.database_id);

    // ── 2. Descriptor version validation ──────────────────────────────────
    let catalog_ref = state.credentials.catalog();
    check_descriptor_versions(
        catalog_ref,
        database_id,
        req.tenant_id,
        req.descriptor_versions
            .iter()
            .map(|entry| (entry.collection.as_str(), entry.version)),
    )
    .map_err(|e| match e {
        DescriptorCheckError::VersionMismatch {
            collection,
            expected_version,
            actual_version,
        } => TypedClusterError::DescriptorMismatch {
            collection,
            expected_version,
            actual_version,
        },
        DescriptorCheckError::CatalogLookup { detail, .. } => TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: format!("catalog lookup failed: {detail}"),
        },
    })?;

    Ok((deadline, database_id))
}
