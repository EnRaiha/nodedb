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
/// Returns the remaining budget for the local half of the statement and the
/// request's `DatabaseId` on success, or a typed cluster error to surface to
/// the caller.
///
/// The budget is the one the coordinator sent. It resolved the statement's
/// deadline once, from the session's `statement_timeout` or its own
/// `default_deadline_secs`, and every hop of that statement carries what is
/// left of it. Re-deciding it here against this node's own default would give
/// one statement two budgets, so a session timeout longer than the default
/// would hold on the node that parsed it and shrink on every other node.
pub(super) fn validate_request(
    state: &SharedState,
    req: &ExecuteRequest,
) -> Result<(Duration, DatabaseId), TypedClusterError> {
    // ── 1. Deadline check ─────────────────────────────────────────────────
    let deadline = hop_budget(req.deadline_remaining_ms)?;

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

/// The local budget for one hop, from the remaining budget the coordinator
/// sent on the wire.
///
/// Zero means the statement is already out of time and nothing runs here. Any
/// other value is used as it arrived.
fn hop_budget(deadline_remaining_ms: u64) -> Result<Duration, TypedClusterError> {
    if deadline_remaining_ms == 0 {
        return Err(TypedClusterError::DeadlineExceeded { elapsed_ms: 0 });
    }
    Ok(Duration::from_millis(deadline_remaining_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_remaining_budget_is_already_out_of_time() {
        assert!(matches!(
            hop_budget(0),
            Err(TypedClusterError::DeadlineExceeded { elapsed_ms: 0 })
        ));
    }

    #[test]
    fn a_budget_longer_than_the_node_default_survives_the_hop() {
        // `SET statement_timeout = '120s'` on the coordinator reaches here as
        // 120_000ms. The receiving node runs the statement on that budget: its
        // own `default_deadline_secs` names the budget for a statement that
        // brings none, and is not a ceiling on one that does.
        let configured = nodedb_types::config::tuning::NetworkTuning::default();
        let session_budget_ms = configured.default_deadline_secs * 1_000 * 4;
        assert_eq!(
            hop_budget(session_budget_ms).expect("a live budget is accepted"),
            Duration::from_millis(session_budget_ms)
        );
    }

    #[test]
    fn a_budget_shorter_than_the_node_default_survives_the_hop() {
        assert_eq!(
            hop_budget(250).expect("a live budget is accepted"),
            Duration::from_millis(250)
        );
    }
}
