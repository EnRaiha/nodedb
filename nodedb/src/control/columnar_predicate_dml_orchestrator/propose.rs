// SPDX-License-Identifier: BUSL-1.1

//! Propose the resolved `ColumnarOp::ResolvedUpdate` / `ResolvedDelete` plan
//! through Raft, exactly as an ordinary replicated write is proposed.

use std::sync::atomic::Ordering;

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::server::dispatch_utils::publish_origin_change_events;
use crate::control::state::SharedState;
use crate::control::wal_replication::{propose_replicated_entry, to_replicated_entry};
use crate::types::{RequestId, VShardId};

/// One propose attempt's outcome.
pub(super) enum ProposeOutcome {
    /// Committed and applied; carries the response the statement returns.
    Applied(Response),
    /// The shipped row set no longer matches current state (concurrent
    /// drift). Nothing was applied — re-resolve and retry.
    RetryRequired,
}

/// Propose `plan` (a `ColumnarOp::ResolvedUpdate` / `ResolvedDelete`) through
/// the live Raft proposer and await commit + apply.
///
/// Only reachable when `state.async_raft_proposer()` is `Some` — the
/// interception in `orchestrator::is_governed_columnar_predicate_dml`
/// combined with the caller's own `async_raft_proposer().is_some()` check is
/// what guarantees that; an absent proposer here is an internal invariant
/// break, not a runtime condition to handle gracefully.
pub(super) async fn propose_resolved(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
) -> crate::Result<ProposeOutcome> {
    let proposer = state
        .async_raft_proposer()
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "columnar predicate DML orchestrator invoked for '{collection}' with no active \
                 Raft proposer; this path is only reachable when async_raft_proposer().is_some()"
            ),
        })?;
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    let entry =
        to_replicated_entry(tenant_id, database_id, vshard_id, &plan)?.ok_or_else(|| {
            crate::Error::Internal {
                detail: format!(
                    "columnar predicate DML: resolved plan for '{collection}' did not map to a \
                 replicated write"
                ),
            }
        })?;

    match propose_replicated_entry(state, proposer, entry).await {
        Ok((payload, write_version)) => {
            let request_id =
                RequestId::new(state.request_id_counter.fetch_add(1, Ordering::Relaxed));
            let response = Response {
                request_id,
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: payload.into(),
                watermark_lsn: write_version,
                error_code: None,
                read_set_valid: None,
                read_version_lsn: write_version,
                write_set: Vec::new(),
            };
            // Mirrors `dispatch_replicated_write`: the proposing node is the
            // one node that handled this write exactly once, so it is the one
            // that publishes the CDC change event.
            publish_origin_change_events(state, tenant_id, database_id, &plan, &response);
            Ok(ProposeOutcome::Applied(response))
        }
        Err(crate::Error::DataPlane(ErrorCode::OllpRetryRequired)) => {
            Ok(ProposeOutcome::RetryRequired)
        }
        Err(e) => Err(e),
    }
}
