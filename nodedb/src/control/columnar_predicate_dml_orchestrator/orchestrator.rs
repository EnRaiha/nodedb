// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for a governed columnar predicate
//! `UPDATE`/`DELETE` under Raft replication.
//!
//! A collection carrying an RLS write policy cannot replicate a bare
//! predicate `UPDATE`/`DELETE`: the leader would have to re-decide the policy
//! after commit (rejecting what followers already applied — divergence), and
//! a follower has no writing identity to evaluate `$auth.*` against (silent
//! bypass either way). `wal_replication::encode::entry_columnar_family`'s
//! `refuse_governed_predicate_dml` already fails such a write loudly rather
//! than replicate it unsafely — this orchestrator is what makes it succeed
//! again, correctly:
//!
//! 1. **Resolve**: dispatch `ColumnarOp::ResolveDml`
//!    ([`resolve::resolve_dml`]) to the Data Plane over the normal SPSC
//!    dispatch path — an ordinary read, not a plane violation. The Data
//!    Plane scans the target collection, applies the statement's `WHERE`
//!    filters (and, for an UPDATE, its assignments) against the native
//!    rows it holds, and decides the write policy against every match's
//!    exact image with the same evaluator a direct predicate DML uses.
//! 2. **Build the resolved op**: `ColumnarOp::ResolvedUpdate` /
//!    `ResolvedDelete`, carrying the resolved row set unchanged and
//!    `RlsWriteCheck::decided_earlier_in_request()` — this same request
//!    already admitted these exact row images.
//! 3. **Propose** ([`propose::propose_resolved`]) through the same Raft
//!    proposer an ordinary replicated write uses.
//! 4. **Retry on drift**: the Data Plane apply returns
//!    `ErrorCode::OllpRetryRequired` (without applying) when the shipped row
//!    set no longer matches current state; re-resolve from step 1 and
//!    propose again, bounded by [`MAX_COLUMNAR_PREDICATE_DML_RETRIES`].
//!
//! A row the resolve step's write-policy gate refuses surfaces as this
//! statement's error unchanged — see [`resolve::resolve_dml`].
//!
//! Only reachable when `state.async_raft_proposer().is_some()` — see
//! [`is_governed_columnar_predicate_dml`]. On the local (non-Raft, single
//! node) path the predicate reaches the Data Plane intact and the gate
//! enforces it correctly today; resolving there would only add cost with no
//! correctness benefit, so that path is never routed through this
//! orchestrator.

use nodedb_types::{DatabaseId, RlsWriteCheck, TenantId};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::ColumnarOp;

use super::propose::{self, ProposeOutcome};
use super::resolve::{self, ResolveDmlArgs, ResolvedDml};

/// Attempts a governed columnar predicate `UPDATE`/`DELETE` makes before a
/// resolution that keeps drifting under concurrent writes is reported rather
/// than retried forever. Mirrors `update_from_join_orchestrator`'s bound: the
/// retry exists to absorb concurrent drift between resolve and apply, not to
/// mask a resolution that can never converge.
const MAX_COLUMNAR_PREDICATE_DML_RETRIES: u32 = 8;

/// True when `plan` is a `ColumnarOp::Update` / `ColumnarOp::Delete` carrying
/// a live RLS write predicate — the shape this orchestrator exists to
/// resolve before it reaches Raft replication.
///
/// This alone is not the routing condition: a caller must also check
/// `state.async_raft_proposer().is_some()`. On the local (non-Raft) path the
/// predicate reaches the Data Plane gate intact and is enforced correctly
/// there, so routing it through this orchestrator would only add cost.
pub fn is_governed_columnar_predicate_dml(plan: &PhysicalPlan) -> bool {
    let PhysicalPlan::Columnar(op) = plan else {
        return false;
    };
    match op {
        ColumnarOp::Update {
            rls_write_check, ..
        }
        | ColumnarOp::Delete {
            rls_write_check, ..
        } => rls_write_check.has_predicate(),
        ColumnarOp::Insert { .. }
        | ColumnarOp::Scan { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::ResolveDml { .. }
        | ColumnarOp::MaterializeScan { .. } => false,
    }
}

/// An `UPDATE`'s field assignments, or a plain `DELETE`.
enum DmlAssignment {
    Update(Vec<(String, Vec<u8>)>),
    Delete,
}

/// Bundled, already-extracted fields of the intercepted governed columnar
/// predicate `UPDATE` / `DELETE`.
struct ColumnarPredicateDmlArgs<'a> {
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &'a str,
    filters: &'a [u8],
    assignment: &'a DmlAssignment,
    rls_write_check: &'a RlsWriteCheck,
}

/// Consume an authorized, governed, replicated columnar predicate
/// `UPDATE`/`DELETE` at the orchestration boundary.
pub async fn run_authorized_columnar_predicate_dml(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> crate::Result<Response> {
    let bad_shape = || crate::Error::BadRequest {
        detail: "authorized task is not a governed columnar predicate UPDATE/DELETE".into(),
    };
    let task = authorized.into_physical_task();
    let PhysicalPlan::Columnar(op) = task.plan else {
        return Err(bad_shape());
    };
    let (collection, filters, assignment, rls_write_check) = match op {
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check,
        } => (
            collection,
            filters,
            DmlAssignment::Update(updates),
            rls_write_check,
        ),
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check,
        } => (collection, filters, DmlAssignment::Delete, rls_write_check),
        ColumnarOp::Insert { .. }
        | ColumnarOp::Scan { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::ResolveDml { .. }
        | ColumnarOp::MaterializeScan { .. } => return Err(bad_shape()),
    };
    if !rls_write_check.has_predicate() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "authorized task for '{collection}' reached the columnar predicate DML \
                 orchestrator without a live RLS write predicate"
            ),
        });
    }

    let tenant_id = task.tenant_id;
    run_columnar_predicate_dml(
        state,
        ColumnarPredicateDmlArgs {
            tenant_id,
            database_id: task.database_id,
            collection: &collection,
            filters: &filters,
            assignment: &assignment,
            rls_write_check: &rls_write_check,
        },
    )
    .await
    .map_err(|e| surface_policy_refusal(e, tenant_id))
}

/// Turn the Data Plane's policy refusal into the Control Plane's own
/// authorization error.
///
/// The resolve step runs the write gate inside the Data Plane, so a refused
/// row comes back wrapped as `Error::DataPlane`. That wrapper's `Display` is
/// the debug form of the code, which would put an internal enum on the wire.
/// A refusal reaches the client through the same error a directly dispatched
/// statement uses, so the SQLSTATE and the message stay the same either way.
fn surface_policy_refusal(error: crate::Error, tenant_id: TenantId) -> crate::Error {
    match error {
        crate::Error::DataPlane(ErrorCode::RejectedAuthz { resource }) => {
            crate::Error::RejectedAuthz {
                tenant_id,
                resource,
            }
        }
        other => other,
    }
}

/// Drive the resolve → propose → retry-on-drift loop.
async fn run_columnar_predicate_dml(
    state: &SharedState,
    args: ColumnarPredicateDmlArgs<'_>,
) -> crate::Result<Response> {
    let (updates, is_update): (&[(String, Vec<u8>)], bool) = match args.assignment {
        DmlAssignment::Update(updates) => (updates.as_slice(), true),
        DmlAssignment::Delete => (&[], false),
    };

    let mut attempt: u32 = 0;
    loop {
        let resolved = resolve::resolve_dml(
            state,
            ResolveDmlArgs {
                tenant_id: args.tenant_id,
                database_id: args.database_id,
                collection: args.collection,
                filters: args.filters,
                updates,
                is_update,
                rls_write_check: args.rls_write_check,
            },
        )
        .await?;

        let resolved_plan = PhysicalPlan::Columnar(match resolved {
            ResolvedDml::Update(rows) => ColumnarOp::ResolvedUpdate {
                collection: args.collection.to_string(),
                rows,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            },
            ResolvedDml::Delete(pks) => ColumnarOp::ResolvedDelete {
                collection: args.collection.to_string(),
                pks,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            },
        });

        match propose::propose_resolved(
            state,
            args.tenant_id,
            args.database_id,
            args.collection,
            resolved_plan,
        )
        .await?
        {
            ProposeOutcome::Applied(response) => return Ok(response),
            ProposeOutcome::RetryRequired => {
                attempt += 1;
                if attempt > MAX_COLUMNAR_PREDICATE_DML_RETRIES {
                    return Err(crate::Error::OllpExhausted {
                        retries: MAX_COLUMNAR_PREDICATE_DML_RETRIES.min(u8::MAX as u32) as u8,
                    });
                }
                continue;
            }
        }
    }
}
