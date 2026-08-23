// SPDX-License-Identifier: BUSL-1.1

//! Decodes the wire-encoded `PhysicalPlan`, re-resolves an unresolved
//! point-get primary-key surrogate against the local catalog, and rejects
//! plans that still carry an unresolved Exchange node.

use nodedb_cluster::rpc_codec::TypedClusterError;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::wire as plan_wire;

use super::support::{PLAN_DECODE_FAILED, plan_contains_exchange};

/// Decodes `plan_bytes` into a [`PhysicalPlan`], re-resolves a
/// `DocumentOp::PointGet` surrogate when the coordinator shipped
/// `Surrogate::ZERO`, and rejects a plan that still contains an
/// unresolved Exchange node.
pub(super) fn decode_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    plan_bytes: &[u8],
) -> Result<PhysicalPlan, TypedClusterError> {
    // ── 3. Decode the PhysicalPlan ────────────────────────────────────────
    let mut plan = match plan_wire::decode(plan_bytes) {
        Ok(p) => p,
        Err(e) => {
            return Err(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: format!("plan decode failed: {e}"),
            });
        }
    };

    // ── 3a. Re-resolve an unresolved PK point-get surrogate ───────────────
    //
    // The query coordinator resolves `WHERE pk = <v>` → surrogate against
    // ITS OWN local catalog. The surrogate↔PK map is sharded to the
    // collection's data-group members, so a coordinator that is NOT a
    // member of that group misses the binding and ships `Surrogate::ZERO`.
    // We (the owner) ARE a group member, so our local catalog HAS the
    // binding — re-resolve here before the plan reaches the Data Plane.
    //
    // Scope is intentionally tight: only `DocumentOp::PointGet` reads, only
    // when the carried surrogate is ZERO and `pk_bytes` is non-empty. A
    // non-ZERO carried surrogate is authoritative (immutable first-wins
    // bind) and is left untouched; a genuinely-absent PK stays ZERO and
    // correctly resolves to not-found.
    let catalog_ref = state.credentials.catalog();
    if let nodedb_physical::physical_plan::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::PointGet {
            surrogate,
            pk_bytes,
            collection,
            ..
        },
    ) = &mut plan
        && *surrogate == nodedb_types::Surrogate::ZERO
        && !pk_bytes.is_empty()
        && let Ok(Some(resolved)) = catalog_ref.get_surrogate_for_pk(
            database_id,
            crate::types::TenantId::new(tenant_id),
            collection,
            pk_bytes,
        )
    {
        *surrogate = resolved;
    }

    // ── 3b. Reject unresolved Exchange nodes ──────────────────────────────
    if plan_contains_exchange(&plan) {
        return Err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: "received plan with unresolved Exchange node; coordinator must resolve \
                      data movement before cross-node dispatch"
                .into(),
        });
    }

    Ok(plan)
}
