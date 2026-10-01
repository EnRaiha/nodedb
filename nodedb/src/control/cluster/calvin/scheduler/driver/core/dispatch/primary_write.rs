// SPDX-License-Identifier: BUSL-1.1

//! Per-slice write classification: primary-write and RETURNING predicates
//! shared by the static and active dispatch paths.

use nodedb_physical::physical_plan::PhysicalPlan;

/// Whether the transaction carries any write that is NOT a derived side
/// effect (an implicit graph edge, a cross-shard balance delta). Decided over
/// the FULL plan set before it is sliced per vShard, because a slice alone
/// cannot tell a lone derived participant from a derived-only statement.
pub(super) fn txn_has_non_derived_write(plans: &[PhysicalPlan]) -> bool {
    crate::control::planner::calvin::write_class::plans_have_user_write(plans)
}

/// Whether this vShard's slice carries the USER'S own write, as opposed to a
/// derived side effect the Control Plane appended alongside it.
///
/// It gates the applied-response deposit, and that is the whole reason the
/// distinction has to be made: a statement's `CommandComplete` is shaped from
/// ONE deposited response, primary-write participants coalesce first-wins, and
/// a derived participant's response describes a row the user's statement never
/// named. A balance write that won that race handed an `INSERT` tag a count —
/// or, when its flush found the commit already resolved and answered with an
/// empty payload, no count at all — belonging to a different write entirely.
///
/// `is_derived_side_effect` is the named predicate rather than an inline
/// `!matches!(plan, PhysicalPlan::Graph(_))`: the implicit graph edge and the
/// cross-shard balance are the same concept, and spelling it inline here is why
/// the second one never inherited the exclusion.
///
/// `txn_has_non_derived_write` is the transaction-level answer from
/// [`txn_has_non_derived_write`]. When the transaction has one, only the slice
/// holding it is primary. When it has none — the standalone `GRAPH INSERT /
/// DELETE EDGE` DSL's Graph-only tx_class — every write slice is the user's
/// write and deposits; first-wins coalescing then picks one identical count.
/// An empty slice (validate-only read) is never primary.
pub(super) fn plans_have_primary_write(
    plans: &[PhysicalPlan],
    txn_has_non_derived_write: bool,
) -> bool {
    if txn_has_non_derived_write {
        return self::txn_has_non_derived_write(plans);
    }
    plans
        .iter()
        .any(crate::control::planner::calvin::is_write_plan)
}

/// Whether this vShard's slice carries a RETURNING-bearing write — a plan whose
/// applied response is DATA-ROWs rather than a bare affected-count. Uses the
/// SAME `describe_plan` classification the coordinator's response-shaping uses,
/// so the two never disagree about which participant owns the returned rows.
pub(super) fn plans_have_returning(plans: &[PhysicalPlan]) -> bool {
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};
    plans
        .iter()
        .any(|plan| matches!(describe_plan(plan), PlanKind::ReturningRows))
}
