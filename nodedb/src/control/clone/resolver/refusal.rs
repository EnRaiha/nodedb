// SPDX-License-Identifier: BUSL-1.1

//! Outcome type and refusal rule for clone read rewriting.
//!
//! A read over a shadowed clone either reads through to the source or refuses.
//! Returning target-only rows is never an option: the target holds post-clone
//! writes alone, so the answer would be silently incomplete.

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

use crate::control::security::identity::{Permission, required_permission};
use crate::control::server::shared::plan_util::{extract_collection, plan_engine};

/// Outcome of rewriting one target-side plan into its source-side twin.
///
/// The two "no plan produced" cases must never share a value. A plan that does
/// not read the cloned collection needs no source task; a plan that DOES read
/// it but has no rewrite is a correctness hole. The first is
/// [`SourceRewrite::NoSourceTask`]; the second is a typed error, so a query
/// shape added to the planner tomorrow refuses loudly instead of inheriting the
/// silent-empty bug.
pub enum SourceRewrite {
    /// Dispatch this plan against the source database.
    Task(Box<PhysicalPlan>),
    /// No source task is needed: writes, DDL, plans over another collection,
    /// and `DocumentOp::PointGet` whose key has no binding in the source (the
    /// row never existed there, so there is nothing to fetch).
    NoSourceTask,
}

impl SourceRewrite {
    /// Dispatch `plan` against the source database.
    pub fn task(plan: PhysicalPlan) -> Self {
        Self::Task(Box::new(plan))
    }
}

/// Typed refusal for a read that names the cloned collection but has no
/// source-side rewrite.
///
/// Answering such a read from the target alone returns rows that are silently
/// incomplete; concatenating an unrewritable shape returns rows that are
/// silently wrong. Both are worse than an error naming the way out.
pub fn refuse_clone_read_shape(plan: &PhysicalPlan, target_coll: &str) -> crate::Error {
    crate::Error::PlanError {
        detail: format!(
            "'{target_coll}' cannot be read through an unmaterialized clone with this query \
             shape ({:?} engine); run ALTER DATABASE <clone> MATERIALIZE first",
            plan_engine(plan)
        ),
    }
}

/// Whether `plan` READS the cloned collection `qualified`.
///
/// Writes are excluded: they execute against the target only and need no
/// source task, so they are not a dropped-source-rows hazard.
pub fn plan_reads_cloned_collection(plan: &PhysicalPlan, qualified: &str) -> bool {
    if required_permission(plan) != Permission::Read {
        return false;
    }
    extract_collection(plan) == Some(qualified) || join_reads_collection(plan, qualified)
}

/// Whether a join reads `qualified` on EITHER side.
///
/// [`extract_collection`] reports a join's LEFT collection only, so a clone
/// joined on the right would otherwise slip past the refusal.
fn join_reads_collection(plan: &PhysicalPlan, qualified: &str) -> bool {
    match plan {
        PhysicalPlan::Query(
            QueryOp::HashJoin {
                left_collection,
                right_collection,
                ..
            }
            | QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                ..
            }
            | QueryOp::SortMergeJoin {
                left_collection,
                right_collection,
                ..
            },
        ) => left_collection == qualified || right_collection == qualified,
        _ => false,
    }
}
