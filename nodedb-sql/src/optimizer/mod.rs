// SPDX-License-Identifier: Apache-2.0

pub mod constant_fold;
pub mod point_get;
pub mod predicate_pushdown;

use crate::catalog::SqlCatalog;
use crate::types::SqlPlan;

/// Apply all optimization passes to a plan.
pub fn optimize(plan: SqlPlan, catalog: &dyn SqlCatalog) -> SqlPlan {
    let plan = point_get::optimize(plan, catalog);
    predicate_pushdown::optimize(plan)
}
