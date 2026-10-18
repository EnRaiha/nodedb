// SPDX-License-Identifier: BUSL-1.1

//! Which gathered reads are cluster array reads.
//!
//! A cluster array read (`ClusterArrayOp::Slice` / `Agg` / `Elementwise`)
//! reaches the coordinator as the bare relational rows its array coordinator
//! merged (`cluster_leaf`): a slice row holds one field per dim and attr,
//! keyed by name, an aggregate row is `{group, result}` or `{result}`, and
//! an elementwise row is `{coords, attrs}`. The storage
//! flatten that unwraps `{id, data}` document rows must not run over them: it
//! will read an attr named `data` as a row envelope.
//!
//! A local `PhysicalPlan::Array` read never yields gathered rows.
//! `gather_all_vshards` hands every array plan to `cluster_leaf`, which
//! inlines cluster array leaves and refuses a local one.

use nodedb_physical::physical_plan::{ClusterArrayOp, PhysicalPlan};

/// Whether `plan` is a cluster array read, whose gathered rows are already
/// relational.
pub(super) fn is_cluster_array_read(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::ClusterArray(
            ClusterArrayOp::Slice { .. }
                | ClusterArrayOp::Agg { .. }
                | ClusterArrayOp::Elementwise { .. }
        )
    )
}
