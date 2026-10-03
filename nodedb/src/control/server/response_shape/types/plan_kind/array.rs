// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp` classification.

use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::kind::PlanKind;

/// A cluster array op answers with the payload its local `ArrayOp`
/// counterpart answers with, so it shapes the same way.
pub(super) fn describe_cluster_array(op: &ClusterArrayOp) -> PlanKind {
    match op {
        ClusterArrayOp::Slice { .. } => PlanKind::ArraySlice,
        ClusterArrayOp::Agg { .. } => PlanKind::MultiRow,
        ClusterArrayOp::Put { .. } => PlanKind::DmlResult("INSERT"),
        ClusterArrayOp::Delete { .. } => PlanKind::DmlResult("DELETE"),
    }
}

pub(super) fn describe_array(op: &ArrayOp) -> PlanKind {
    match op {
        ArrayOp::Slice { .. } => PlanKind::ArraySlice,

        // JSON-array payloads: each row streams as its own pgwire field.
        ArrayOp::Project { .. } | ArrayOp::Aggregate { .. } | ArrayOp::Elementwise { .. } => {
            PlanKind::MultiRow
        }

        // Reports `{"inserted": n}` / `{"deleted": n}`.
        ArrayOp::Put { .. } => PlanKind::DmlResult("INSERT"),
        ArrayOp::Delete { .. } => PlanKind::DmlResult("DELETE"),

        // Flush/Compact return status JSON — route SingleDocument.
        ArrayOp::Flush { .. } | ArrayOp::Compact { .. } => PlanKind::SingleDocument,

        // Array DDL: `{"opened": 1}` / `{"dropped": 1}` status, not a row count.
        ArrayOp::OpenArray { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RekeyArray { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        // Internal roaring bitmap for cross-engine prefilter, never a client row.
        | ArrayOp::SurrogateBitmapScan { .. } => PlanKind::Execution,
    }
}
