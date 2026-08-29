// SPDX-License-Identifier: BUSL-1.1

//! Predicate extraction and implicit-edge lifecycle classification for the
//! dependent (OLLP) Calvin recon dispatch. Both are derived ONCE per statement,
//! before the retry loop, because neither changes across retries.

use crate::Error;
use crate::control::planner::implicit_edges::{EdgeFieldOverrides, parse_edge_field_overrides};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

/// The implicit-edge lifecycle a dependent (OLLP) Calvin task drives, derived
/// once from the dependent task's plan variant. `Update` carries the SET-clause
/// overrides (parsed once — they are constant across retries).
pub(super) enum EdgeLifecycle {
    Delete,
    Update(EdgeFieldOverrides),
}

/// Extract the collection name and serialized filter bytes from a
/// `BulkUpdate` or `BulkDelete` plan.
///
/// Returns `("", vec![])` for plan variants that are not bulk predicates.
pub(super) fn extract_bulk_predicate_info(plan: &PhysicalPlan) -> (String, Vec<u8>) {
    match plan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection,
            filters,
            ..
        }) => (collection.to_string(), filters.clone()),
        // Not a bulk predicate. The two bulk arms above take precedence; these
        // inner wildcards catch every other op (including non-bulk document
        // ops). Exhaustive so a new PhysicalPlan variant forces a decision.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => (String::new(), vec![]),
    }
}

/// Classify the implicit-edge lifecycle `plan` drives. A `BulkDelete` retracts
/// the matched edge documents' mirrored edges; a `BulkUpdate` reconciles them
/// against the SET clause, whose overrides are parsed here once — they are
/// constant across retries.
pub(super) fn classify_edge_lifecycle(plan: &PhysicalPlan) -> crate::Result<EdgeLifecycle> {
    match plan {
        PhysicalPlan::Document(DocumentOp::BulkDelete { .. }) => Ok(EdgeLifecycle::Delete),
        PhysicalPlan::Document(DocumentOp::BulkUpdate { updates, .. }) => {
            Ok(EdgeLifecycle::Update(parse_edge_field_overrides(updates)?))
        }
        // Unreachable: `is_dependent_predicate` only selects BulkUpdate /
        // BulkDelete. Surface a typed error rather than panicking. Exhaustive
        // so a new `PhysicalPlan` variant forces a decision here.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Err(Error::Internal {
            detail: "dependent Calvin task is neither BulkUpdate nor BulkDelete".to_owned(),
        }),
    }
}
