// SPDX-License-Identifier: BUSL-1.1

//! OLLP prediction injection helpers for dependent physical plans.

use nodedb_physical::physical_plan::{DocumentOp, OllpPredictedEdge, PhysicalPlan};

/// Inject `ollp_predicted_surrogates` into a `BulkUpdate` or `BulkDelete`
/// plan in-place.
///
/// Other plan variants are left unchanged. Idempotent — calling twice
/// replaces the previous prediction with the new one.
pub(super) fn inject_ollp_surrogates(plan: &mut PhysicalPlan, surrogates: Vec<u32>) {
    match plan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            ollp_predicted_surrogates,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkDelete {
            ollp_predicted_surrogates,
            ..
        }) => {
            *ollp_predicted_surrogates = Some(surrogates);
        }
        // Non-bulk plans are left unchanged. The two bulk arms above take
        // precedence; these inner wildcards catch every other op. Exhaustive
        // so a new PhysicalPlan variant forces a decision.
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
        | PhysicalPlan::ClusterEvent(_) => {}
    }
}

/// Inject `ollp_predicted_edges` into a `BulkUpdate` or `BulkDelete` plan
/// in-place.
///
/// `edges` is sorted by `(surrogate, from, to, label)` before storing so the
/// data-plane edge-content comparison is order-independent — mirroring how
/// `inject_ollp_surrogates` relies on the surrogate set being sorted. Other
/// plan variants are left unchanged; calling on a non-bulk plan is a no-op.
/// Edge-content validation currently runs only on the `BulkDelete` path, but
/// the field is set on whichever bulk variant the plan is for symmetry.
pub(super) fn inject_ollp_predicted_edges(
    plan: &mut PhysicalPlan,
    mut edges: Vec<OllpPredictedEdge>,
) {
    // Canonical `(surrogate, from, to, label)` order via derived `Ord`, matching
    // the data-plane verifier's sort so the set comparison is well-defined.
    edges.sort_unstable();
    match plan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            ollp_predicted_edges,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::BulkDelete {
            ollp_predicted_edges,
            ..
        }) => {
            *ollp_predicted_edges = Some(edges);
        }
        // Non-bulk plans are left unchanged. The two bulk arms above take
        // precedence; these inner wildcards catch every other op. Exhaustive
        // so a new PhysicalPlan variant forces a decision.
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
        | PhysicalPlan::ClusterEvent(_) => {}
    }
}
