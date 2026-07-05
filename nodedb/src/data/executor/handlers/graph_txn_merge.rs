// SPDX-License-Identifier: BUSL-1.1

//! Read-your-own-writes merge for GRAPH single-hop reads (`Neighbors`,
//! `Hop` at depth 1).
//!
//! The durable neighbor list a CSR partition returns reflects only
//! committed state. When a request carries a `txn_id` with a staged
//! `GraphTxnOverlay`, [`merge_graph_txn_overlay_neighbors`] folds that
//! transaction's pending edge writes into the durable result: staged
//! tombstones subtract a durable neighbor, staged puts add one. `Hop`
//! beyond depth 1 is NOT merged here (see `execute_graph_hop`'s doc
//! comment) -- this module only covers the single-hop case both ops share.
//!
//! Pure function, not a `CoreLoop` method: callers resolve the overlay via
//! `self.graph_txn_overlays.get(&txn_id)` and pass it in, so this logic is
//! unit-testable without constructing a full `CoreLoop`.

use crate::data::executor::handlers::transaction::overlay::GraphTxnOverlay;
use crate::engine::graph::csr::GraphOverlayDelta;
use crate::engine::graph::edge_store::Direction;
use crate::types::TenantId;
use nodedb_types::DatabaseId;

/// Translate a transaction's [`GraphTxnOverlay`] into a shared-crate
/// [`GraphOverlayDelta`] scoped to `(database_id, tenant)`, for the multi-hop
/// `Hop` (depth > 1) and `Subgraph` read-your-own-writes paths.
///
/// Neighbors / single-hop `Hop` use [`merge_graph_txn_overlay_neighbors`] /
/// [`merge_hop_single_hop`] instead; the traversal engine itself cannot merge
/// staged edges through staged-only intermediate nodes, so multi-hop pushes
/// the whole delta down into `traverse_bfs` / `subgraph`.
pub(in crate::data::executor) fn build_graph_overlay_delta(
    overlay: &GraphTxnOverlay,
    database_id: DatabaseId,
    tenant: TenantId,
) -> GraphOverlayDelta {
    let mut delta = GraphOverlayDelta::new();
    for (src, label, dst) in overlay.all_staged_edges(database_id, tenant) {
        delta.stage_edge(&src, &label, &dst);
    }
    for (src, label, dst) in overlay.all_tombstones(database_id, tenant) {
        delta.stage_tombstone(&src, &label, &dst);
    }
    delta
}

/// Merge a transaction's staged GRAPH edge writes into a durable `(label,
/// node)` neighbor list for `node_id`, respecting `direction` and
/// `edge_label`. No-op (returns `durable` unchanged) when `overlay` is
/// `None`.
pub(in crate::data::executor) fn merge_graph_txn_overlay_neighbors(
    overlay: Option<&GraphTxnOverlay>,
    database_id: DatabaseId,
    tenant: TenantId,
    node_id: &str,
    edge_label: Option<&str>,
    direction: Direction,
    durable: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let Some(overlay) = overlay else {
        return durable;
    };

    // Subtract any durable neighbor whose backing edge was tombstoned in
    // this transaction.
    let mut merged: Vec<(String, String)> = durable
        .into_iter()
        .filter(|(label, other)| {
            let (src, dst) = edge_endpoints(direction, node_id, other);
            !overlay.is_edge_tombstoned_any_collection(database_id, tenant, src, label, dst)
        })
        .collect();

    // Add staged edges matching direction + label that aren't already
    // present (a staged put re-adding an edge that survived the tombstone
    // filter above, or a brand-new edge).
    let mut staged: Vec<(String, String, Vec<u8>)> = Vec::new();
    if matches!(direction, Direction::Out | Direction::Both) {
        staged.extend(overlay.edges_for_src_any_collection(database_id, tenant, node_id));
    }
    if matches!(direction, Direction::In | Direction::Both) {
        staged.extend(overlay.edges_for_dst_any_collection(database_id, tenant, node_id));
    }
    for (label, other, _props) in staged {
        if edge_label.is_some_and(|f| f != label) {
            continue;
        }
        if !merged.iter().any(|(l, n)| *l == label && *n == other) {
            merged.push((label, other));
        }
    }
    merged
}

/// Merge a transaction's staged GRAPH edge writes into `Hop`'s durable BFS
/// result, but ONLY for the single-hop case (`depth == 1`) -- multi-hop
/// `Hop` stays durable-only, since merging staged edges into an N-hop BFS
/// would require re-deriving frontier expansion per staged edge across
/// every hop, out of scope for this unit (which covers single-hop reads).
///
/// `durable_neighbors_of` fetches one start node's durable `(label, node)`
/// neighbor list on demand (the caller's `csr_partition(..).neighbors(..)`),
/// so this function stays free of any `CoreLoop` dependency.
///
/// When `has_bitmap` is `true` (a `frontier_bitmap` prefilter is active),
/// tombstone subtraction is still applied (always safe -- removing a result
/// never violates a prefilter), but staged-edge addition is skipped, since a
/// brand-new staged node's bitmap membership can't be validated here.
///
/// Bundled into a params struct (rather than a long positional argument
/// list) since the durable-neighbor fetch closure, the merge identity
/// (database/tenant/label/direction), and the BFS-result bookkeeping
/// (depth/bitmap/durable_result) are each a distinct concern.
pub(in crate::data::executor) struct HopMergeParams<'a, F>
where
    F: Fn(&str) -> Vec<(String, String)>,
{
    pub overlay: Option<&'a GraphTxnOverlay>,
    pub durable_neighbors_of: F,
    pub starts: &'a [&'a str],
    pub depth: usize,
    pub database_id: DatabaseId,
    pub tenant: TenantId,
    pub edge_label: Option<&'a str>,
    pub direction: Direction,
    pub has_bitmap: bool,
    pub durable_result: Vec<String>,
}

pub(in crate::data::executor) fn merge_hop_single_hop<F>(
    params: HopMergeParams<'_, F>,
) -> Vec<String>
where
    F: Fn(&str) -> Vec<(String, String)>,
{
    let HopMergeParams {
        overlay,
        durable_neighbors_of,
        starts,
        depth,
        database_id,
        tenant,
        edge_label,
        direction,
        has_bitmap,
        durable_result,
    } = params;

    if depth != 1 {
        return durable_result;
    }
    let Some(overlay) = overlay else {
        return durable_result;
    };

    let mut merged: std::collections::HashSet<String> = durable_result.into_iter().collect();
    for start in starts {
        let durable_here = durable_neighbors_of(start);
        let durable_names: std::collections::HashSet<&str> =
            durable_here.iter().map(|(_, n)| n.as_str()).collect();
        let merged_here = merge_graph_txn_overlay_neighbors(
            Some(overlay),
            database_id,
            tenant,
            start,
            edge_label,
            direction,
            durable_here.clone(),
        );
        let merged_names: std::collections::HashSet<&str> =
            merged_here.iter().map(|(_, n)| n.as_str()).collect();

        for name in durable_names.difference(&merged_names) {
            merged.remove(*name);
        }
        if !has_bitmap {
            for name in merged_names.difference(&durable_names) {
                merged.insert((*name).to_string());
            }
        }
    }
    merged.into_iter().collect()
}

/// Resolve the `(src, dst)` pair for a tombstone lookup given the direction
/// a durable neighbor entry was returned under: for `Out`, `other` is the
/// dst and `node_id` is the src; for `In`, `other` is the src and `node_id`
/// is the dst. `Both` is resolved as `Out`'s shape -- a mismatched guess
/// only costs a missed subtraction on a durable `Both` result, and neither
/// `Neighbors` nor `Hop` (the only two callers of this merge) is invoked
/// with `Both` by any current planner path.
fn edge_endpoints<'a>(
    direction: Direction,
    node_id: &'a str,
    other: &'a str,
) -> (&'a str, &'a str) {
    match direction {
        Direction::In => (other, node_id),
        Direction::Out | Direction::Both => (node_id, other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> TenantId {
        TenantId::new(1)
    }

    fn coll_key(coll: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::new(1), tenant(), coll.to_string())
    }

    #[test]
    fn no_overlay_returns_durable_unchanged() {
        let durable = vec![("knows".to_string(), "b".to_string())];
        let out = merge_graph_txn_overlay_neighbors(
            None,
            DatabaseId::new(1),
            tenant(),
            "a",
            None,
            Direction::Out,
            durable.clone(),
        );
        assert_eq!(out, durable);
    }

    #[test]
    fn staged_put_added_for_out_direction() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "a",
            None,
            Direction::Out,
            Vec::new(),
        );
        assert_eq!(out, vec![("knows".to_string(), "b".to_string())]);
    }

    #[test]
    fn staged_put_added_for_in_direction() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "b",
            None,
            Direction::In,
            Vec::new(),
        );
        assert_eq!(out, vec![("knows".to_string(), "a".to_string())]);
    }

    #[test]
    fn tombstoned_durable_edge_excluded() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "c");

        let durable = vec![("knows".to_string(), "c".to_string())];
        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "a",
            None,
            Direction::Out,
            durable,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn label_filter_excludes_non_matching_staged_edge() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "other_label", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "a",
            Some("knows"),
            Direction::Out,
            Vec::new(),
        );
        assert!(out.is_empty());
    }

    #[test]
    fn build_delta_carries_staged_edges_and_tombstones() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());
        overlay.stage_edge_delete(coll_key("g"), "x", "knows", "y");

        let delta = build_graph_overlay_delta(&overlay, DatabaseId::new(1), tenant());
        assert!(!delta.is_empty());
        let out: Vec<_> = delta.out_neighbors("a", None).collect();
        assert_eq!(out, vec![("knows", "b")]);
        let inn: Vec<_> = delta.in_neighbors("b", None).collect();
        assert_eq!(inn, vec![("knows", "a")]);
        assert!(delta.is_tombstoned("x", "knows", "y"));
    }

    #[test]
    fn build_delta_scopes_to_database_tenant() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        // A different database id sees none of the staged edges.
        let delta = build_graph_overlay_delta(&overlay, DatabaseId::new(999), tenant());
        assert!(delta.is_empty());
    }

    #[test]
    fn unrelated_node_unaffected() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", Vec::new());

        let out = merge_graph_txn_overlay_neighbors(
            Some(&overlay),
            DatabaseId::new(1),
            tenant(),
            "z",
            None,
            Direction::Out,
            Vec::new(),
        );
        assert!(out.is_empty());
    }
}
