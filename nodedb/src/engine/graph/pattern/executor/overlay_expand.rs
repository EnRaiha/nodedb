// SPDX-License-Identifier: BUSL-1.1

//! Read-your-own-writes overlay merge for the fixed-hop MATCH triple path.
//!
//! When a MATCH runs inside a transaction, its expansion must observe that
//! transaction's own staged edge writes and deletes (read-your-own-writes),
//! including through nodes reachable only via a staged edge — which carry no
//! durable CSR id. The durable u32-keyed path in [`super::core::execute_triple`]
//! cannot represent such staged-only nodes, so when a non-empty
//! [`GraphOverlayDelta`] is present a single fixed-hop triple is expanded here
//! against a NAME-keyed merge instead: durable CSR neighbours (minus staged
//! tombstones) UNION staged edges. This is the per-triple analogue of the
//! union+tombstone shape [`crate::engine::graph::csr::CsrIndex::traverse_bfs`]
//! already applies for `GRAPH NEIGHBORS`/`Hop`, reusing the same shared
//! [`GraphOverlayDelta`] primitives rather than a second merge implementation.
//!
//! Variable-length (`[*min..max]`) edges are NOT handled here — that BFS keys
//! its visited set on dense CSR ids and would need a separate string-keyed
//! rewrite to walk staged-only nodes; the caller keeps the durable varlen path
//! for those.
//!
//! Transaction overlays are single-node by construction (a txn's staged writes
//! live on the shard that owns the txn), so this path never emits a cross-shard
//! unresolved frontier: a staged edge is always locally resolvable.

use super::super::ast::{NodeBinding, PatternTriple};
use super::types::{BindingRow, ExecutionState};
use super::varlen_named::merge_neighbors_named;
use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};

/// Expand one fixed-hop (non variable-length) triple against a name-keyed merge
/// of durable CSR adjacency and the transaction's staged overlay.
///
/// Runs only when `overlay` is non-empty. Mirrors the row-building shape of the
/// durable branch in [`super::core::execute_triple`] (source resolution →
/// per-neighbour destination-binding compatibility → row emission) but keys on
/// node NAMES so a staged-only intermediate node (no CSR id) participates.
pub(super) fn expand_triple_overlay(
    triple: &PatternTriple,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &ExecutionState<'_>,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    overlay: &GraphOverlayDelta,
) -> Vec<BindingRow> {
    let direction = triple.edge.direction.to_csr_direction();
    let label_filter = triple.edge.edge_type.as_deref();
    let sources = resolve_sources(&triple.src, csr, input_row, frontier_bitmap, overlay);

    let mut results = Vec::new();
    for (src_name, src_id) in &sources {
        let neighbors = merge_neighbors_named(
            csr,
            src_name,
            *src_id,
            label_filter,
            direction,
            state.collection_filter,
            overlay,
        );
        for (label, dst_name) in neighbors {
            if !dst_compatible(&triple.dst, csr, input_row, &dst_name) {
                continue;
            }
            let mut row = input_row.clone();
            bind_name(&mut row, &triple.src, src_name);
            bind_name(&mut row, &triple.dst, &dst_name);
            if let Some(ref edge_name) = triple.edge.name {
                row.insert(edge_name.clone(), format!("{src_name}|{label}|{dst_name}"));
            }
            results.push(row);
        }
    }
    results
}

/// Resolve a triple's source binding to `(node_name, csr_id)` candidates.
///
/// A bound variable (name already present in `input_row`) resolves to that one
/// name; its CSR id is `None` when the node exists only via a staged edge. A
/// free-ranging source enumerates every durable node (honouring the label and
/// frontier-bitmap filters exactly as [`super::core`] does) plus any
/// staged-only endpoint the overlay introduced. A label constraint cannot be
/// verified against a staged-only node (the overlay carries no node labels), so
/// such a node is admitted only when the binding has no label.
fn resolve_sources(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    overlay: &GraphOverlayDelta,
) -> Vec<(String, Option<u32>)> {
    if let Some(name) = binding.name.as_ref().and_then(|n| row.get(n)) {
        let id = csr.node_id_raw(name);
        if let Some(ref label) = binding.label {
            match id {
                Some(i) if csr.node_has_label(i, label) => {}
                _ => return Vec::new(),
            }
        }
        return vec![(name.clone(), id)];
    }

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<(String, Option<u32>)> = Vec::new();
    for id in 0..csr.node_count() as u32 {
        let label_ok = binding
            .label
            .as_ref()
            .is_none_or(|l| csr.node_has_label(id, l));
        let bitmap_ok = frontier_bitmap
            .is_none_or(|bm| bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id))));
        if label_ok && bitmap_ok {
            let name = csr.node_name_raw(id).to_string();
            seen.insert(name.clone());
            out.push((name, Some(id)));
        }
    }
    // Staged-only endpoints (no durable CSR id) as additional free anchors.
    // Bitmap gating needs a durable surrogate, so it does not apply to the
    // transaction's own staged nodes; a label constraint is unverifiable and
    // therefore excludes them.
    if binding.label.is_none() {
        for name in overlay.staged_endpoint_names() {
            if csr.node_id_raw(name).is_none() && seen.insert(name.to_string()) {
                out.push((name.to_string(), None));
            }
        }
    }
    out
}

/// Name-based analogue of [`super::core::binding_compatible`]. A staged-only
/// destination has no CSR id, so a label constraint on it cannot be verified
/// and fails closed; an already-bound destination variable must match by name.
pub(super) fn dst_compatible(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    name: &str,
) -> bool {
    if let Some(ref label) = binding.label {
        match csr.node_id_raw(name) {
            Some(id) if csr.node_has_label(id, label) => {}
            _ => return false,
        }
    }
    if let Some(ref var) = binding.name
        && let Some(existing) = row.get(var)
    {
        return existing == name;
    }
    true
}

/// Name-based analogue of [`super::core::bind_node`]: record `name` under the
/// binding's variable if it has one and is not already bound.
pub(super) fn bind_name(row: &mut BindingRow, binding: &NodeBinding, name: &str) {
    if let Some(ref var) = binding.name {
        row.entry(var.clone()).or_insert_with(|| name.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::pattern::ast::{EdgeBinding, EdgeDirection};

    fn triple(src: Option<&str>, label: &str, dst: Option<&str>) -> PatternTriple {
        PatternTriple {
            src: NodeBinding {
                name: src.map(str::to_string),
                label: None,
            },
            edge: EdgeBinding {
                name: None,
                edge_type: Some(label.to_string()),
                direction: EdgeDirection::Right,
                min_hops: 1,
                max_hops: 1,
            },
            dst: NodeBinding {
                name: dst.map(str::to_string),
                label: None,
            },
        }
    }

    fn state() -> ExecutionState<'static> {
        ExecutionState::new(None, super::super::expansion::VarLenCaps::default())
    }

    /// A staged PUT is unioned with durable neighbours: a bound source `a` with
    /// a durable `a->b` edge and a staged `a->c` edge yields both destinations.
    #[test]
    fn staged_put_unions_with_durable_neighbours() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("a", "KNOWS", "c");

        let mut row = BindingRow::new();
        row.insert("a".to_string(), "a".to_string());
        // `(a)-[:KNOWS]->(y)` with `y` a free variable: both the durable `b` and
        // the staged `c` bind to `y`.
        let rows = expand_triple_overlay(
            &triple(Some("a"), "KNOWS", Some("y")),
            &csr,
            &row,
            &state(),
            None,
            &ov,
        );
        let dsts: std::collections::HashSet<&str> = rows.iter().map(|r| r["y"].as_str()).collect();
        assert_eq!(
            dsts,
            ["b", "c"]
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
        );
    }

    /// A staged tombstone hides a durable edge.
    #[test]
    fn staged_tombstone_hides_durable_edge() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "KNOWS", "b").unwrap();
        let mut ov = GraphOverlayDelta::new();
        ov.stage_tombstone("a", "KNOWS", "b");

        let mut row = BindingRow::new();
        row.insert("a".to_string(), "a".to_string());
        let rows = expand_triple_overlay(
            &triple(Some("a"), "KNOWS", Some("x")),
            &csr,
            &row,
            &state(),
            None,
            &ov,
        );
        assert!(rows.is_empty(), "tombstoned edge must not expand");
    }

    /// A staged-only intermediate node (no CSR id) can be a bound source and
    /// expand its own staged edge.
    #[test]
    fn staged_only_node_expands_as_source() {
        let csr = CsrIndex::new(); // no durable nodes at all
        let mut ov = GraphOverlayDelta::new();
        ov.stage_edge("x", "KNOWS", "y");

        let mut row = BindingRow::new();
        row.insert("m".to_string(), "x".to_string());
        let rows = expand_triple_overlay(
            &triple(Some("m"), "KNOWS", Some("n")),
            &csr,
            &row,
            &state(),
            None,
            &ov,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["n"], "y");
    }
}
