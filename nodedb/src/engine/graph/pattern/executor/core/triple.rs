// SPDX-License-Identifier: BUSL-1.1

//! Single-triple `(src)-[edge]->(dst)` evaluation: fixed-hop and
//! variable-length expansion, read-your-own-writes overlay dispatch, and
//! cross-shard frontier recording.

use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::Direction;
use crate::engine::graph::pattern::ast::PatternTriple;
use crate::engine::graph::pattern::executor::expansion;
use crate::engine::graph::pattern::executor::overlay_expand;
use crate::engine::graph::pattern::executor::types::{
    BindingRow, ExecutionState, UnresolvedExpansion, VarLenResume,
};
use crate::engine::graph::pattern::executor::varlen_named::{self, NameOrId};

use super::binding::{bind_node, binding_compatible, resolve_binding};

/// Execute a single triple `(src)-[edge]->(dst)` against a binding row.
///
/// `triple_idx` is the 0-based position of this triple within its chain;
/// it is recorded in any `UnresolvedExpansion` emitted.
pub(in crate::engine::graph::pattern::executor) fn execute_triple(
    triple: &PatternTriple,
    triple_idx: usize,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    // Read-your-own-writes: inside a transaction with staged graph edges, a
    // fixed-hop triple is expanded against a name-keyed merge of durable CSR
    // adjacency and the staged overlay, so staged edges are visible, staged
    // tombstones are hidden, and staged-only intermediate nodes (which have no
    // durable CSR id) participate. Variable-length edges keep the durable BFS
    // path (its visited set keys on dense CSR ids); autocommit / empty-overlay
    // runs are unaffected and fall through to the durable path below.
    //
    // The merge runs in cluster mode too: a bound source whose merged (durable
    // ∪ staged, minus tombstone) out-degree is zero and which the locality
    // predicate marks remote is emitted as a cross-shard `UnresolvedExpansion`
    // by `expand_triple_overlay`, exactly as the durable fixed-hop path does for
    // a zero-raw-degree bound source, so a resumed pattern's fixed-hop tail
    // continues onto the staged edge's owning core instead of being dropped.
    if let Some(ov) = overlay
        && !ov.is_empty()
        && !triple.edge.is_variable_length()
    {
        return Ok(overlay_expand::expand_triple_overlay(
            triple,
            triple_idx,
            csr,
            input_row,
            state,
            frontier_bitmap,
            ov,
        ));
    }

    let direction = triple.edge.direction.to_csr_direction();
    let label_filter = triple.edge.edge_type.as_deref();
    let src_nodes = resolve_binding(&triple.src, csr, input_row, frontier_bitmap);

    if src_nodes.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();

    if triple.edge.is_variable_length() {
        // Path strings are only needed when the edge variable is bound
        // (e.g. `(a)-[e*1..3]->(b) RETURN e`). For anonymous variable
        // expansions skip all `format!`/`String` work in the hot loop.
        let want_path = triple.edge.name.is_some();
        let pattern = expansion::VarLenPattern {
            label_filter,
            direction,
            min_hops: triple.edge.min_hops,
            max_hops: triple.edge.max_hops,
            want_path,
            collection_filter: state.collection_filter,
        };
        for &src_id in &src_nodes {
            let expansion = expansion::expand_variable_length(
                csr,
                src_id,
                &pattern,
                state.varlen_caps,
                overlay,
            );
            if let Some(cursor) = expansion.cursor {
                // Capture the LIVE resume cursor instead of silently dropping
                // the un-expanded frontier. The cursor's `source_row` MUST carry
                // this expansion's source binding (e.g. `a = src_id`): a
                // free-ranging anchor has an empty `input_row`, and the resumed
                // rows are rebuilt from `source_row`, so without binding the
                // source here the resumed rows would lack the anchor variable and
                // be dropped by a `WHERE`/projection that references it.
                let mut source_row = input_row.clone();
                bind_node(&mut source_row, &triple.src, csr, src_id);
                state.record_truncation(VarLenResume {
                    triple_idx,
                    source_row,
                    frontier: cursor.frontier,
                    depth: cursor.depth,
                });
            }

            // Cross-boundary continuations: frontier nodes with zero local
            // out-degree whose remaining edges may be homed on another shard.
            // Shipping them (gated on the remote-node predicate) is what makes a
            // staged/durable cross-boundary edge reachable without depending on
            // the result cap firing. The anchor's binding is carried on the
            // resumed rows via `source_row`, identical to the cap-cursor case.
            if !expansion.boundary.is_empty() && state.is_remote_node.is_some() {
                let mut source_row = input_row.clone();
                bind_node(&mut source_row, &triple.src, csr, src_id);
                varlen_named::record_boundary_resumes(
                    state,
                    triple_idx,
                    &source_row,
                    &expansion.boundary,
                );
            }

            for (dst_id, path) in expansion.results {
                if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, csr, src_id);
                bind_node(&mut row, &triple.dst, csr, dst_id);
                if let Some(ref edge_name) = triple.edge.name {
                    row.insert(edge_name.clone(), path);
                }
                results.push(row);
            }

            // Overlay path destinations: a durable dst binds by id (as above); a
            // staged-only dst has no CSR id and binds by name.
            for (bound, path) in expansion.named_results {
                match bound {
                    NameOrId::Id(dst_id) => {
                        if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                            continue;
                        }
                        let mut row = input_row.clone();
                        bind_node(&mut row, &triple.src, csr, src_id);
                        bind_node(&mut row, &triple.dst, csr, dst_id);
                        if let Some(ref edge_name) = triple.edge.name {
                            row.insert(edge_name.clone(), path);
                        }
                        results.push(row);
                    }
                    NameOrId::Name(dst_name) => {
                        if !overlay_expand::dst_compatible(&triple.dst, csr, input_row, &dst_name) {
                            continue;
                        }
                        let mut row = input_row.clone();
                        bind_node(&mut row, &triple.src, csr, src_id);
                        overlay_expand::bind_name(&mut row, &triple.dst, &dst_name);
                        if let Some(ref edge_name) = triple.edge.name {
                            row.insert(edge_name.clone(), path);
                        }
                        results.push(row);
                    }
                }
            }
        }
    } else {
        // Determine whether the source variable was BOUND (resolved from a
        // prior binding in `input_row` or from a literal match) or
        // FREE-RANGING (enumerated over all local nodes because no binding
        // existed yet).  `resolve_binding` takes the BOUND path only when
        // `binding.name` is `Some` AND that name already appears in `input_row`.
        // Everything else — anonymous nodes, unbound variables, and
        // frontier-bitmap-restricted enumeration — is FREE-RANGING.
        //
        // Only BOUND sources can produce a frontier entry: they represent a
        // locally-originated partial match whose continuation must be dispatched
        // to the source node's home shard.  A FREE-RANGING source must NOT emit
        // because every shard will range over the same local nodes during its own
        // pass; emitting here would duplicate work and pollute the frontier with
        // every zero-degree sink.
        let source_is_bound = triple
            .src
            .name
            .as_deref()
            .is_some_and(|n| input_row.contains_key(n));

        for &src_id in &src_nodes {
            // Check raw degree in the queried direction BEFORE any label
            // filter. A source with zero raw adjacency means its edges may
            // live on a remote shard — record it in the frontier so the
            // Control Plane can dispatch a continuation. A source that has
            // edges locally but none pass the label filter is a legitimate
            // empty local result; do NOT add it to the frontier.
            let raw_degree = match direction {
                Direction::Out => csr.out_degree_raw(src_id),
                Direction::In => csr.in_degree_raw(src_id),
                Direction::Both => csr.out_degree_raw(src_id) + csr.in_degree_raw(src_id),
            };
            if raw_degree == 0 {
                // Emit a frontier entry only when ALL four conditions hold:
                // 1. The source variable was BOUND (not free-ranging).
                // 2. The caller supplied a locality predicate.
                // 3. The predicate identifies this node as remote.
                // 4. (implicit) Zero raw adjacency — we are inside this branch.
                //
                // A free-ranging unbound source NEVER emits regardless of
                // degree or predicate.  Without a predicate (None) — the
                // fully-local single-node path — every leaf is a legitimate
                // terminal, not a cross-shard ghost.
                if source_is_bound && let Some(pred) = state.is_remote_node {
                    let node_name = csr.node_name_raw(src_id).to_string();
                    if pred(&node_name) {
                        let binding_var =
                            triple.src.name.clone().unwrap_or_else(|| node_name.clone());
                        state.frontier.push(UnresolvedExpansion {
                            binding_var,
                            node_name,
                            triple_idx,
                            partial_row: input_row.clone(),
                        });
                    }
                }
                // No local edges to produce — continue to next src.
                continue;
            }

            let neighbors = expansion::collect_neighbors(
                csr,
                src_id,
                label_filter,
                direction,
                state.collection_filter,
            );
            for (lid, dst_id) in neighbors {
                if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, csr, src_id);
                bind_node(&mut row, &triple.dst, csr, dst_id);
                if let Some(ref edge_name) = triple.edge.name {
                    let src_name = csr.node_name_raw(src_id);
                    let dst_name = csr.node_name_raw(dst_id);
                    let label_name = csr.label_name(lid);
                    row.insert(
                        edge_name.clone(),
                        format!("{src_name}|{label_name}|{dst_name}"),
                    );
                }
                results.push(row);
            }
        }
    }

    Ok(results)
}
