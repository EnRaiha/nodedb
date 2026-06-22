// SPDX-License-Identifier: BUSL-1.1

//! MATCH pattern executor — runs pattern matching on the CSR index.
//!
//! Takes a parsed `MatchQuery` and produces a result set of bound variable
//! assignments. Each assignment is a row mapping variable names to node/edge IDs.

mod continuation;
mod expansion;
mod predicates;

use std::collections::HashMap;

pub use continuation::execute_continuation;

use super::ast::*;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::{Direction, EdgeStore};

/// A single result row: variable bindings.
pub type BindingRow = HashMap<String, String>;

/// An expansion source that has zero local adjacency in the queried
/// direction — its out-edges (or in-edges) are homed on another shard.
///
/// Emitted by the executor so the Control Plane can dispatch a
/// continuation query to the owning shard. The frontier is produced
/// on every MATCH execution; on a fully-local CSR it is always empty.
///
/// # Cross-shard contract
///
/// The executor emits an `UnresolvedExpansion` for a source node when
/// **all four** conditions hold:
/// 1. The source variable was **bound** — it was resolved from an existing
///    binding in `input_row` (the multi-hop intermediate case), not
///    free-ranged over all local nodes.  A free-ranging anchor must NOT
///    emit because every shard covers all its own local nodes during its
///    own pass; emitting would duplicate work and flood the frontier with
///    every zero-degree local sink.
/// 2. The node has **zero raw adjacency** in the triple's direction
///    (regardless of edge-label filter).
/// 3. The caller supplied a locality predicate (`is_remote_node`).
/// 4. The predicate returns `true` for the node's name.
///
/// A node that has edges in the direction but none that pass the label
/// filter produces an empty local result and is NOT included in the
/// frontier (that is a legitimate "no match locally," not a
/// missing-shard situation).
///
/// Passing `None` for the predicate (the default for fully-local,
/// single-node deployments) guarantees the frontier is always empty.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct UnresolvedExpansion {
    /// The source binding variable name from the triple (e.g. `"b"`).
    pub binding_var: String,
    /// The resolved source node name with no local edges (e.g. `"bob"`).
    pub node_name: String,
    /// 0-based index of the triple in its chain that could not expand.
    pub triple_idx: usize,
    /// Bindings accumulated up to (but not including) this triple.
    pub partial_row: BindingRow,
}

/// Result of running a MATCH query.
///
/// `truncated` is `true` iff a hard cap inside variable-length expansion
/// fired — the binding rows are incomplete. Data Plane handlers MUST set
/// the `partial` flag on the response envelope when this is set so
/// clients can observe the incomplete result.
///
/// `unresolved_frontier` lists expansion sources whose edges are not
/// present in the local CSR partition. On a fully-local CSR this vec
/// is always empty and existing behaviour is byte-identical to before.
pub struct MatchOutcome {
    pub rows: Vec<BindingRow>,
    pub truncated: bool,
    pub unresolved_frontier: Vec<UnresolvedExpansion>,
}

/// Shared mutable state collected during triple execution: the list of
/// binding rows being built + the across-query truncation flag +
/// the cross-shard unresolved frontier.
///
/// `is_remote_node` is an optional caller-supplied predicate: when
/// `Some(pred)`, `pred(node_name)` returns `true` for nodes that are
/// homed on a remote shard. When `None` every node is treated as local
/// and no frontier entries are ever emitted. The predicate is borrowed
/// for the lifetime `'a` of the execution call to avoid allocation.
pub(super) struct ExecutionState<'a> {
    pub truncated: bool,
    pub frontier: Vec<UnresolvedExpansion>,
    pub is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
}

impl<'a> ExecutionState<'a> {
    pub(super) fn new(is_remote_node: Option<&'a dyn Fn(&str) -> bool>) -> Self {
        Self {
            truncated: false,
            frontier: Vec::new(),
            is_remote_node,
        }
    }
}

/// Execute a MATCH query on a CSR index and edge store.
///
/// Applies join order optimization before execution: triples within each
/// PatternChain are reordered by selectivity (lowest edge count first,
/// bound variables preferred).
///
/// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in the
/// bitmap are eligible as pattern anchors. Bound variables (already resolved
/// from a prior binding row) bypass the bitmap check — only free-variable
/// anchor enumeration is restricted.
///
/// `is_remote_node`: when `Some(pred)`, `pred(node_name)` returns `true` for
/// nodes homed on a remote shard. Only nodes that (a) were reached via a
/// **bound** source variable (resolved from `input_row`, not free-ranged),
/// AND (b) satisfy this predicate, AND (c) have zero raw directional
/// adjacency, are added to `unresolved_frontier`.  Free-ranging anchors never
/// emit, even when the predicate and degree conditions hold, because each
/// shard's own pass covers all its local nodes.
/// Pass `None` (the production default on a fully-local CSR) to guarantee
/// an always-empty frontier, preserving byte-identical single-node behaviour.
pub fn execute<'a>(
    query: &MatchQuery,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
) -> Result<MatchOutcome, crate::Error> {
    // Optimize query before execution (reorder triples by selectivity).
    let mut optimized = query.clone();
    super::optimizer::optimize(&mut optimized, csr);
    execute_query(&optimized, csr, edge_store, frontier_bitmap, is_remote_node)
}

/// Execute a pre-optimized MATCH query (internal, skip optimizer).
fn execute_query<'a>(
    query: &MatchQuery,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
) -> Result<MatchOutcome, crate::Error> {
    let mut rows: Vec<BindingRow> = vec![HashMap::new()];
    let mut state = ExecutionState::new(is_remote_node);

    for clause in &query.clauses {
        let clause_rows = execute_clause(clause, csr, &rows, &mut state, frontier_bitmap)?;
        if clause.optional {
            rows = left_join_rows(&rows, &clause_rows, clause);
        } else {
            rows = clause_rows;
        }
    }

    let rows = continuation::finalize_rows(query, rows, csr, edge_store, frontier_bitmap)?;

    Ok(MatchOutcome {
        rows,
        truncated: state.truncated,
        unresolved_frontier: state.frontier,
    })
}

/// Serialize binding rows to MessagePack for SPSC transport.
///
/// The Data Plane MUST produce MessagePack so that broadcast merge
/// (`extract_msgpack_elements`) can correctly split and re-merge rows
/// from multiple cores. BindingRow is `HashMap<String, String>` — all
/// values are strings, so we write raw msgpack directly.
pub fn rows_to_msgpack(rows: &[BindingRow]) -> Result<Vec<u8>, crate::Error> {
    use nodedb_query::msgpack_scan::{write_array_header, write_map_header, write_str};

    // MATCH bindings now carry user-visible node ids directly. The
    // CSR partition that produced them is tenant-scoped by
    // construction, so there is no `<tid>:` prefix to strip — what
    // the user inserted is what the user sees back.
    let mut buf = Vec::with_capacity(rows.len() * 64);
    write_array_header(&mut buf, rows.len());
    for row in rows {
        write_map_header(&mut buf, row.len());
        for (k, v) in row {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }
    }
    Ok(buf)
}

/// Execute a single MATCH clause.
pub(super) fn execute_clause(
    clause: &MatchClause,
    csr: &CsrIndex,
    input_rows: &[BindingRow],
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut result_rows = input_rows.to_vec();

    for chain in &clause.patterns {
        let mut next_rows = Vec::new();
        for row in &result_rows {
            next_rows.extend(execute_chain(chain, csr, row, state, frontier_bitmap)?);
        }
        result_rows = next_rows;
    }

    Ok(result_rows)
}

/// Execute a single pattern chain against a binding row.
///
/// Thin wrapper over [`continuation::run_chain_from`] that starts at triple 0
/// with the single supplied input row — the from-scratch execution path.
fn execute_chain(
    chain: &PatternChain,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
    continuation::run_chain_from(
        chain,
        0,
        vec![input_row.clone()],
        csr,
        state,
        frontier_bitmap,
    )
}

/// Execute a single triple `(src)-[edge]->(dst)` against a binding row.
///
/// `triple_idx` is the 0-based position of this triple within its chain;
/// it is recorded in any `UnresolvedExpansion` emitted.
pub(super) fn execute_triple(
    triple: &PatternTriple,
    triple_idx: usize,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
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
        for &src_id in &src_nodes {
            let expansion = expansion::expand_variable_length(
                csr,
                src_id,
                label_filter,
                direction,
                triple.edge.min_hops,
                triple.edge.max_hops,
                want_path,
            );
            if expansion.truncated {
                state.truncated = true;
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

            let neighbors = expansion::collect_neighbors(csr, src_id, label_filter, direction);
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

fn resolve_binding(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Vec<u32> {
    if let Some(ref name) = binding.name
        && let Some(value) = row.get(name)
    {
        if let Some(id) = csr.node_id_raw(value) {
            // Check label constraint if specified.
            if let Some(ref label) = binding.label
                && !csr.node_has_label(id, label)
            {
                return Vec::new();
            }
            return vec![id];
        }
        return Vec::new();
    }
    // No binding yet — enumerate all nodes, filtering by label and bitmap.
    let all = 0..csr.node_count() as u32;
    all.filter(|&id| {
        let label_ok = binding
            .label
            .as_ref()
            .is_none_or(|l| csr.node_has_label(id, l));
        let bitmap_ok = frontier_bitmap
            .is_none_or(|bm| bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id))));
        label_ok && bitmap_ok
    })
    .collect()
}

fn binding_compatible(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    node_id: u32,
) -> bool {
    // Check label constraint.
    if let Some(ref label) = binding.label
        && !csr.node_has_label(node_id, label)
    {
        return false;
    }
    if let Some(ref name) = binding.name
        && let Some(existing) = row.get(name)
    {
        return existing == csr.node_name_raw(node_id);
    }
    true
}

fn bind_node(row: &mut BindingRow, binding: &NodeBinding, csr: &CsrIndex, node_id: u32) {
    if let Some(ref name) = binding.name {
        row.entry(name.clone())
            .or_insert_with(|| csr.node_name_raw(node_id).to_string());
    }
}

/// LEFT JOIN: merge clause results with existing rows.
fn left_join_rows(
    input: &[BindingRow],
    clause_rows: &[BindingRow],
    clause: &MatchClause,
) -> Vec<BindingRow> {
    let new_vars: Vec<String> = clause
        .patterns
        .iter()
        .flat_map(|chain| {
            chain.triples.iter().flat_map(|t| {
                let mut vars = Vec::new();
                if let Some(ref n) = t.src.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.dst.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.edge.name {
                    vars.push(n.clone());
                }
                vars
            })
        })
        .collect();

    let mut result = Vec::new();

    for input_row in input {
        let matches: Vec<&BindingRow> = clause_rows
            .iter()
            .filter(|cr| {
                input_row
                    .iter()
                    .all(|(k, v)| cr.get(k).is_none_or(|cv| cv == v))
            })
            .collect();

        if matches.is_empty() {
            let mut row = input_row.clone();
            for var in &new_vars {
                row.entry(var.clone()).or_insert_with(|| "NULL".to_string());
            }
            result.push(row);
        } else {
            result.extend(matches.into_iter().cloned());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_social_graph() -> (CsrIndex, EdgeStore, tempfile::TempDir) {
        make_csr(&[
            ("alice", "KNOWS", "bob"),
            ("bob", "KNOWS", "carol"),
            ("carol", "KNOWS", "dave"),
            ("alice", "LIKES", "carol"),
            ("bob", "BLOCKED", "dave"),
        ])
    }

    #[test]
    fn execute_simple_one_hop() {
        let (csr, store, _dir) = make_social_graph();
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b")
                .unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "alice");
        assert_eq!(rows[0]["b"], "bob");
    }

    #[test]
    fn execute_two_hops() {
        let (csr, store, _dir) = make_social_graph();
        let query = super::super::compiler::parse(
            "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice' RETURN a, b, c",
        )
        .unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["c"], "carol");
    }

    #[test]
    fn execute_optional_match() {
        let (csr, store, _dir) = make_social_graph();
        let query = super::super::compiler::parse(
            "MATCH (a)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:LIKES]->(c) WHERE a = 'alice' RETURN a, b, c",
        ).unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["c"], "NULL");
    }

    #[test]
    fn execute_anti_join() {
        let (csr, store, _dir) = make_social_graph();
        let query = super::super::compiler::parse(
            "MATCH (a)-[:KNOWS]->(b) WHERE NOT EXISTS { MATCH (a)-[:BLOCKED]->(b) } RETURN a, b",
        )
        .unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn execute_with_limit() {
        let (csr, store, _dir) = make_social_graph();
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b LIMIT 2").unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn execute_empty_result() {
        let (csr, store, _dir) = make_social_graph();
        let query =
            super::super::compiler::parse("MATCH (a)-[:NONEXISTENT]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert!(rows.is_empty());
    }

    #[test]
    fn execute_with_node_labels() {
        let (mut csr, store, _dir) = make_social_graph();

        // Set labels.
        csr.add_node_label("alice", "Person").unwrap();
        csr.add_node_label("bob", "Person").unwrap();
        csr.add_node_label("carol", "Person").unwrap();
        csr.add_node_label("dave", "Bot").unwrap();

        // Without label filter — all KNOWS edges.
        let query = super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 3);

        // With label filter — only Person src.
        let query =
            super::super::compiler::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        // alice->bob, bob->carol, carol->dave — all 3 srcs are Person.
        assert_eq!(rows.len(), 3);

        // With label filter — only Bot dst.
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b:Bot) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        // Only carol->dave where dave is Bot.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "carol");
        assert_eq!(rows[0]["b"], "dave");

        // Both labels — Person->Bot.
        let query = super::super::compiler::parse("MATCH (a:Person)-[:KNOWS]->(b:Bot) RETURN a, b")
            .unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "carol");

        // Non-matching labels — should return 0.
        let query = super::super::compiler::parse("MATCH (a:Bot)-[:KNOWS]->(b:Person) RETURN a, b")
            .unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert!(rows.is_empty());
    }

    #[test]
    fn rows_to_msgpack_format() {
        let mut row = BindingRow::new();
        row.insert("a".into(), "alice".into());
        row.insert("b".into(), "bob".into());
        let msgpack = rows_to_msgpack(&[row]).unwrap();
        let json = nodedb_types::json_from_msgpack(&msgpack).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr[0]["a"], "alice");
    }

    /// Anchor nodes not in the frontier bitmap are never expanded as sources.
    /// alice (surrogate 1) is in the bitmap; bob and carol (surrogates 2, 3)
    /// are not. A free-variable MATCH should only yield rows where the src
    /// anchor is alice.
    #[test]
    fn match_frontier_bitmap_excludes_non_member_anchors() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let (mut csr, store, _dir) = make_social_graph();
        // Assign surrogates: alice=1, bob=2, carol=3, dave=4.
        csr.set_node_surrogate("alice", Surrogate::new(1));
        csr.set_node_surrogate("bob", Surrogate::new(2));
        csr.set_node_surrogate("carol", Surrogate::new(3));
        csr.set_node_surrogate("dave", Surrogate::new(4));

        // Bitmap contains only alice (surrogate 1).
        let bm = SurrogateBitmap::from_iter([Surrogate::new(1)]);

        let query = super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, Some(&bm), None).unwrap().rows;

        // Only alice->bob should appear; bob->carol and carol->dave are blocked
        // because the src anchor (bob, carol) is not in the bitmap.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "alice");
        assert_eq!(rows[0]["b"], "bob");
    }

    // ── Unresolved frontier tests ─────────────────────────────────────────

    /// Helper: build a CsrIndex from a list of `(src, label, dst)` edges.
    pub(crate) fn make_csr(
        edges: &[(&str, &str, &str)],
    ) -> (
        CsrIndex,
        crate::engine::graph::edge_store::EdgeStore,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store =
            crate::engine::graph::edge_store::EdgeStore::open(&dir.path().join("graph.redb"))
                .unwrap();

        use crate::engine::graph::edge_store::EdgeRef;
        use nodedb_types::{DatabaseId, TenantId};
        const DB: DatabaseId = DatabaseId::DEFAULT;
        const T: TenantId = TenantId::new(1);
        let mut ord = 0i64;
        for &(src, label, dst) in edges {
            ord += 1;
            store
                .put_edge_versioned(
                    EdgeRef::new(DB, T, "col", src, label, dst),
                    b"",
                    ord,
                    ord,
                    i64::MAX,
                )
                .unwrap();
        }
        let csr = crate::engine::graph::csr::rebuild::rebuild_from_store(&store).unwrap();
        (csr, store, dir)
    }

    /// Frontier emitted for a BOUND multi-hop intermediate: hop-1 binds `b`
    /// to "bob" via the output row; hop-2 receives `b` as a bound source
    /// (present in `input_row`). Bob has zero out-edges AND is flagged remote,
    /// so the executor records it in `unresolved_frontier`.
    ///
    /// The hop-1 source `a` is free-ranging (WHERE a='alice' is a
    /// post-processing predicate, not carried in the initial `input_row={}`),
    /// so it NEVER produces a frontier entry — only the bound intermediate
    /// `b` does.
    ///
    /// Graph: alice --KNOWS--> bob (bob has no out-edges)
    /// Query: MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice'
    /// bob is bound from hop-1; hop-2 finds bob has 0 out-edges AND bob is
    /// flagged remote → exactly one frontier entry.
    #[test]
    fn frontier_emitted_when_source_has_zero_out_degree() {
        let (csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
        let query = super::super::compiler::parse(
            "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice' RETURN a, b, c",
        )
        .unwrap();
        // "bob" is the hop-2 source with zero out-edges; mark it remote.
        let is_remote: &dyn Fn(&str) -> bool = &|name| name == "bob";
        let outcome = execute(&query, &csr, &store, None, Some(is_remote)).unwrap();

        // No local rows — bob has no out-edges locally.
        assert!(
            outcome.rows.is_empty(),
            "expected no rows; got {:?}",
            outcome.rows
        );

        // Exactly one frontier entry for the hop-2 source "bob".
        assert_eq!(
            outcome.unresolved_frontier.len(),
            1,
            "expected 1 frontier entry; got {:?}",
            outcome.unresolved_frontier.len()
        );
        let entry = &outcome.unresolved_frontier[0];
        assert_eq!(entry.node_name, "bob");
        assert_eq!(entry.triple_idx, 1, "hop-2 is triple_idx 1");
        // partial_row carries the hop-1 binding.
        assert_eq!(
            entry.partial_row.get("a").map(String::as_str),
            Some("alice")
        );
        assert_eq!(entry.partial_row.get("b").map(String::as_str), Some("bob"));
    }

    /// No false positive on label miss: a source HAS out-edges but none
    /// match the triple's label filter. The executor must NOT emit a frontier
    /// entry — this is a legitimate empty result, not a cross-shard gap.
    ///
    /// Two independent gates both suppress the frontier here:
    /// (1) The source variable `a` is free-ranging (WHERE a='alice' is a
    ///     post-processing predicate, not a binding carried in `input_row`),
    ///     so the bound gate suppresses emission.
    /// (2) Even if `a` were bound, alice has a LIKES out-edge so raw_degree
    ///     > 0 in the Out direction, which means the degree gate would also
    ///     suppress it — a node with local edges but no matching-label edges
    ///     is a legitimate empty result, not a cross-shard gap.
    ///
    /// We pass an all-remote predicate (`|_| true`) to prove that neither
    /// the degree gate nor the bound gate is defeated by the predicate alone.
    ///
    /// Graph: alice --LIKES--> bob (alice has LIKES out-edge, no KNOWS out-edge)
    /// Query: MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b
    #[test]
    fn no_frontier_when_source_has_edges_but_label_does_not_match() {
        let (csr, store, _dir) = make_csr(&[("alice", "LIKES", "bob")]);
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b")
                .unwrap();
        // All-remote predicate: even with every node marked remote, a source
        // that is free-ranging or has local edges must not produce a frontier
        // entry.
        let is_remote: &dyn Fn(&str) -> bool = &|_| true;
        let outcome = execute(&query, &csr, &store, None, Some(is_remote)).unwrap();

        assert!(outcome.rows.is_empty(), "expected no rows");
        assert!(
            outcome.unresolved_frontier.is_empty(),
            "must NOT emit frontier when source is free-ranging or has local edges; \
             got {:?}",
            outcome.unresolved_frontier
        );
    }

    /// Normal local expansion unchanged: a source with matching local edges
    /// produces rows as expected and the frontier stays empty.
    /// With `None` predicate no frontier entries can ever be emitted.
    #[test]
    fn no_frontier_for_fully_local_expansion() {
        let (csr, store, _dir) = make_social_graph();
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b")
                .unwrap();
        let outcome = execute(&query, &csr, &store, None, None).unwrap();

        assert_eq!(outcome.rows.len(), 1);
        assert_eq!(outcome.rows[0]["b"], "bob");
        assert!(
            outcome.unresolved_frontier.is_empty(),
            "frontier must be empty when all edges resolve locally"
        );
    }

    /// Multi-hop: a 2-triple pattern where hop-1 binds an intermediate that
    /// has no local out-edges for hop-2. The frontier entry must record
    /// `triple_idx == 1` and the partial_row carrying hop-1's binding.
    ///
    /// Graph: root --EDGE--> mid (mid has zero out-edges)
    /// Query: MATCH (x)-[:EDGE]->(y)-[:EDGE]->(z) WHERE x = 'root'
    /// "mid" is marked remote so the frontier is emitted for it.
    #[test]
    fn frontier_triple_idx_and_partial_row_for_multi_hop() {
        let (csr, store, _dir) = make_csr(&[("root", "EDGE", "mid")]);
        let query = super::super::compiler::parse(
            "MATCH (x)-[:EDGE]->(y)-[:EDGE]->(z) WHERE x = 'root' RETURN x, y, z",
        )
        .unwrap();
        // "mid" is the hop-2 source with zero out-edges; mark it remote.
        let is_remote: &dyn Fn(&str) -> bool = &|name| name == "mid";
        let outcome = execute(&query, &csr, &store, None, Some(is_remote)).unwrap();

        assert!(outcome.rows.is_empty());
        assert_eq!(
            outcome.unresolved_frontier.len(),
            1,
            "expected exactly one frontier entry for hop-2"
        );
        let entry = &outcome.unresolved_frontier[0];
        assert_eq!(entry.node_name, "mid", "frontier source should be mid");
        assert_eq!(entry.triple_idx, 1, "second triple is index 1");
        // partial_row must carry x=root and y=mid (hop-1 bindings).
        assert_eq!(entry.partial_row.get("x").map(String::as_str), Some("root"));
        assert_eq!(entry.partial_row.get("y").map(String::as_str), Some("mid"));
    }
}
