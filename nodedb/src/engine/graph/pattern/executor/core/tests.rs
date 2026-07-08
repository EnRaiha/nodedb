// SPDX-License-Identifier: BUSL-1.1

use super::super::types::BindingRow;
use super::*;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;

/// Open a standalone `SparseEngine` in a fresh tempdir for tests that need
/// to exercise the property-predicate path (or just to satisfy the
/// `PropertyLookup` borrow for tests that have no property predicates).
pub(crate) fn make_sparse() -> (SparseEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let sparse = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
    (sparse, dir)
}

/// Build a `PropertyLookup` over `sparse` + `csr` scoped to the `make_csr`
/// graph's `(DatabaseId::DEFAULT, TenantId::new(1), "col")`.
///
/// `csr` resolves a bound node name to its surrogate; the document is then
/// fetched at `surrogate_to_doc_id(surrogate)`, mirroring the real keying.
pub(crate) fn props_for<'a>(sparse: &'a SparseEngine, csr: &'a CsrIndex) -> PropertyLookup<'a> {
    PropertyLookup {
        sparse,
        csr,
        database_id: 0,
        tenant_id: 1,
        collection: Some("col"),
    }
}

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
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b",
    )
    .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["a"], "alice");
    assert_eq!(rows[0]["b"], "bob");
}

#[test]
fn execute_two_hops() {
    let (csr, store, _dir) = make_social_graph();
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice' RETURN a, b, c",
    )
    .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["c"], "carol");
}

#[test]
fn execute_optional_match() {
    let (csr, store, _dir) = make_social_graph();
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:LIKES]->(c) WHERE a = 'alice' RETURN a, b, c",
    )
    .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["c"], "NULL");
}

#[test]
fn execute_anti_join() {
    let (csr, store, _dir) = make_social_graph();
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) WHERE NOT EXISTS { MATCH (a)-[:BLOCKED]->(b) } RETURN a, b",
    )
    .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 3);
}

#[test]
fn execute_with_limit() {
    let (csr, store, _dir) = make_social_graph();
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b LIMIT 2")
        .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 2);
}

#[test]
fn execute_empty_result() {
    let (csr, store, _dir) = make_social_graph();
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query =
        super::super::super::compiler::parse("MATCH (a)-[:NONEXISTENT]->(b) RETURN a, b").unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert!(rows.is_empty());
}

#[test]
fn execute_with_node_labels() {
    let (mut csr, store, _dir) = make_social_graph();
    let (sparse, _sdir) = make_sparse();

    // Set labels.
    csr.add_node_label("alice", "Person").unwrap();
    csr.add_node_label("bob", "Person").unwrap();
    csr.add_node_label("carol", "Person").unwrap();
    csr.add_node_label("dave", "Bot").unwrap();

    // Build the lookup AFTER mutating the CSR so the immutable borrow does
    // not overlap the `set`/`add` calls above.
    let props = props_for(&sparse, &csr);

    // Without label filter — all KNOWS edges.
    let query =
        super::super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 3);

    // With label filter — only Person src.
    let query =
        super::super::super::compiler::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b").unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    // alice->bob, bob->carol, carol->dave — all 3 srcs are Person.
    assert_eq!(rows.len(), 3);

    // With label filter — only Bot dst.
    let query =
        super::super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b:Bot) RETURN a, b").unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    // Only carol->dave where dave is Bot.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["a"], "carol");
    assert_eq!(rows[0]["b"], "dave");

    // Both labels — Person->Bot.
    let query =
        super::super::super::compiler::parse("MATCH (a:Person)-[:KNOWS]->(b:Bot) RETURN a, b")
            .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["a"], "carol");

    // Non-matching labels — should return 0.
    let query =
        super::super::super::compiler::parse("MATCH (a:Bot)-[:KNOWS]->(b:Person) RETURN a, b")
            .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
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
    let (sparse, _sdir) = make_sparse();
    // Assign surrogates: alice=1, bob=2, carol=3, dave=4.
    csr.set_node_surrogate("alice", Surrogate::new(1));
    csr.set_node_surrogate("bob", Surrogate::new(2));
    csr.set_node_surrogate("carol", Surrogate::new(3));
    csr.set_node_surrogate("dave", Surrogate::new(4));
    // Build the lookup AFTER mutating the CSR so the immutable borrow does
    // not overlap the surrogate assignments above.
    let props = props_for(&sparse, &csr);

    // Bitmap contains only alice (surrogate 1).
    let bm = SurrogateBitmap::from_iter([Surrogate::new(1)]);

    let query =
        super::super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: Some(&bm),
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;

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
        crate::engine::graph::edge_store::EdgeStore::open(&dir.path().join("graph.redb")).unwrap();

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
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice' RETURN a, b, c",
    )
    .unwrap();
    // "bob" is the hop-2 source with zero out-edges; mark it remote.
    let is_remote: &dyn Fn(&str) -> bool = &|name| name == "bob";
    let outcome = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: Some(is_remote),
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap();

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
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b",
    )
    .unwrap();
    // All-remote predicate: even with every node marked remote, a source
    // that is free-ranging or has local edges must not produce a frontier
    // entry.
    let is_remote: &dyn Fn(&str) -> bool = &|_| true;
    let outcome = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: Some(is_remote),
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap();

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
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b",
    )
    .unwrap();
    let outcome = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap();

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
    let (sparse, _sdir) = make_sparse();
    let props = props_for(&sparse, &csr);
    let query = super::super::super::compiler::parse(
        "MATCH (x)-[:EDGE]->(y)-[:EDGE]->(z) WHERE x = 'root' RETURN x, y, z",
    )
    .unwrap();
    // "mid" is the hop-2 source with zero out-edges; mark it remote.
    let is_remote: &dyn Fn(&str) -> bool = &|name| name == "mid";
    let outcome = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: Some(is_remote),
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap();

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

// ── Property-predicate filtering (the silent-wrong-results bug fix) ────────

/// Store a node-property document in collection `"col"` (matching
/// `make_csr`'s `(DatabaseId::DEFAULT, TenantId::new(1))` scope), keyed by
/// `surrogate_to_doc_id(surrogate)` — the REAL document key. A graph node
/// and its same-pk document share one surrogate, so the caller assigns the
/// same surrogate to the node in the CSR via `set_node_surrogate`.
fn put_node_doc(
    sparse: &SparseEngine,
    surrogate: nodedb_types::Surrogate,
    doc: nodedb_types::Value,
) {
    use crate::engine::document::store::key::surrogate_to_doc_id;
    let bytes = nodedb_types::value_to_msgpack(&doc).unwrap();
    sparse
        .put(0, 1, "col", &surrogate_to_doc_id(surrogate), &bytes)
        .unwrap();
}

fn obj(pairs: &[(&str, nodedb_types::Value)]) -> nodedb_types::Value {
    let mut map = std::collections::HashMap::new();
    for (k, v) in pairs {
        map.insert(k.to_string(), v.clone());
    }
    nodedb_types::Value::Object(map)
}

/// `WHERE a.age = '30'` now FILTERS by the node's stored document instead
/// of matching every row (the stub bug). alice(age=30) and bob(age=25) both
/// have a KNOWS edge to carol/dave; only alice survives the predicate.
#[test]
fn property_equals_filters_by_stored_document() {
    let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "carol"), ("bob", "KNOWS", "dave")]);
    let (sparse, _sdir) = make_sparse();
    // alice/bob share their surrogate with their stored document (the real
    // keying): node → surrogate → surrogate_to_doc_id → sparse.
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    csr.set_node_surrogate("bob", nodedb_types::Surrogate::new(2));
    let props = props_for(&sparse, &csr);
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[("age", nodedb_types::Value::Integer(30))]),
    );
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(2),
        obj(&[("age", nodedb_types::Value::Integer(25))]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a.age = '30' RETURN a, b",
    )
    .unwrap();
    assert_eq!(query.collection.as_deref(), Some("col"));

    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;

    assert_eq!(rows.len(), 1, "only alice(age=30) matches");
    assert_eq!(rows[0]["a"], "alice");
    assert_eq!(rows[0]["b"], "carol");
}

/// A predicate matching no node's stored value returns zero rows (proving
/// the predicate truly evaluates, not a no-op pass).
#[test]
fn property_equals_no_match_returns_empty() {
    let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "carol")]);
    let (sparse, _sdir) = make_sparse();
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    let props = props_for(&sparse, &csr);
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[("age", nodedb_types::Value::Integer(30))]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a.age = '99' RETURN a, b",
    )
    .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert!(rows.is_empty(), "no node has age=99; got {rows:?}");
}

/// A node with NO stored document cannot satisfy a property predicate and
/// is excluded (`Ok(false)`), even though it has a matching edge.
#[test]
fn property_predicate_excludes_node_without_document() {
    let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "carol"), ("bob", "KNOWS", "dave")]);
    let (sparse, _sdir) = make_sparse();
    // Both nodes have a surrogate, but only alice has a document stored at
    // its surrogate key; bob's surrogate resolves to no stored row → fetch
    // returns None → bob is excluded.
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    csr.set_node_surrogate("bob", nodedb_types::Surrogate::new(2));
    let props = props_for(&sparse, &csr);
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[("age", nodedb_types::Value::Integer(30))]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a.age >= '20' RETURN a, b",
    )
    .unwrap();
    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;
    assert_eq!(rows.len(), 1, "bob has no document so is excluded");
    assert_eq!(rows[0]["a"], "alice");
}

/// A property predicate with NO `IN '<collection>'` clause is unresolvable
/// and must return a typed `BadRequest` error — never silently pass/drop.
#[test]
fn property_predicate_without_collection_is_bad_request() {
    let (csr, store, _dir) = make_csr(&[("alice", "KNOWS", "carol")]);
    let (sparse, _sdir) = make_sparse();
    // No collection on the lookup (mirrors a query without `IN '...'`).
    // The BadRequest fires before any fetch, so no surrogate is needed.
    let props = PropertyLookup {
        sparse: &sparse,
        csr: &csr,
        database_id: 0,
        tenant_id: 1,
        collection: None,
    };
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[("age", nodedb_types::Value::Integer(30))]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) WHERE a.age = '30' RETURN a, b",
    )
    .unwrap();
    assert_eq!(query.collection, None);

    let result = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    );
    // `MatchOutcome` (the Ok type) is not `Debug`, so match on the Result
    // directly rather than `unwrap_err()`.
    assert!(
        matches!(result, Err(crate::Error::BadRequest { .. })),
        "expected BadRequest error for a property predicate with no IN collection"
    );
}

/// Direct `check_property` coverage for each `ComparisonOp` plus the
/// missing-field and missing-document branches, against a real
/// `SparseEngine`.
#[test]
fn check_property_ops_against_real_sparse_engine() {
    use super::super::predicates::check_property_for_test as check;

    let (mut csr, _store, _dir) = make_csr(&[("alice", "KNOWS", "carol")]);
    let (sparse, _sdir) = make_sparse();
    // alice has a surrogate + document; "ghost" is unknown to the CSR so its
    // surrogate resolves to None → missing-document branch.
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[
            ("age", nodedb_types::Value::Integer(30)),
            ("name", nodedb_types::Value::String("alice".into())),
        ]),
    );
    let props = props_for(&sparse, &csr);

    // Eq / Neq (numeric coercion: stored Integer(30) vs literal "30").
    assert!(check(&props, "alice", "age", &ComparisonOp::Eq, "30").unwrap());
    assert!(!check(&props, "alice", "age", &ComparisonOp::Eq, "31").unwrap());
    assert!(check(&props, "alice", "age", &ComparisonOp::Neq, "31").unwrap());
    // Ordering.
    assert!(check(&props, "alice", "age", &ComparisonOp::Lt, "40").unwrap());
    assert!(!check(&props, "alice", "age", &ComparisonOp::Lt, "30").unwrap());
    assert!(check(&props, "alice", "age", &ComparisonOp::Lte, "30").unwrap());
    assert!(check(&props, "alice", "age", &ComparisonOp::Gt, "20").unwrap());
    assert!(check(&props, "alice", "age", &ComparisonOp::Gte, "30").unwrap());
    // String field equality.
    assert!(check(&props, "alice", "name", &ComparisonOp::Eq, "alice").unwrap());
    // Missing field → false.
    assert!(!check(&props, "alice", "missing", &ComparisonOp::Eq, "x").unwrap());
    // Missing document → false.
    assert!(!check(&props, "ghost", "age", &ComparisonOp::Eq, "30").unwrap());
}

// ── Property projection (`RETURN a.field`) ────────────────────────────────

/// `RETURN a.name, a.age` now projects the node's STORED property values
/// instead of the old `"NULL"` stub. alice's document `{name, age}` resolves
/// to the string forms "Alice" / "30" via the canonical value display
/// convention. The non-dotted `b` column still projects the node identity.
#[test]
fn property_projection_returns_stored_values() {
    let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
    let (sparse, _sdir) = make_sparse();
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    let props = props_for(&sparse, &csr);
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[
            ("name", nodedb_types::Value::String("Alice".into())),
            ("age", nodedb_types::Value::Integer(30)),
        ]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.name, a.age, b",
    )
    .unwrap();

    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["a.name"], "Alice", "string property projected");
    assert_eq!(rows[0]["a.age"], "30", "integer property stringified");
    assert_eq!(
        rows[0]["b"], "bob",
        "non-dotted identity projection unchanged"
    );
}

/// A node WITHOUT a stored document projects `"NULL"` for a property column
/// (SQL projection: missing row → NULL), not an error.
#[test]
fn property_projection_no_document_is_null() {
    let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
    let (sparse, _sdir) = make_sparse();
    // alice has a surrogate but NO document stored at its key → fetch None.
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    let props = props_for(&sparse, &csr);

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.name",
    )
    .unwrap();

    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["a.name"], "NULL", "absent document → NULL");
}

/// A property column whose `field` is absent from an EXISTING document
/// projects `"NULL"` (missing field → NULL), not an error.
#[test]
fn property_projection_missing_field_is_null() {
    let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
    let (sparse, _sdir) = make_sparse();
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    let props = props_for(&sparse, &csr);
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[("name", nodedb_types::Value::String("Alice".into()))]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.age",
    )
    .unwrap();

    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["a.age"], "NULL", "missing field → NULL");
}

/// Property PROJECTION with no `IN '<collection>'` clause is unresolvable and
/// must return a typed `BadRequest` — the same rule as the predicate path,
/// never a silent `"NULL"`.
#[test]
fn property_projection_without_collection_is_bad_request() {
    let (csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
    let (sparse, _sdir) = make_sparse();
    // BadRequest fires before any fetch, so no surrogate is needed.
    let props = PropertyLookup {
        sparse: &sparse,
        csr: &csr,
        database_id: 0,
        tenant_id: 1,
        collection: None,
    };
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[("name", nodedb_types::Value::String("Alice".into()))]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a.name",
    )
    .unwrap();
    assert_eq!(query.collection, None);

    let result = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    );
    assert!(
        matches!(result, Err(crate::Error::BadRequest { .. })),
        "expected BadRequest for a property projection with no IN collection"
    );
}

/// Aliased property projection (`RETURN a.name AS who`) keys the output by
/// the alias, and a plain `RETURN a` still projects the node identity.
#[test]
fn property_projection_alias_and_identity() {
    let (mut csr, store, _dir) = make_csr(&[("alice", "KNOWS", "bob")]);
    let (sparse, _sdir) = make_sparse();
    csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
    let props = props_for(&sparse, &csr);
    put_node_doc(
        &sparse,
        nodedb_types::Surrogate::new(1),
        obj(&[("name", nodedb_types::Value::String("Alice".into()))]),
    );

    let query = super::super::super::compiler::parse(
        "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a = 'alice' RETURN a.name AS who, a",
    )
    .unwrap();

    let rows = execute(
        &query,
        MatchExecCtx {
            csr: &csr,
            edge_store: &store,
            frontier_bitmap: None,
            is_remote_node: None,
            varlen_caps: expansion::VarLenCaps::default(),
            props: &props,
            overlay: None,
        },
    )
    .unwrap()
    .rows;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["who"], "Alice", "property keyed by alias");
    assert_eq!(rows[0]["a"], "alice", "non-dotted identity unchanged");
}
