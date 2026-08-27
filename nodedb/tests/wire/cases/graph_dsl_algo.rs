// SPDX-License-Identifier: BUSL-1.1

//! Integration coverage for `GRAPH ALGO` output contracts.
//!
//! The Data Plane stores nodes under tenant-scoped keys
//! (`<tid>:<name>`) so that each tenant's subgraph is isolated in the
//! shared CSR index. That scoping is an internal addressing concern —
//! it must not cross the Data Plane → Control Plane boundary. Every
//! algorithm result emitter in `engine/graph/algo/*.rs` currently
//! calls `csr.node_name(...)` (which returns the scoped key) and
//! pushes it straight into `AlgoResultBatch`, leaking the prefix to
//! clients.
//!
//! This is the same design flaw the MATCH path had and fixed: the
//! API boundary is responsible for returning user-visible ids. These
//! tests assert that contract for every algorithm whose result schema
//! has a `node_id` text column:
//!
//! PAGERANK, COMMUNITY (LabelPropagation), WCC, LOUVAIN, DEGREE,
//! KCORE, CLOSENESS — and a round-trip test that a PAGERANK result
//! row is directly usable as an id in `WHERE id = ...` against the
//! home collection (the reported downstream-impact scenario).
//!
//! Diameter is intentionally excluded (global scalar, no node_id).
//! Triangles global mode emits a `__global__` sentinel that is not a
//! node and therefore excluded from the prefix check.

use crate::harness::TestServer;

fn assert_first_col_unscoped(rows: &[String], algo: &str) {
    assert!(!rows.is_empty(), "{algo} must return at least one row");
    for v in rows {
        assert!(
            !v.contains(':'),
            "{algo}: node_id must be unscoped (no `<tid>:` prefix); got {v:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_pagerank_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION memories").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'memories' FROM 'alice' TO 'bob' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'memories' FROM 'bob' TO 'carol' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO PAGERANK ON memories DAMPING 0.85 ITERATIONS 10 TOLERANCE 0.0001")
        .await
        .expect("PAGERANK must succeed");
    assert_first_col_unscoped(&rows, "PAGERANK");
    let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
    for expected in ["alice", "bob", "carol"] {
        assert!(
            got.contains(expected),
            "PAGERANK must return unscoped id {expected:?}; got {got:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_community_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION memories").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'memories' FROM 'alice' TO 'bob' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'memories' FROM 'bob' TO 'carol' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO COMMUNITY ON memories ITERATIONS 5 RESOLUTION 1.0")
        .await
        .expect("COMMUNITY must succeed");
    assert_first_col_unscoped(&rows, "COMMUNITY");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_wcc_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION wcc_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'wcc_nodes' FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO WCC ON wcc_nodes")
        .await
        .expect("WCC must succeed");
    assert_first_col_unscoped(&rows, "WCC");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_louvain_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION louv_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'louv_nodes' FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'louv_nodes' FROM 'b' TO 'c' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO LOUVAIN ON louv_nodes ITERATIONS 5 RESOLUTION 1.0")
        .await
        .expect("LOUVAIN must succeed");
    assert_first_col_unscoped(&rows, "LOUVAIN");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_degree_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION deg_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'deg_nodes' FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO DEGREE ON deg_nodes")
        .await
        .expect("DEGREE must succeed");
    assert_first_col_unscoped(&rows, "DEGREE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_kcore_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION kc_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'kc_nodes' FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'kc_nodes' FROM 'b' TO 'c' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'kc_nodes' FROM 'c' TO 'a' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO KCORE ON kc_nodes")
        .await
        .expect("KCORE must succeed");
    assert_first_col_unscoped(&rows, "KCORE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_closeness_returns_unscoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION cl_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'cl_nodes' FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'cl_nodes' FROM 'b' TO 'c' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO CLOSENESS ON cl_nodes")
        .await
        .expect("CLOSENESS must succeed");
    assert_first_col_unscoped(&rows, "CLOSENESS");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_pagerank_preserves_digit_prefixed_user_id() {
    // The Data Plane prepends `<tid>:` to store keys; the unscoper at
    // the exit boundary must strip *exactly that prefix*, not any
    // leading `\d+:` run. A user id that legitimately begins with
    // digits + `:` (e.g. `"77:node"` — a stringified composite key,
    // common when ids encode `shard:slug` or `year:slug`) must round-
    // trip intact. A heuristic stripper corrupts it.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION dp_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'dp_nodes' FROM '77:node' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO PAGERANK ON dp_nodes ITERATIONS 5 TOLERANCE 0.0001")
        .await
        .expect("PAGERANK must succeed");
    let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
    assert!(
        got.contains("77:node"),
        "PAGERANK must preserve digit-prefixed user id '77:node'; got {got:?}"
    );
    assert!(
        !got.contains("node"),
        "PAGERANK must not corrupt '77:node' into 'node' via heuristic strip; got {got:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_pagerank_node_id_is_joinable_with_collection_id() {
    // The reported downstream impact: consumers cannot feed `node_id`
    // into `WHERE id = '...'` because the scoped prefix makes it
    // non-comparable to the row id. This test drives the full
    // round-trip: INSERT rows keyed by the same ids used in edges, run
    // PAGERANK, then a SELECT on that id must resolve the real row.
    // Passes iff the algo output is directly usable as an id set.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION rank_join_docs")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rank_join_docs (id, label) VALUES ('alice', 'A')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO rank_join_docs (id, label) VALUES ('bob', 'B')")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'rank_join_docs' FROM 'alice' TO 'bob' TYPE 'l'")
        .await
        .unwrap();

    let rank_rows = server
        .query_text("GRAPH ALGO PAGERANK ON rank_join_docs ITERATIONS 5 TOLERANCE 0.0001")
        .await
        .expect("PAGERANK must succeed");
    let first = rank_rows
        .first()
        .expect("PAGERANK must return at least one row")
        .clone();
    assert!(
        !first.contains(':'),
        "PAGERANK node_id must be joinable with collection id; got scoped form {first:?}"
    );

    let joined = server
        .query_text_joined(&format!(
            "SELECT * FROM rank_join_docs WHERE id = '{first}'"
        ))
        .await
        .expect("SELECT must succeed");
    assert_eq!(
        joined.len(),
        1,
        "PAGERANK node_id {first:?} must join against collection id column; got {joined:?}"
    );
    // SELECT * over a multi-column row expands into one pgwire field per
    // column; the harness joins them with tabs, so an exact substring
    // match on the id value is sufficient (the id appears verbatim once).
    assert!(
        joined[0].contains(&first),
        "row resolved for PAGERANK node_id {first:?} must contain matching id; got {joined:?}"
    );
}

// ─── ON <collection> scoping ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_on_collection_excludes_nodes_from_other_collections() {
    // Two disconnected subgraphs live in two separate collections under the
    // same tenant. Running the algorithm with `ON coll_a` must return only
    // nodes whose graph home is coll_a — nodes from coll_b must be absent.
    // The current executor calls csr_partition(tid) unconditionally, so
    // both collections' nodes appear in every ON-scoped run.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION scope_a").await.unwrap();
    server.exec("CREATE COLLECTION scope_b").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'scope_a' FROM 'a0' TO 'a1' TYPE 'link'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'scope_a' FROM 'a1' TO 'a2' TYPE 'link'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'scope_b' FROM 'b0' TO 'b1' TYPE 'link'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'scope_b' FROM 'b1' TO 'b2' TYPE 'link'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO PAGERANK ON scope_a DAMPING 0.85 ITERATIONS 5 TOLERANCE 0.001")
        .await
        .expect("PAGERANK ON scope_a must succeed");

    let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
    for b_node in ["b0", "b1", "b2"] {
        assert!(
            !got.contains(b_node),
            "PAGERANK ON scope_a must not return node {b_node:?} from scope_b; got {got:?}"
        );
    }
    for a_node in ["a0", "a1", "a2"] {
        assert!(
            got.contains(a_node),
            "PAGERANK ON scope_a must include node {a_node:?}; got {got:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_on_nonexistent_collection_returns_empty_not_tenant_wide() {
    // A query scoped to a collection that has no graph edges must return an
    // empty result — not fall through to tenant-wide data.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION real_graph").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'real_graph' FROM 'x' TO 'y' TYPE 'link'")
        .await
        .unwrap();

    let rows = server
        .query_text(
            "GRAPH ALGO PAGERANK ON ghost_collection DAMPING 0.85 ITERATIONS 5 TOLERANCE 0.001",
        )
        .await
        .expect("PAGERANK ON unknown collection must not error — empty result expected");

    assert!(
        rows.is_empty(),
        "PAGERANK ON nonexistent collection must return empty; got {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_wcc_on_collection_excludes_nodes_from_other_collections() {
    // Confirms the scoping gap affects WCC, not just PAGERANK.
    // All 13 algorithm implementations read from the same unfiltered CSR.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION wcc_scope_a").await.unwrap();
    server.exec("CREATE COLLECTION wcc_scope_b").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'wcc_scope_a' FROM 'n0' TO 'n1' TYPE 'link'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'wcc_scope_b' FROM 'm0' TO 'm1' TYPE 'link'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH ALGO WCC ON wcc_scope_a")
        .await
        .expect("WCC ON wcc_scope_a must succeed");

    let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
    for b_node in ["m0", "m1"] {
        assert!(
            !got.contains(b_node),
            "WCC ON wcc_scope_a must not return node {b_node:?} from wcc_scope_b; got {got:?}"
        );
    }
}

// ─── EDGE_LABEL filtering ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_edge_label_excludes_unmatched_edge_types() {
    // EDGE_LABEL has no field on AlgoParams — dropped at parse time, full
    // tenant graph traversed. Nodes reachable only via 'links' edges must
    // not appear when EDGE_LABEL restricts traversal to 'derives'.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION el_graph").await.unwrap();
    // cluster_d: connected via 'derives' only
    server
        .exec("GRAPH INSERT EDGE IN 'el_graph' FROM 'd0' TO 'd1' TYPE 'derives'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'el_graph' FROM 'd1' TO 'd2' TYPE 'derives'")
        .await
        .unwrap();
    // cluster_l: connected via 'links' only — no 'derives' path to/from here
    server
        .exec("GRAPH INSERT EDGE IN 'el_graph' FROM 'l0' TO 'l1' TYPE 'links'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'el_graph' FROM 'l1' TO 'l2' TYPE 'links'")
        .await
        .unwrap();

    let rows = server
        .query_text(
            "GRAPH ALGO PAGERANK ON el_graph EDGE_LABEL 'derives' DAMPING 0.85 ITERATIONS 5 TOLERANCE 0.001",
        )
        .await
        .expect("PAGERANK with EDGE_LABEL must succeed");

    let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
    for links_only_node in ["l0", "l1", "l2"] {
        assert!(
            !got.contains(links_only_node),
            "PAGERANK EDGE_LABEL 'derives' must not include node {links_only_node:?} which is \
             reachable only via 'links' edges; got {got:?}"
        );
    }
}

// ─── ON (<subquery>) form ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_on_subquery_does_not_silently_return_tenant_wide_data() {
    // Tokenizer strips `(` `)` as whitespace so word_after("ON") returns
    // `"SELECT"` as the collection name, which is then ignored — silently
    // producing tenant-wide results. Must either scope to the subquery
    // node-id set or return an explicit error; silent wrong data is invalid.
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION subq_g TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, cluster STRING)",
        )
        .await
        .unwrap();
    for &(id, cluster) in &[
        ("a0", "a"),
        ("a1", "a"),
        ("a2", "a"),
        ("b0", "b"),
        ("b1", "b"),
        ("b2", "b"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO subq_g (id, cluster) VALUES ('{id}', '{cluster}')"
            ))
            .await
            .unwrap();
    }
    server
        .exec("GRAPH INSERT EDGE IN 'subq_g' FROM 'a0' TO 'a1' TYPE 'derives'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'subq_g' FROM 'a1' TO 'a2' TYPE 'derives'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'subq_g' FROM 'b0' TO 'b1' TYPE 'derives'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE IN 'subq_g' FROM 'b1' TO 'b2' TYPE 'derives'")
        .await
        .unwrap();

    let result = server
        .query_text(
            "GRAPH ALGO PAGERANK ON (SELECT id FROM subq_g WHERE cluster = 'a') \
             DAMPING 0.85 ITERATIONS 5 TOLERANCE 0.001",
        )
        .await;

    match result {
        Ok(rows) => {
            // If the subquery form succeeds it must scope to cluster='a' only.
            let got: std::collections::HashSet<&str> = rows.iter().map(String::as_str).collect();
            for b_node in ["b0", "b1", "b2"] {
                assert!(
                    !got.contains(b_node),
                    "PAGERANK ON (subquery) must not return node {b_node:?} from cluster_b; \
                     got {got:?}"
                );
            }
        }
        Err(_) => {
            // An explicit error is acceptable — the requirement is that the
            // subquery form does NOT silently return tenant-wide data.
            // Any error satisfies the contract.
        }
    }
}
