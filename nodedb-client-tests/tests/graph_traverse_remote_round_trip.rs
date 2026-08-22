// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDb::graph_traverse` returns the subgraph
//! reachable from edges inserted in the same session.
//!
//! After three `graph_insert_edge` calls fanning from a seed
//! (`a → b`, `b → c`, `a → s`), `graph_traverse(seed, depth=2)` must
//! return a non-empty `SubGraph` containing every reachable node.
//! An empty subgraph is indistinguishable from "the wire short-circuits
//! before the server's traversal runs" — the silent-fake pattern this
//! test guards against.

use nodedb_client::{NodeDb, NodeDbRemote, NodeId};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn graph_traverse_returns_inserted_subgraph() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    remote
        .execute_sql("CREATE COLLECTION smoke_g", &[])
        .await
        .expect("CREATE COLLECTION smoke_g must succeed");

    let a = NodeId::try_new("chunk_a").expect("fixture");
    let b = NodeId::try_new("chunk_b").expect("fixture");
    let c = NodeId::try_new("chunk_c").expect("fixture");
    let s = NodeId::try_new("sess").expect("fixture");

    remote
        .graph_insert_edge("smoke_g", &a, &b, "next", None)
        .await
        .expect("seed edge a->b");
    remote
        .graph_insert_edge("smoke_g", &b, &c, "next", None)
        .await
        .expect("seed edge b->c");
    remote
        .graph_insert_edge("smoke_g", &a, &s, "in_session", None)
        .await
        .expect("seed edge a->s");

    let sg = remote
        .graph_traverse("smoke_g", &a, 2, None)
        .await
        .expect("graph_traverse must complete against a populated graph");

    // Spec: with edges {a→b, b→c, a→s} present, a depth-2 traversal
    // from `a` reaches at least {a, b, c, s}. Returning an empty
    // subgraph reproduces the original bug: the wire returns success
    // with `nodes=[], edges=[]` indistinguishable from "the seed has
    // no out-edges within depth N".
    assert!(
        !sg.nodes.is_empty(),
        "depth-2 traversal from seed must surface reachable nodes, got empty subgraph \
         (regression: graph_traverse short-circuits after same-session graph_insert_edge)"
    );

    let node_ids: std::collections::HashSet<&str> =
        sg.nodes.iter().map(|n| n.id.as_str()).collect();
    assert!(
        node_ids.contains("chunk_b"),
        "depth-2 traversal must reach direct neighbor chunk_b; nodes={node_ids:?}"
    );
    assert!(
        node_ids.contains("chunk_c"),
        "depth-2 traversal must reach two-hop neighbor chunk_c; nodes={node_ids:?}"
    );
    assert!(
        node_ids.contains("sess"),
        "depth-2 traversal must reach direct neighbor sess; nodes={node_ids:?}"
    );

    // Edge regression guard: a populated traversal must also carry
    // the edges it crossed. The original bug returned an object with
    // both `nodes=[]` AND `edges=[]`; asserting on edges separately
    // catches a half-fix that surfaces nodes but drops edges.
    assert!(
        !sg.edges.is_empty(),
        "depth-2 traversal must surface the edges it crossed; got empty edges \
         (regression: traverse drops traversed edges from the wire response)"
    );

    server.graceful_shutdown().await;
}
