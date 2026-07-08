// SPDX-License-Identifier: BUSL-1.1

//! A `DELETE ... RETURNING` routed through Calvin surfaces its deleted rows,
//! not a bare command tag.
//!
//! An edge-bearing schemaless collection routes a `DELETE FROM coll WHERE id =
//! 'x'` through the OLLP/Calvin dependent path (the implicit-edge routing gate
//! rewrites it as a `BulkDelete` so mirrored edges are cleaned atomically). The
//! Calvin completion path signals done via a Raft-replicated ack that carries no
//! payload, so before this fix the applied Data-Plane response — including the
//! `RETURNING` rows — was dropped and the client saw only `DELETE 1`.
//!
//! This test proves the fix: with the single-node Calvin stack on, deleting one
//! implicit-edge document with `RETURNING *` returns that document's row through
//! the completion path.
//!
//! File name contains "calvin" within the cluster-tests crate so nextest applies
//! the cluster test-group serialization.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::cluster_harness::{TestClusterNode, wait_for};

/// Number of implicit-edge documents seeded before the RETURNING delete.
const SOURCES: usize = 6;

/// Count of transactions the single-node sequencer has admitted to an epoch, or
/// `0` if the sequencer metrics handle is not installed yet.
fn sequencer_admitted(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Whether `coll` is flagged edge-bearing in this node's local catalog. The
/// implicit-edge mark is committed via the replicated metadata path, so the
/// DELETE must wait for it before planning — otherwise the PK delete lowers to a
/// static `PointDelete` (fast path) instead of the Calvin/OLLP `BulkDelete`.
fn collection_edge_bearing(node: &TestClusterNode, coll: &str) -> bool {
    node.shared
        .credentials
        .catalog()
        .load_collections_for_tenant(nodedb_types::DatabaseId::DEFAULT, 1)
        .map(|v| v.iter().any(|c| c.name == coll && c.has_implicit_edges))
        .unwrap_or(false)
}

/// Flag ON: a `DELETE ... RETURNING` on an edge-bearing collection routes
/// through the single-node Calvin OLLP path and returns the deleted row through
/// the completion path (previously dropped as a bare tag).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_returning_delete_surfaces_deleted_row() {
    // 4 Data-Plane cores so the edge endpoints land on distinct vShards — the
    // doc-delete participant carries the RETURNING rows while the edge-delete
    // participants carry none, exercising the deposit gate.
    let node = TestClusterNode::spawn_single_node_calvin(4)
        .await
        .expect("spawn standalone single-node-calvin server");

    // The lone sequencer voter self-elects; wait for it before submitting.
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.sequencer_leader() == node.node_id,
    )
    .await;

    // The collection name deliberately embeds "returning": it doubles as
    // regression coverage that a `*_returning` identifier is not mistaken for the
    // RETURNING keyword by the clause stripper.
    let coll = "sncalvin_returning";
    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "collection visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;

    // src_0..src_(SOURCES-1) -> hub as IMPLICIT edges (plain docs carrying
    // _from/_to/_type). Inserting an edge document marks the collection
    // edge-bearing, which is what routes the later DELETE through OLLP/Calvin.
    for i in 0..SOURCES {
        node.client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub', _type: 'l', mark: 'keep' }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert implicit edge src_{i} -> hub: {e}"));
    }

    // Wait for the implicit-edge mark to land so the PK delete plans as a
    // `BulkDelete` and routes through Calvin (not the fast `PointDelete` path).
    wait_for(
        "collection marked edge-bearing",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || collection_edge_bearing(&node, coll),
    )
    .await;

    let admitted_before = sequencer_admitted(&node);

    // A PK-equality DELETE on the edge-bearing collection is rewritten to a
    // `BulkDelete` (id filter) and routed through the OLLP/Calvin dependent path.
    // With RETURNING it must come back carrying the deleted document's row.
    let msgs = node
        .client
        .simple_query(&format!(
            "DELETE FROM {coll} WHERE id = 'edge_3' RETURNING *"
        ))
        .await
        .expect("RETURNING delete routed through Calvin must complete");

    // Proof it traversed the sequencer→scheduler path (not the fast PointDelete
    // path that never touches Calvin): the delete was admitted to a Calvin epoch.
    let admitted_after = sequencer_admitted(&node);
    assert!(
        admitted_after > admitted_before,
        "the RETURNING delete must be admitted to a Calvin epoch \
         (before={admitted_before}, after={admitted_after})"
    );

    let rows: Vec<&tokio_postgres::SimpleQueryRow> = msgs
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();

    // The core assertion: the response CONTAINS the deleted row, not just a bare
    // `DELETE 1` command tag. Before the fix this vec is empty.
    assert_eq!(
        rows.len(),
        1,
        "DELETE ... RETURNING routed through Calvin must surface exactly the one \
         deleted row, not a bare command tag; got {} row(s)",
        rows.len()
    );
    let id = rows[0].get("id").expect("returned row has an id column");
    assert_eq!(
        id, "edge_3",
        "the returned row must be the deleted document (id = 'edge_3'); got id = {id:?}"
    );

    // Sanity: the deleted document is actually gone and the others remain.
    let remaining = node
        .client
        .simple_query(&format!("SELECT * FROM {coll}"))
        .await
        .expect("SELECT all rows");
    let remaining_count = remaining
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        remaining_count,
        SOURCES - 1,
        "exactly the RETURNING-deleted document must be removed"
    );

    node.shutdown().await;
}
