// SPDX-License-Identifier: BUSL-1.1

//! A plain (non-RETURNING) write routed through Calvin surfaces its ACTUAL
//! affected-row count, not a synthesized zero.
//!
//! An edge-bearing schemaless collection routes a predicate `DELETE FROM coll
//! WHERE <non-pk>` through the OLLP/Calvin dependent path (rewritten as a
//! `BulkDelete` so mirrored edges are cleaned atomically). The Calvin completion
//! ack carries no payload, so before this fix the applied Data-Plane response —
//! including the affected-row count — was deposited ONLY for a RETURNING write.
//! A plain delete therefore came back as `DELETE 0` even though it removed rows.
//!
//! This test proves the fix: the primary-write participant now deposits its full
//! applied response (count included) for a plain write too, so a multi-row
//! delete routed through Calvin reports the correct `DELETE N`.
//!
//! File name contains "calvin" within the cluster-tests crate so nextest applies
//! the cluster test-group serialization.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::cluster_harness::{TestClusterNode, wait_for};
use tokio_postgres::SimpleQueryMessage;

/// Documents seeded with `mark = 'del'` (all deleted by the predicate) and with
/// `mark = 'keep'` (all retained).
const TO_DELETE: usize = 3;
const TO_KEEP: usize = 3;

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
/// DELETE must wait for it before planning — otherwise the predicate delete
/// lowers to a fast-path bulk delete instead of the Calvin/OLLP `BulkDelete`.
fn collection_edge_bearing(node: &TestClusterNode, coll: &str) -> bool {
    node.shared
        .credentials
        .catalog()
        .load_collections_for_tenant(nodedb_types::DatabaseId::DEFAULT, 1)
        .map(|v| v.iter().any(|c| c.name == coll && c.has_implicit_edges))
        .unwrap_or(false)
}

/// Affected-row count carried by the first `CommandComplete` in a simple-query
/// response (PostgreSQL's `DELETE N` count).
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

/// A plain predicate `DELETE` on an edge-bearing collection routes through the
/// single-node Calvin OLLP path and reports the correct affected-row count
/// through the completion path (previously reported `DELETE 0`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_plain_delete_surfaces_affected_count() {
    // 4 Data-Plane cores so the edge endpoints land on distinct vShards: the
    // doc-delete participant carries the primary write (and its count) while the
    // edge-delete participants carry none — exercising the primary-write deposit
    // gate on a genuinely multi-participant transaction.
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

    let coll = "sncalvin_affected_count";
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

    // Seed implicit-edge documents (plain docs carrying _from/_to/_type, which
    // marks the collection edge-bearing). `mark = 'del'` docs will be matched by
    // the predicate delete; `mark = 'keep'` docs must survive.
    for i in 0..TO_DELETE {
        node.client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'del_{i}', _from: 'src_del_{i}', _to: 'hub', _type: 'l', mark: 'del' }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert deletable edge del_{i}: {e}"));
    }
    for i in 0..TO_KEEP {
        node.client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'keep_{i}', _from: 'src_keep_{i}', _to: 'hub', _type: 'l', mark: 'keep' }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert retained edge keep_{i}: {e}"));
    }

    // Wait for the implicit-edge mark to land so the predicate delete plans as a
    // `BulkDelete` and routes through Calvin (not the fast bulk path).
    wait_for(
        "collection marked edge-bearing",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || collection_edge_bearing(&node, coll),
    )
    .await;

    let admitted_before = sequencer_admitted(&node);

    // A non-PK predicate DELETE on the edge-bearing collection is a `BulkDelete`
    // routed through the OLLP/Calvin dependent path. Plain (no RETURNING): it
    // must report the actual number of matched rows.
    let msgs = node
        .client
        .simple_query(&format!("DELETE FROM {coll} WHERE mark = 'del'"))
        .await
        .expect("plain predicate delete routed through Calvin must complete");

    // Proof it traversed the sequencer→scheduler path (not a fast path that
    // never touches Calvin): the delete was admitted to a Calvin epoch.
    let admitted_after = sequencer_admitted(&node);
    assert!(
        admitted_after > admitted_before,
        "the predicate delete must be admitted to a Calvin epoch \
         (before={admitted_before}, after={admitted_after})"
    );

    // The core assertion: the reported affected count is the number of matched
    // rows, carried through the completion sidecar. Before the fix it was 0.
    let count = command_count(&msgs).expect("DELETE returns a CommandComplete count");
    assert_eq!(
        count, TO_DELETE as u64,
        "a plain DELETE routed through Calvin must report the actual affected-row \
         count ({TO_DELETE}), not a synthesized 0; got {count}"
    );

    // Sanity: exactly the matched documents are gone and the rest remain.
    let remaining = node
        .client
        .simple_query(&format!("SELECT * FROM {coll}"))
        .await
        .expect("SELECT all rows");
    let remaining_count = remaining
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        remaining_count, TO_KEEP,
        "exactly the mark='del' documents must be removed"
    );

    node.shutdown().await;
}
