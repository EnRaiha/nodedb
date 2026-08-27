// SPDX-License-Identifier: BUSL-1.1

//! An interactive transaction that writes one vShard and reads a DIFFERENT
//! vShard is a cross-shard transaction at COMMIT time — even though the
//! buffered write batch itself is single-shard. The real `nodedb` binary
//! defaults `single_node_calvin = true` (`config/server/section.rs`), so a
//! standalone spawned server always has a Calvin sequencer wired and elects
//! itself leader; the strict cross-shard COMMIT path
//! (`dispatch_strict_atomic_tasks_to_calvin`,
//! `control/planner/calvin/dispatch_multi.rs`) admits the batch, submits it
//! through the sequencer, and commits — it is never rejected with
//! `SequencerUnavailable` here. That rejection is reachable only when
//! `sequencer_inbox` is unset, which does not happen on this harness.

use crate::harness::TestServer;
use nodedb::types::VShardId;

/// Find two collection names whose vShards differ. Deterministic within a
/// process.
fn find_two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("xrd_col_{i}");
        let vshard =
            VShardId::from_collection_in_database(nodedb::types::DatabaseId::DEFAULT, &name)
                .as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_one_shard_read_another_commits_via_calvin_in_explicit_txn() {
    let server = TestServer::start().await;

    let (col_a, col_b) = find_two_distinct_collections();

    server
        .exec(&format!(
            "CREATE COLLECTION {col_a} (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION col_a");
    server
        .exec(&format!(
            "CREATE COLLECTION {col_b} (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION col_b");

    // Seed col_b with a row before the transaction so the in-transaction
    // SELECT reads real committed data, not just an absent-key phantom.
    server
        .exec(&format!("INSERT INTO {col_b} (id, val) VALUES ('b1', 1)"))
        .await
        .expect("seed col_b");

    server.exec("BEGIN").await.unwrap();

    // WRITE to col_a: a single-shard write by itself.
    server
        .exec(&format!("INSERT INTO {col_a} (id, val) VALUES ('a1', 1)"))
        .await
        .expect("write to col_a should succeed at statement time (single-shard write)");

    // READ from col_b: a DIFFERENT vShard than the one just written.
    let read_rows = server
        .query_text(&format!("SELECT id FROM {col_b} WHERE id = 'b1'"))
        .await
        .expect("read from col_b should succeed at statement time");
    assert_eq!(read_rows, vec!["b1".to_string()]);

    // COMMIT: the write-shard (col_a) union read-shard (col_b) read set now
    // spans 2 vShards, so classify_dispatch reports MultiShard. This node's
    // own single-node Calvin sequencer admits and commits the batch.
    server.exec("COMMIT").await.expect(
        "COMMIT must succeed: the single-node Calvin sequencer admits this cross-shard batch",
    );

    // The committed write must be visible, and the pre-existing read row
    // must be unaffected.
    let rows_a = server
        .query_text(&format!("SELECT id FROM {col_a} WHERE id = 'a1'"))
        .await
        .expect("post-commit read of col_a should succeed");
    assert_eq!(
        rows_a,
        vec!["a1".to_string()],
        "write to col_a must have persisted after the committed cross-shard transaction"
    );
    let rows_b = server
        .query_text(&format!("SELECT id FROM {col_b} WHERE id = 'b1'"))
        .await
        .expect("post-commit read of col_b should succeed");
    assert_eq!(
        rows_b,
        vec!["b1".to_string()],
        "pre-existing row in col_b must still be present after the committed cross-shard transaction"
    );
}
