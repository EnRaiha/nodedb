// SPDX-License-Identifier: BUSL-1.1

//! `CREATE COLLECTION ... FIELDS (id SERIAL, ...)` inside an open transaction
//! must not leak its implicit sequence into the shared, process-wide
//! `SequenceRegistry` before COMMIT.
//!
//! The auto-created sequence rides the same buffered-DDL `PutSequence`
//! proposal a standalone `CREATE SEQUENCE` uses. Installing it into the
//! shared registry unconditionally — rather than gating on
//! `ProposeOutcome::needs_local_apply()` — would let another connection see,
//! and consume values from, a sequence this transaction has not committed,
//! and would leave it stranded with no cleanup path if the transaction then
//! rolls back. This proves ROLLBACK leaves nothing behind: a different
//! connection must not see the sequence, and the collection name must be
//! free to reuse.

use crate::harness::TestServer;

/// Force the connection task to be repolled so the runtime can migrate it to
/// another worker between the statements of one transaction.
async fn hop_workers() {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// The first column of every row a raw `simple_query` returned, as text.
fn column_zero(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.get(0).unwrap_or("").to_string())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rolled_back_serial_collection_leaves_no_sequence_behind() {
    let server = TestServer::start().await;
    let (other, other_task) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("second pgwire session");

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE COLLECTION serial_leak_coll FIELDS (id SERIAL, name TEXT)")
        .await
        .expect("CREATE COLLECTION with a SERIAL column must buffer inside the transaction");
    hop_workers().await;

    // Mid-transaction: another connection must not see the buffered
    // sequence either — it is not yet committed, let alone the collection
    // that owns it.
    let mid_tx = other
        .simple_query("SHOW SEQUENCES")
        .await
        .expect("SHOW SEQUENCES must succeed on the other connection");
    assert!(
        !column_zero(&mid_tx)
            .iter()
            .any(|n| n == "serial_leak_coll_id_seq"),
        "a sequence this transaction has only buffered must not be visible to another \
         connection before COMMIT, got: {:?}",
        column_zero(&mid_tx)
    );

    server.exec("ROLLBACK").await.unwrap();

    // Post-ROLLBACK: the sequence must be gone from the shared registry —
    // on this connection and, decisively, on the other one too.
    let after_self = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(
        !after_self.iter().any(|n| n == "serial_leak_coll_id_seq"),
        "ROLLBACK must leave no trace of the SERIAL sequence, got: {after_self:?}"
    );
    let after_other = other
        .simple_query("SHOW SEQUENCES")
        .await
        .expect("SHOW SEQUENCES must succeed on the other connection");
    assert!(
        !column_zero(&after_other)
            .iter()
            .any(|n| n == "serial_leak_coll_id_seq"),
        "a rolled-back SERIAL sequence must not be visible to another connection, got: {:?}",
        column_zero(&after_other)
    );

    // The collection name must be free to reuse — nothing durable was ever
    // written for the rolled-back CREATE.
    server
        .exec("CREATE COLLECTION serial_leak_coll FIELDS (id SERIAL, name TEXT)")
        .await
        .expect("the collection name must be free to reuse after ROLLBACK");
    let after_recreate = server.query_text("SHOW SEQUENCES").await.unwrap();
    assert!(
        after_recreate
            .iter()
            .any(|n| n == "serial_leak_coll_id_seq"),
        "the real, committed CREATE must install its own sequence, got: {after_recreate:?}"
    );

    other_task.abort();
}
