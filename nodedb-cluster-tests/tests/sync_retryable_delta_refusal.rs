// SPDX-License-Identifier: BUSL-1.1
//! A CRDT delta Origin refuses *retryably* must reach the edge as a retryable
//! `DeltaAck { status: Gap }`, never as a terminal `DeltaReject`.
//!
//! ## What this guards
//!
//! Origin refuses a peer delta in two structurally different ways, and the two
//! demand opposite client behaviour:
//!
//! * **Terminal** — a constraint the delta will never satisfy (UNIQUE
//!   collision, malformed bytes). The edge must roll the optimistic write back
//!   and compensate. Origin advances the producer high-water-mark, because a
//!   re-push would fail identically and holding the stream buys nothing.
//! * **Retryable** — nothing was applied and the *same bytes at the same seq*
//!   are expected to succeed once a transient precondition resolves. Origin
//!   deliberately holds the high-water-mark back so the re-push is admitted
//!   rather than deduplicated away as a `Duplicate`.
//!
//! The held high-water-mark only pays off if the edge is actually told to
//! re-push. An edge that receives a terminal `DeltaReject` retires the write
//! and never sends it again, while Origin sits holding the stream open for a
//! re-push that will never arrive — the write is gone from the client's
//! perspective while every server-side counter stays green. These tests pin the
//! client-visible frame for the retryable refusals so that shape cannot recur.
//!
//! ## The retryable refusal exercised here
//!
//! A delta whose causal predecessors are absent from the target collection's
//! document: Loro buffers the operations as *pending* and the applied state
//! does not move. This is routine after a partial resync — the edge holds
//! operations whose history Origin has not received yet — and it resolves the
//! moment the missing history arrives, which is precisely what makes it
//! retryable rather than terminal.
//!
//! The sibling retryable refusal, a delta admitted against a constraint version
//! the accepting replica has not installed yet, is covered by
//! `sync_constraint_version_fence.rs`.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::sync::wire::AckStatus;

const COLL: &str = "notes";
const DOC: &str = "doc1";
const PEER_ID: u64 = 11;
/// The stream seq every push in these tests uses. A real edge assigns a seq
/// once and reuses it across re-sends, so pinning one constant here mirrors
/// production rather than papering over the dedup semantics with fresh seqs.
const SEQ: u64 = 1;

/// Loro blobs for one document written twice: the update carrying **only** the
/// second commit, and a snapshot carrying the complete history.
///
/// Importing `gapped_update` into a document that has never seen the first
/// commit leaves its operations causally pending — nothing is applied.
/// Importing `full_history` afterwards supplies the missing predecessor and the
/// same logical write lands cleanly.
fn split_history(collection: &str, row_id: &str) -> (Vec<u8>, Vec<u8>) {
    let doc = loro::LoroDoc::new();
    let coll = doc.get_map(collection);
    let row = coll
        .insert_container(row_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("body", "first").expect("first field write");
    doc.commit();
    let after_first = doc.oplog_vv();

    row.insert("body", "second").expect("second field write");
    doc.commit();

    let gapped_update = doc
        .export(loro::ExportMode::updates_owned(after_first))
        .expect("export updates since the first commit");
    let full_history = doc
        .export(loro::ExportMode::Snapshot)
        .expect("export full snapshot");
    (gapped_update, full_history)
}

/// Read `crdt_state(COLL, DOC)`. `Ok(None)` means the document is absent — the
/// CRDT read path answers a missing document with `NotFound`, a deterministic
/// terminal outcome rather than a transient error. Any other error is treated
/// as catch-up lag and retried until `timeout`.
async fn read_crdt_doc(
    client: &tokio_postgres::Client,
    timeout: Duration,
) -> Result<Option<String>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT crdt_state('{COLL}', '{DOC}')"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        return Ok(Some(r.get(0).unwrap_or("").to_string()));
                    }
                }
                return Ok(Some(String::new()));
            }
            Err(e)
                if e.as_db_error()
                    .is_some_and(|d| d.message().contains("NotFound")) =>
            {
                return Ok(None);
            }
            Err(e) => {
                if Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    continue;
                }
                return Err(format!(
                    "crdt_state query failed: code={:?} msg={:?}",
                    e.code().map(|c| c.code()),
                    e.as_db_error().map(|d| d.message()),
                ));
            }
        }
    }
}

/// Bring up a three-node cluster with `COLL` created and a sync client
/// connected to node 0's sync listener.
async fn cluster_with_sync_client() -> (TestCluster, SyncTestClient) {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION notes");

    let cfg = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) = nodedb::control::shutdown::ShutdownBus::new(
        std::sync::Arc::new(nodedb::control::shutdown::ShutdownWatch::new()),
    );
    let state = start_sync_listener(
        cfg,
        Some(std::sync::Arc::clone(&cluster.nodes[0].shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener");
    let client = SyncTestClient::connect(state.config.listen_addr)
        .await
        .expect("sync handshake with node 0");
    (cluster, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn causally_gapped_delta_is_acked_as_a_retryable_gap() {
    let (cluster, mut client) = cluster_with_sync_client().await;
    let (gapped_update, _full_history) = split_history(COLL, DOC);

    let outcome = client
        .push_delta_at_seq(COLL, DOC, PEER_ID, 1, SEQ, gapped_update)
        .await
        .expect("push causally-gapped delta");

    // The refusal is retryable, so it must ride the ack channel. A
    // `DeltaReject` here is the silent-loss shape: the edge retires the write
    // and Origin keeps holding the high-water-mark for a re-push that the edge
    // has already decided never to send.
    let ack = match outcome {
        DeltaOutcome::Ack(ack) => ack,
        DeltaOutcome::Reject(reject) => panic!(
            "a causally-gapped delta is retryable and must be acked as Gap, but Origin sent a \
             terminal DeltaReject (reason={:?}, compensation={:?}) — the edge will roll the write \
             back and never re-push it",
            reject.reason, reject.compensation
        ),
    };
    assert_eq!(
        ack.status,
        AckStatus::Gap { expected: SEQ },
        "a delta whose causal predecessors are missing applied nothing; the ack must report the \
         gap at the seq Origin still expects, got {:?}",
        ack.status
    );

    // A Gap ack asserts the opposite of an apply, so nothing may have landed.
    let landed = read_crdt_doc(&cluster.nodes[0].client, Duration::from_secs(5))
        .await
        .expect("crdt_state query after gapped delta");
    assert!(
        landed.is_none(),
        "the gapped delta applied nothing, so the document must be absent, got: {landed:?}"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_high_water_mark_admits_the_same_seq_re_push() {
    let (cluster, mut client) = cluster_with_sync_client().await;
    let (gapped_update, full_history) = split_history(COLL, DOC);

    // First send: refused, nothing applied. Whatever frame shape Origin
    // currently answers with, the high-water-mark must not advance past SEQ —
    // that is the entire reason the retryable arm exists.
    let _refused = client
        .push_delta_at_seq(COLL, DOC, PEER_ID, 1, SEQ, gapped_update)
        .await
        .expect("push causally-gapped delta");

    // The edge re-sends at the same stored seq once it has the missing history.
    // If the high-water-mark had advanced, this comes back `Duplicate` and the
    // write is lost permanently.
    let outcome = client
        .push_delta_at_seq(COLL, DOC, PEER_ID, 1, SEQ, full_history)
        .await
        .expect("re-push at the same seq with the complete history");
    let ack = match outcome {
        DeltaOutcome::Ack(ack) => ack,
        DeltaOutcome::Reject(reject) => panic!(
            "the re-push carries complete history and must apply, got DeltaReject \
             (reason={:?}, compensation={:?})",
            reject.reason, reject.compensation
        ),
    };
    assert_eq!(
        ack.status,
        AckStatus::Applied,
        "the held high-water-mark must admit the same-seq re-push rather than deduplicate it away; \
         a Duplicate here means the refused delta advanced the mark and the write is lost, got {:?}",
        ack.status
    );

    let landed = read_crdt_doc(&cluster.nodes[0].client, Duration::from_secs(10))
        .await
        .expect("crdt_state query after the re-push");
    assert!(
        matches!(landed, Some(ref s) if !s.is_empty()),
        "the re-pushed delta was acked as applied, so the document must exist, got: {landed:?}"
    );

    cluster.shutdown().await;
}
