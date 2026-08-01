// SPDX-License-Identifier: BUSL-1.1
//! Two replicas must not be able to share one Loro peer id.
//!
//! ## What this guards
//!
//! A Loro peer id is the identity every CRDT operation is attributed to. Two
//! replicas that claim the same one allocate overlapping `(peer, counter)`
//! ranges for *different* writes, and the merge resolves that the only way it
//! can: it trims whichever operations the document already covers and reports a
//! successful import. That trim is correct — it is exactly how an idempotent
//! resync works — and it is indistinguishable, at the `(peer, counter)` level,
//! from the second replica's writes being thrown away.
//!
//! The observable consequence is the worst shape a database can have: the
//! client is acked, the session closes with `rejected=0`, and the rows are
//! simply not there. No log line at any level records it, because from the
//! merge's point of view nothing went wrong.
//!
//! So the refusal has to happen before the merge, at the one layer where the
//! two cases *are* distinguishable: the server knows which durable producer
//! each session belongs to, so it can hold a peer id to its first owner and
//! answer the second with a rejection the client can act on.
//!
//! ## What these tests pin
//!
//! * A second producer writing under an owned peer id is refused terminally,
//!   and its row is genuinely absent afterwards — the refusal replaces the
//!   silent loss rather than accompanying it.
//! * The first owner keeps writing, including after a reconnect: a binding that
//!   refused its own owner would be worse than none.
//! * The same peer id in a different collection is not refused. Collections are
//!   separate documents whose counter ranges never meet, and every client that
//!   derives one peer id per collection from a single base depends on that.

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use std::sync::Arc;
use std::sync::atomic::Ordering;

use nodedb::control::server::sync::listener::{
    SyncListenerConfig, SyncListenerState, start_sync_listener,
};
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::sync::wire::AckStatus;

const COLL: &str = "notes";
const OTHER_COLL: &str = "memos";
/// The single peer id both replicas claim. Its value does not matter; that they
/// share it does.
const SHARED_PEER: u64 = 1;

/// A self-contained Loro snapshot writing one row, authored under `peer_id`.
///
/// Both replicas here build their blobs the same way from a fresh document, so
/// their counters both start at zero — the shape a fresh install produces, and
/// the one that makes the ranges overlap exactly.
fn row_snapshot(peer_id: u64, collection: &str, row_id: &str, body: &str) -> Vec<u8> {
    let doc = loro::LoroDoc::new();
    doc.set_peer_id(peer_id).expect("set peer id");
    let coll = doc.get_map(collection);
    let row = coll
        .insert_container(row_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("body", body).expect("field write");
    doc.commit();
    doc.export(loro::ExportMode::Snapshot)
        .expect("export snapshot")
}

/// Append one row to `doc` and export its complete history.
///
/// Reusing one document is what makes the counters advance instead of
/// restarting, which is the whole difference between a reconnecting client and
/// a colliding one.
fn append_row(doc: &loro::LoroDoc, row_id: &str, body: &str) -> Vec<u8> {
    let coll = doc.get_map(COLL);
    let row = coll
        .insert_container(row_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("body", body).expect("field write");
    doc.commit();
    doc.export(loro::ExportMode::Snapshot)
        .expect("export snapshot")
}

/// Whether `crdt_state(collection, row)` finds a document.
///
/// The CRDT read path answers a missing document with `NotFound`, a
/// deterministic terminal outcome; any other error is treated as catch-up lag
/// and retried until `timeout`.
async fn row_is_readable(
    client: &tokio_postgres::Client,
    collection: &str,
    row: &str,
    timeout: Duration,
) -> Result<bool, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT crdt_state('{collection}', '{row}')"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        return Ok(!r.get(0).unwrap_or("").is_empty());
                    }
                }
                return Ok(false);
            }
            Err(e)
                if e.as_db_error()
                    .is_some_and(|d| d.message().contains("NotFound")) =>
            {
                return Ok(false);
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

/// A three-node cluster with both collections created and node 0's sync
/// listener running.
///
/// The listener state is returned alongside the cluster because it carries the
/// delta accounting the close line reports; a test that only had the address
/// could assert what a client was told but not what the server counted.
async fn cluster_with_sync_listener() -> (TestCluster, Arc<SyncListenerState>) {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");
    for collection in [COLL, OTHER_COLL] {
        cluster
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION {collection} WITH (engine='document_schemaless')"
            ))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {collection}: {e}"));
    }

    let cfg = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) = nodedb::control::shutdown::ShutdownBus::new(Arc::new(
        nodedb::control::shutdown::ShutdownWatch::new(),
    ));
    let state = start_sync_listener(
        cfg,
        Some(Arc::clone(&cluster.nodes[0].shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener");
    (cluster, state)
}

fn expect_applied(outcome: DeltaOutcome, what: &str) {
    match outcome {
        DeltaOutcome::Ack(ack) => assert_eq!(
            ack.status,
            AckStatus::Applied,
            "{what} must apply, got {:?}",
            ack.status
        ),
        DeltaOutcome::Reject(reject) => panic!(
            "{what} must apply, got DeltaReject (reason={:?}, compensation={:?})",
            reject.reason, reject.compensation
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_replica_claiming_an_owned_peer_id_is_refused_not_absorbed() {
    let (cluster, listener) = cluster_with_sync_listener().await;
    let addr = listener.config.listen_addr;

    let mut first = SyncTestClient::connect_as_lite(addr, "replica-a", 1)
        .await
        .expect("first replica handshake");
    let mut second = SyncTestClient::connect_as_lite(addr, "replica-b", 1)
        .await
        .expect("second replica handshake");
    assert_ne!(
        first.producer_id(),
        second.producer_id(),
        "two distinct lite ids must register distinct producers, or this test \
         proves nothing about collisions between them"
    );

    expect_applied(
        first
            .push_delta(
                COLL,
                "from-a",
                SHARED_PEER,
                1,
                row_snapshot(SHARED_PEER, COLL, "from-a", "a"),
            )
            .await
            .expect("first replica push"),
        "the first replica's write",
    );

    // The second replica's blob covers the same (peer, counter) range with
    // different content. Without the binding the merge trims it away and this
    // comes back as a success.
    let outcome = second
        .push_delta(
            COLL,
            "from-b",
            SHARED_PEER,
            1,
            row_snapshot(SHARED_PEER, COLL, "from-b", "b"),
        )
        .await
        .expect("second replica push");

    let reject = match outcome {
        DeltaOutcome::Reject(reject) => reject,
        DeltaOutcome::Ack(ack) => panic!(
            "a delta written under another producer's peer id cannot land — the merge discards \
             it — so acking it ({:?}) retires a write that no longer exists anywhere",
            ack.status
        ),
    };
    assert!(
        reject.reason.contains("PEER_ID_COLLISION"),
        "the refusal must name the collision so the client can regenerate its peer id \
         rather than retry forever, got: {}",
        reject.reason
    );

    assert!(
        !row_is_readable(
            &cluster.nodes[0].client,
            COLL,
            "from-b",
            Duration::from_secs(5)
        )
        .await
        .expect("read the refused row"),
        "the refused row must be absent; a rejection alongside a landed row would mean \
         the refusal is decided in the wrong place"
    );
    assert!(
        row_is_readable(
            &cluster.nodes[0].client,
            COLL,
            "from-a",
            Duration::from_secs(10)
        )
        .await
        .expect("read the first replica's row"),
        "the owner's write must be unaffected by the refusal of another replica's"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_owning_replica_keeps_its_peer_id_across_a_reconnect() {
    // A binding that outlived the session but not its owner would refuse every
    // client that ever reconnects — worse than the collision it prevents.
    //
    // The client keeps ONE document across the reconnect and appends to it, and
    // resumes its producer stream at the next sequence number. Both are what a
    // real edge persists: a rebuilt document would restart its Loro counters at
    // zero, and a restarted sequence is deduplicated by the producer gate before
    // the apply is ever reached — neither says anything about peer-id ownership,
    // which is what this test is for.
    let (cluster, listener) = cluster_with_sync_listener().await;
    let addr = listener.config.listen_addr;
    let doc = loro::LoroDoc::new();
    doc.set_peer_id(SHARED_PEER).expect("set peer id");

    let mut client = SyncTestClient::connect_as_lite(addr, "replica-a", 1)
        .await
        .expect("first handshake");
    expect_applied(
        client
            .push_delta(
                COLL,
                "first",
                SHARED_PEER,
                1,
                append_row(&doc, "first", "1"),
            )
            .await
            .expect("push before reconnect"),
        "the owner's first write",
    );
    drop(client);

    let mut reconnected = SyncTestClient::connect_as_lite(addr, "replica-a", 1)
        .await
        .expect("reconnect handshake");
    expect_applied(
        reconnected
            .push_delta_at_seq(
                COLL,
                "second",
                SHARED_PEER,
                2,
                2,
                append_row(&doc, "second", "2"),
            )
            .await
            .expect("push after reconnect"),
        "the owner's write after reconnecting",
    );
    assert!(
        row_is_readable(
            &cluster.nodes[0].client,
            COLL,
            "second",
            Duration::from_secs(10)
        )
        .await
        .expect("read the post-reconnect row"),
        "the post-reconnect write was acked as applied, so its row must be readable"
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_peer_id_in_another_collection_is_not_a_collision() {
    // Each collection is its own document, so identical peer ids in two of them
    // never share a counter range. Refusing here would break every client that
    // derives one peer id per collection from a single base.
    let (cluster, listener) = cluster_with_sync_listener().await;
    let addr = listener.config.listen_addr;

    let mut first = SyncTestClient::connect_as_lite(addr, "replica-a", 1)
        .await
        .expect("first replica handshake");
    let mut second = SyncTestClient::connect_as_lite(addr, "replica-b", 1)
        .await
        .expect("second replica handshake");

    expect_applied(
        first
            .push_delta(
                COLL,
                "from-a",
                SHARED_PEER,
                1,
                row_snapshot(SHARED_PEER, COLL, "from-a", "a"),
            )
            .await
            .expect("first replica push"),
        "the first replica's write",
    );
    expect_applied(
        second
            .push_delta(
                OTHER_COLL,
                "from-b",
                SHARED_PEER,
                1,
                row_snapshot(SHARED_PEER, OTHER_COLL, "from-b", "b"),
            )
            .await
            .expect("second replica push into another collection"),
        "a write under the same peer id in a different collection",
    );

    assert!(
        row_is_readable(
            &cluster.nodes[0].client,
            OTHER_COLL,
            "from-b",
            Duration::from_secs(10)
        )
        .await
        .expect("read the second collection's row"),
        "the write was acked as applied, so its row must be readable"
    );

    cluster.shutdown().await;
}

/// The residual case the peer-id binding cannot refuse, pinned so it stays
/// visible rather than becoming folklore.
///
/// A client that reinstalls — wiping its CRDT store but keeping its `lite_id`
/// and its peer id — restarts its counters at zero and collides with its *own*
/// earlier writes. The binding has nothing to object to: the producer really
/// does own that peer id. What must not happen is the apply reporting `Applied`
/// for a delta the merge discarded, because that is the shape that retires a
/// write which exists nowhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reinstalled_replica_reusing_its_own_peer_id_is_not_reported_as_applied() {
    let (cluster, listener) = cluster_with_sync_listener().await;
    let addr = listener.config.listen_addr;

    let mut client = SyncTestClient::connect_as_lite(addr, "replica-a", 1)
        .await
        .expect("handshake");
    expect_applied(
        client
            .push_delta(
                COLL,
                "before",
                SHARED_PEER,
                1,
                row_snapshot(SHARED_PEER, COLL, "before", "1"),
            )
            .await
            .expect("push before the reinstall"),
        "the write made before the reinstall",
    );

    // A fresh document under the same peer id: the counter range the server
    // already holds, carrying entirely different operations.
    let outcome = client
        .push_delta(
            COLL,
            "after",
            SHARED_PEER,
            2,
            row_snapshot(SHARED_PEER, COLL, "after", "2"),
        )
        .await
        .expect("push after the reinstall");

    match outcome {
        DeltaOutcome::Ack(ack) => assert_ne!(
            ack.status,
            AckStatus::Applied,
            "the merge discarded this delta as already-known, so reporting it applied \
             tells the client a write landed that exists nowhere"
        ),
        DeltaOutcome::Reject(_) => {}
    }
    assert!(
        !row_is_readable(
            &cluster.nodes[0].client,
            COLL,
            "after",
            Duration::from_secs(5)
        )
        .await
        .expect("read the discarded row"),
        "this test only means something while the row is genuinely absent"
    );

    cluster.shutdown().await;
}

/// The trim counter must carry a real, non-zero measurement all the way from
/// the CRDT merge to the session's close accounting.
///
/// Every hop between the two — admission preview, dispatch outcome, delta
/// outcome, session — copies a `u64`. A break anywhere in that chain still
/// compiles and still logs a counter; it just always logs zero. That is
/// indistinguishable from a healthy server, which is the exact failure this
/// counter exists to make visible, so asserting the plumbing is not enough:
/// the value itself has to be observed downstream of a delta that really was
/// trimmed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trimmed_delta_is_counted_in_the_listener_totals() {
    let (cluster, listener) = cluster_with_sync_listener().await;
    let addr = listener.config.listen_addr;

    assert_eq!(
        listener.ops_trimmed.load(Ordering::Relaxed),
        0,
        "nothing has been synced yet"
    );

    {
        let mut client = SyncTestClient::connect_as_lite(addr, "replica-a", 1)
            .await
            .expect("handshake");
        expect_applied(
            client
                .push_delta(
                    COLL,
                    "first",
                    SHARED_PEER,
                    1,
                    row_snapshot(SHARED_PEER, COLL, "first", "1"),
                )
                .await
                .expect("first push"),
            "the first write",
        );
        // A fresh document under the same peer id: its whole counter range is
        // already known, so the merge trims every operation it carries.
        let outcome = client
            .push_delta(
                COLL,
                "second",
                SHARED_PEER,
                2,
                row_snapshot(SHARED_PEER, COLL, "second", "2"),
            )
            .await
            .expect("fully-trimmed push");
        match outcome {
            DeltaOutcome::Ack(ack) => assert_ne!(ack.status, AckStatus::Applied),
            DeltaOutcome::Reject(_) => {}
        }
    }

    // The totals are folded when the session closes, so wait for the loop to
    // notice the dropped socket rather than assuming it already has.
    let deadline = Instant::now() + Duration::from_secs(10);
    while listener.ops_trimmed.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        listener.ops_trimmed.load(Ordering::Relaxed) > 0,
        "a delta whose every operation was already known reached the merge, so the trim \
         count must be non-zero; a zero here means the measurement is lost somewhere \
         between the merge and the session and the counter can never report a collision"
    );
    assert!(
        listener.deltas_deduplicated.load(Ordering::Relaxed) > 0,
        "the same delta applied nothing, so it must be counted as deduplicated"
    );

    cluster.shutdown().await;
}
