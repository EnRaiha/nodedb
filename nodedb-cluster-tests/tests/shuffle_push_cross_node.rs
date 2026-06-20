// SPDX-License-Identifier: BUSL-1.1
//! Cross-node streaming-shuffle push (E1) integration test.
//!
//! Brings up a live cluster and drives the producer-side `send_shuffle_push`
//! helper from one node to another over real QUIC. Asserts that the target
//! node's `ShufflePush` transport read-loop deposited every chunk into the
//! per-`(shuffle_id, part, side)` inbox on its `SharedState.shuffle_registry`
//! and that the per-part build barrier fired once the `End` frame arrived.

mod common;
use common::cluster_harness::TestCluster;

use std::time::Duration;

use nodedb::control::server::shuffle::send_shuffle_push;
use nodedb_cluster::ShufflePushRequest;

/// Poll `cond` until it returns true or the deadline elapses.
async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Happy path: 3 payloads pushed from node A → node B for
/// `(shuffle_id=1, part=0, side=0/build)` with `producer_count=1`. The target's
/// inbox receives all 3 in FIFO order and `barrier_complete()` becomes true
/// after the single producer's `End`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_push_delivers_chunks_and_fires_barrier() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Producer = node 0; receiver = node 1.
    let producer = &cluster.nodes[0];
    let receiver = &cluster.nodes[1];
    let target = receiver.node_id;

    let transport = producer
        .shared
        .cluster_transport
        .as_ref()
        .expect("producer node has a cluster transport")
        .clone();

    let req = ShufflePushRequest {
        shuffle_id: 1,
        part: 0,
        side: 0, // build
        num_parts: 1,
        producer_count: 1,
    };
    let batches = vec![vec![0x91, 0x01], vec![0x91, 0x02], vec![0x91, 0x03]];

    send_shuffle_push(&transport, target, req, batches.clone())
        .await
        .expect("send_shuffle_push to node 1");

    // The read-loop runs on the receiver's transport task; poll its registry.
    let registry = receiver.shared.shuffle_registry.clone();
    let arrived = wait_until(Duration::from_secs(10), || {
        registry
            .get((1, 0, 0))
            .map(|ib| ib.barrier_complete() && ib.buffered_len() == 3)
            .unwrap_or(false)
    })
    .await;
    assert!(
        arrived,
        "node 1 inbox for (1,0,0) did not receive 3 chunks + barrier within 10s"
    );

    let inbox = registry.get((1, 0, 0)).expect("inbox exists");
    assert_eq!(inbox.producer_count(), 1);
    assert!(inbox.barrier_complete(), "build barrier must be complete");
    assert_eq!(inbox.ends_received(), 1);
    // FIFO drain matches what was sent.
    let drained = inbox.try_drain();
    assert_eq!(drained, batches, "chunks must arrive in FIFO order");
    // Clean EOF: no terminal error captured.
    assert!(inbox.take_error().is_none());

    cluster.shutdown().await;
}

/// Terminal-error path: a producer that ends with `Some(error)` causes the
/// receiver inbox's `take_error()` to be `Some`. The E1 wire helper only sends
/// clean EOF, so this case is driven directly against the live receiver node's
/// registry + inbox (the same `SharedState.shuffle_registry` the transport
/// read-loop feeds), exercising the barrier + error-capture semantics that the
/// `ShufflePushEnd { error: Some(..) }` frame triggers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_push_end_with_error_is_captured() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    let receiver = &cluster.nodes[1];
    let registry = receiver.shared.shuffle_registry.clone();

    // Opening frame creates the inbox (single producer).
    let inbox = registry.get_or_create(42, 0, 0, 1, 16);
    assert!(!inbox.barrier_complete());

    // Producer ends with a terminal error → captured + barrier advances.
    inbox.set_error(nodedb_cluster::TypedClusterError::Internal {
        code: 7,
        message: "producer aborted mid-shuffle".into(),
    });
    assert!(
        inbox.record_end(),
        "single producer End must complete the barrier"
    );

    let inbox = registry.get((42, 0, 0)).expect("inbox exists");
    assert!(inbox.barrier_complete());
    match inbox.take_error() {
        Some(nodedb_cluster::TypedClusterError::Internal { code, message }) => {
            assert_eq!(code, 7);
            assert!(message.contains("aborted"));
        }
        other => panic!("expected captured Internal error, got {other:?}"),
    }

    cluster.shutdown().await;
}
