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
    // Each chunk is a 1-element msgpack array (`0x91` + one positive fixint), so
    // each explodes into exactly one staged frame holding that element's bytes.
    let batches = vec![vec![0x91, 0x01], vec![0x91, 0x02], vec![0x91, 0x03]];
    // The rows the receiver stages: each chunk array's single element.
    let expected_rows = vec![vec![0x01u8], vec![0x02u8], vec![0x03u8]];

    send_shuffle_push(&transport, target, req, batches)
        .await
        .expect("send_shuffle_push to node 1");

    // The read-loop runs on the receiver's transport task; poll its registry for
    // the barrier (which also triggers `finalize` of the staged file).
    let registry = receiver.shared.shuffle_registry.clone();
    let arrived = wait_until(Duration::from_secs(10), || {
        registry
            .get((1, 0, 0))
            .map(|ib| ib.barrier_complete())
            .unwrap_or(false)
    })
    .await;
    assert!(
        arrived,
        "node 1 inbox for (1,0,0) did not reach the build barrier within 10s"
    );

    let inbox = registry.get((1, 0, 0)).expect("inbox exists");
    assert_eq!(inbox.producer_count(), 1);
    assert!(inbox.barrier_complete(), "build barrier must be complete");
    assert_eq!(inbox.ends_received(), 1);
    // The staged frame-file holds one frame per exploded row, in arrival order.
    let staged = read_frames(inbox.staged_path());
    assert_eq!(
        staged, expected_rows,
        "staged frames must match the exploded chunk rows in order"
    );
    // Clean EOF: no terminal error captured.
    assert!(inbox.take_error().is_none());

    cluster.shutdown().await;
}

/// Parse a staged `[u32 LE len][row-bytes]` frame file into per-row byte vectors
/// (the format the Data Plane's `FrameStreamReader` consumes).
fn read_frames(path: &std::path::Path) -> Vec<Vec<u8>> {
    let bytes = std::fs::read(path).expect("read staged frame file");
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("len")) as usize;
        pos += 4;
        assert!(pos + len <= bytes.len(), "frame body truncated");
        out.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    assert_eq!(pos, bytes.len(), "trailing bytes after last frame");
    out
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
    let inbox = registry.get_or_create(42, 0, 0, 1);
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
