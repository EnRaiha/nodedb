// SPDX-License-Identifier: BUSL-1.1
//! Cross-node shuffle staging (E3b) end-to-end integration test.
//!
//! Exercises the receive side of the "receive-to-spill, then local grace join"
//! design (D1): node 0 streams a BUILD side and a PROBE side to node 1 over real
//! QUIC via `send_shuffle_push`, each as one `ShufflePushChunk` = a msgpack ARRAY
//! of multiple rows. Node 1's `ShufflePush` read-loop must EXPLODE each array
//! into one `[u32 LE len][row-bytes]` frame per row and append them to a LOCAL
//! per-`(shuffle_id, part, side)` scratch file, gated by the per-part build
//! barrier (the E3b receiver/inbox wiring).
//!
//! This asserts the staged frame-files contain exactly the original rows, in
//! order, for both sides — i.e. cross-node delivery + multi-row array explosion
//! into the staged-file format. End-to-end join correctness then follows BY
//! COMPOSITION with the node-local E3a tests in
//! `nodedb/src/data/executor/handlers/join/{shuffle_join,row_source}.rs`, which
//! prove that staged frame-files joined via `execute_shuffle_join` equal the
//! in-memory reference. (Building the consumer-side join dispatch is E4.)

mod common;
use common::cluster_harness::TestCluster;

use std::path::Path;
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

/// A msgpack map row `{ <fields> }` — the per-row byte shape the staged-file
/// reader (and the grace join) operate on.
fn row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert((*k).to_string(), v.clone());
    }
    nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).expect("encode row")
}

/// Wrap `rows` in a single msgpack ARRAY (`ShufflePushChunk` payload): an array
/// header followed by each row's raw msgpack bytes — exactly the flat row-array
/// shape the receiver's `explode_row_array` / `decode_flat_row_array` reads.
fn msgpack_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    let n = rows.len();
    if n < 16 {
        out.push(0x90 | n as u8);
    } else if n <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for r in rows {
        out.extend_from_slice(r);
    }
    out
}

/// Parse a staged `[u32 LE len][row-bytes]` frame file into per-row byte vectors
/// (the format the Data Plane's `FrameStreamReader` consumes).
fn read_frames(path: &Path) -> Vec<Vec<u8>> {
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

/// BUILD / PROBE fixtures: multiple rows per side (so the array genuinely
/// explodes into several frames), with a match, a non-match on each side, and a
/// duplicate build-side key.
fn fixtures() -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let build = vec![
        row(&[("k", serde_json::json!(1)), ("rv", serde_json::json!("r1"))]),
        row(&[
            ("k", serde_json::json!(1)),
            ("rv", serde_json::json!("r1b")),
        ]), // dup key
        row(&[("k", serde_json::json!(2)), ("rv", serde_json::json!("r2"))]),
        row(&[("k", serde_json::json!(9)), ("rv", serde_json::json!("r9"))]), // no probe match
    ];
    let probe = vec![
        row(&[("k", serde_json::json!(1)), ("lv", serde_json::json!("l1"))]),
        row(&[("k", serde_json::json!(2)), ("lv", serde_json::json!("l2"))]),
        row(&[("k", serde_json::json!(7)), ("lv", serde_json::json!("l7"))]), // no build match
    ];
    (build, probe)
}

/// Stream a multi-row BUILD array and a multi-row PROBE array from node 0 to
/// node 1; assert each side's staged frame-file contains exactly its rows, in
/// order — proving cross-node delivery + array-to-per-row-frame explosion into
/// the staged format the local grace join (E3a) reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_both_sides_stage_to_per_row_frames_across_nodes() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // Producer = node 0; receiver/consumer = node 1.
    let producer = &cluster.nodes[0];
    let receiver = &cluster.nodes[1];
    let target = receiver.node_id;

    let transport = producer
        .shared
        .cluster_transport
        .as_ref()
        .expect("producer node has a cluster transport")
        .clone();

    let (build, probe) = fixtures();
    let shuffle_id = 1u64;

    // BUILD side (side = 0): one chunk = a msgpack array of the build rows.
    let build_req = ShufflePushRequest {
        shuffle_id,
        part: 0,
        side: 0,
        num_parts: 1,
        producer_count: 1,
    };
    send_shuffle_push(&transport, target, build_req, vec![msgpack_array(&build)])
        .await
        .expect("push build side to node 1");

    // PROBE side (side = 1).
    let probe_req = ShufflePushRequest {
        shuffle_id,
        part: 0,
        side: 1,
        num_parts: 1,
        producer_count: 1,
    };
    send_shuffle_push(&transport, target, probe_req, vec![msgpack_array(&probe)])
        .await
        .expect("push probe side to node 1");

    // Await BOTH per-part build barriers on the receiver node's registry (the
    // barrier also triggers `finalize` of each staged file).
    let registry = receiver.shared.shuffle_registry.clone();
    let staged = wait_until(Duration::from_secs(10), || {
        let b = registry.get((shuffle_id, 0, 0));
        let p = registry.get((shuffle_id, 0, 1));
        matches!((&b, &p), (Some(b), Some(p)) if b.barrier_complete() && p.barrier_complete())
    })
    .await;
    assert!(
        staged,
        "both build and probe barriers must fire on node 1 within 10s"
    );

    let build_inbox = registry.get((shuffle_id, 0, 0)).expect("build inbox");
    let probe_inbox = registry.get((shuffle_id, 0, 1)).expect("probe inbox");
    assert!(build_inbox.take_error().is_none(), "clean build EOF");
    assert!(probe_inbox.take_error().is_none(), "clean probe EOF");

    // Each side's chunk array must have exploded into one frame per row, in
    // order, byte-identical to the rows the producer sent. The two sides stage
    // to DISTINCT files.
    let build_path = build_inbox.staged_path().to_path_buf();
    let probe_path = probe_inbox.staged_path().to_path_buf();
    assert_ne!(
        build_path, probe_path,
        "build/probe stage to distinct files"
    );

    assert_eq!(
        read_frames(&build_path),
        build,
        "build chunk array must explode into one staged frame per row, in order"
    );
    assert_eq!(
        read_frames(&probe_path),
        probe,
        "probe chunk array must explode into one staged frame per row, in order"
    );

    cluster.shutdown().await;
}
