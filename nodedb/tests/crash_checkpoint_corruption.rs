// SPDX-License-Identifier: BUSL-1.1

//! Fail-stop-on-corrupt-checkpoint boot regression.
//!
//! A per-engine checkpoint is the only non-WAL home of the rows it holds once
//! the WAL below its LSN has been truncated. If boot silently skipped a corrupt
//! checkpoint and continued, those rows would be gone with no error — silent
//! data loss. The boot sequence must instead refuse to bring the core up.
//!
//! This plants a deliberately-corrupt VECTOR checkpoint in the data directory
//! before the server boots, then asserts the server does NOT come up: the
//! loader returns `Err`, which the data-core boot path turns into a panic that
//! drops the `replay_done` sender, which the cluster-ready gate treats as a
//! boot abort, so `main` returns `Err` and the process exits non-zero. This
//! test crosses the entire chain end-to-end.

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

/// A framed checkpoint (`NCKF` magic + version + CRC32C + len + payload) whose
/// stored CRC is deliberately wrong, so `read_checkpoint_framed` rejects it
/// with `WalError::CheckpointCorrupt` rather than a decode error one layer up.
/// Magic, version, and length are all valid so the frame passes every check
/// EXCEPT the integrity CRC — this exercises the real corruption path, not a
/// malformed-header shortcut.
fn corrupt_framed_checkpoint() -> Vec<u8> {
    let payload: &[u8] = b"this is not a valid vector checkpoint payload";
    let mut framed = Vec::new();
    framed.extend_from_slice(b"NCKF"); // magic
    framed.push(1u8); // frame version
    framed.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // deliberately wrong CRC32C
    framed.extend_from_slice(&(payload.len() as u64).to_le_bytes()); // correct payload_len
    framed.extend_from_slice(payload);
    framed
}

/// Planting a corrupt vector checkpoint must make the server fail-stop at boot
/// instead of silently skipping it and serving a truncated index.
#[test]
fn corrupt_vector_checkpoint_fails_boot() {
    let mut h = CrashHarness::new();

    // The harness spawns with NODEDB_DATA_PLANE_CORES=1, so the sole core is
    // core-0 and its vector checkpoint directory is
    // `{data_dir}/vector-ckpt/core-0/`. The MANIFEST there names the live
    // generation and is the only thing that makes any checkpoint reachable, so
    // corrupting it is corrupting the checkpoint: the loader cannot tell which
    // generation is durable, and the WAL below it may already be truncated.
    let ckpt_dir = h.data_dir().join("vector-ckpt").join("core-0");
    std::fs::create_dir_all(&ckpt_dir).expect("create vector checkpoint dir");
    std::fs::write(ckpt_dir.join("MANIFEST"), corrupt_framed_checkpoint())
        .expect("write corrupt vector checkpoint manifest");

    // Boot must fail-stop: loader Err -> panic in the data-core thread ->
    // dropped `replay_done` sender -> cluster-ready abort -> non-zero exit.
    // The server must never become `/healthz`-ready.
    h.spawn_expect_boot_failure(Duration::from_secs(30));
}
