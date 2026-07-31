// SPDX-License-Identifier: BUSL-1.1

//! The WAL's production `O_DIRECT` default, end to end through the real
//! binary.
//!
//! Direct I/O is the WAL's durability contract: writes bypass the page cache,
//! so an acknowledged record does not depend on kernel writeback and WAL
//! traffic cannot evict the mmapped indexes it shares a machine with. Two
//! properties have to hold for that default to mean anything:
//!
//!   1. A server started with no WAL overrides at all boots on a normal
//!      direct-I/O-capable filesystem, and a write it acknowledges survives a
//!      hard crash and comes back after replay.
//!   2. On a filesystem that cannot do `O_DIRECT`, the server fails to start
//!      with a message naming the directory and both ways out — it never
//!      quietly reopens the WAL buffered, because that would retire the
//!      guarantee above with nothing to observe.
//!
//! The second property is driven by a fail point rather than by hunting for an
//! incapable mount: `O_DIRECT` works on tmpfs from Linux 6.1 onward, so a test
//! that waits for the local filesystem to refuse the flag reports success
//! without ever running the code it names.

#![cfg(target_os = "linux")]

mod crash_harness;

use std::path::PathBuf;
use std::time::Duration;

use crash_harness::{CrashHarness, direct_io_supported};

/// The build's own scratch directory, which lives beside `target/` rather than
/// under `TMPDIR` — on a normal checkout that is a real disk filesystem.
fn target_tmpdir() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
}

/// A server booted with no WAL configuration whatsoever runs on the shipped
/// `O_DIRECT` default, and an acknowledged write survives `kill -9` and comes
/// back after replay. Without this the default is only configured, not proven.
#[tokio::test(flavor = "multi_thread")]
async fn production_direct_io_default_boots_and_survives_restart() {
    let parent = target_tmpdir();
    if std::fs::create_dir_all(&parent).is_err() || !direct_io_supported(&parent) {
        eprintln!(
            "skipping: {} is on a filesystem without O_DIRECT support",
            parent.display()
        );
        return;
    }

    let mut h = CrashHarness::new_in(&parent).with_direct_io_wal();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION direct_io_default (id TEXT PRIMARY KEY, v INT) \
         WITH (engine='document_strict')",
    )
    .await;
    h.exec("INSERT INTO direct_io_default (id, v) VALUES ('a', 7)")
        .await;

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col("SELECT v FROM direct_io_default WHERE id = 'a'", "v")
        .await;
    assert_eq!(
        recovered,
        vec!["7".to_string()],
        "a write acknowledged by an O_DIRECT WAL did not survive kill -9 + replay \
         (got {recovered:?})"
    );
}

/// When the WAL's `O_DIRECT` open is refused, startup fails and says so —
/// naming the WAL directory and both remedies. A silent downgrade to buffered
/// I/O would be the worst outcome: the server would look healthy while the
/// durability property it advertises had been dropped.
///
/// The refusal is injected at the segment-open call site, so the test exercises
/// the real `WalError::DirectIoUnsupported` → startup-abort chain no matter
/// what the local filesystem is capable of. Requires `--features failpoints`.
#[cfg(feature = "failpoints")]
#[test]
fn unsupported_filesystem_fails_startup_with_both_remedies() {
    // `with_direct_io_wal` pins direct I/O on regardless of the probe: the
    // injection only fires on the direct-I/O open path, and a harness that
    // opted out would boot happily and prove nothing.
    let mut h = CrashHarness::new()
        .with_direct_io_wal()
        .with_env(
            "NODEDB_FAILPOINTS",
            "wal::direct_io_unsupported=fail(injected filesystem without O_DIRECT)",
        )
        // The failure is reported through the startup error path, which the
        // default `error` level already emits; boot context makes a mismatch
        // diagnosable rather than a bare missing-substring assertion.
        .with_env("RUST_LOG", "error,nodedb::bootstrap=info");
    h.spawn_expect_boot_failure(Duration::from_secs(20));

    let log = h.server_log();
    assert!(
        log.contains("O_DIRECT"),
        "startup failure did not mention O_DIRECT; the operator cannot tell \
         direct I/O was the problem.\nServer output:\n{log}"
    );
    assert!(
        log.contains(&h.data_dir().display().to_string()),
        "startup failure did not name the data directory.\nServer output:\n{log}"
    );
    assert!(
        log.contains("NODEDB_WAL_DIRECT_IO=false"),
        "startup failure did not offer the explicit opt-out.\nServer output:\n{log}"
    );
    assert!(
        log.contains("move the data directory"),
        "startup failure did not offer relocating the data directory.\nServer output:\n{log}"
    );
    assert!(
        !log.contains("falling back to buffered"),
        "the WAL must never downgrade itself to buffered I/O.\nServer output:\n{log}"
    );
}
