// SPDX-License-Identifier: Apache-2.0

//! The WAL's write path on `wasm32-wasip1`, the target the wasm test job runs.
//!
//! `WalWriter::flush_buffer` wrote the batch with a `#[cfg(unix)]` arm and
//! nothing else. `cfg(unix)` is false on this target — `target_family` is `wasm`
//! — so the write was not compiled at all, while the statements after it still
//! ran: the offset advanced, the buffer was cleared and the flush reported
//! success. The test below is the difference between "the append landed" and
//! "the writer said it landed", and it is decided by the file, not by the
//! return value.
//!
//! `fsync_directory` had the same shape of problem: it opened the directory and
//! called `sync_all`, which wasi preview1 cannot do, so every caller that
//! fsyncs a directory after a rename failed on this target.
//!
//! Notes for whoever runs this:
//!
//! - Run it with `--nocapture`. A panic aborts the process on this target, so
//!   without it a failed assertion arrives as a wasm trap and the message that
//!   says which byte count disagreed is lost.
//! - `wasmtime --dir=.` preopens the working directory (Cargo runs the test
//!   binary from the package root), and `tempfile`'s default base is
//!   `std::env::temp_dir()`, which is `unimplemented!()` in std for wasi. A
//!   directory under the preopen is the only place a wasm test can write.
//! - A panicking test skips its own cleanup, so an aborted run leaves the
//!   tempdir behind next to the package.
//! - The failure-path test needs the crate's own failpoints, so it runs with
//!   `--features failpoints`. The other two run either way.
//!
//! wasm-only on purpose: the native append path is covered by `tests/wal_suite`.

#![cfg(target_arch = "wasm32")]

use nodedb_wal::WalWriter;
use nodedb_wal::segment::atomic_io::{atomic_write_fsync, fsync_directory};
use std::fs;

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .tempdir_in(".")
        .expect("the wasm test runner preopens the working directory")
}

const PAYLOAD: &[u8] = b"wasip1 append probe";

#[test]
fn an_appended_record_reaches_the_file() {
    let dir = tempdir();
    let path = dir.path().join("append.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).expect("open the segment");
    writer.append(1, 0, 0, 0, PAYLOAD).expect("append");
    writer.sync().expect("sync");

    let counted = writer.file_offset();
    let written = fs::read(&path).expect("read the segment back");

    assert_eq!(
        written.len() as u64,
        counted,
        "sync() reported success with {counted} bytes written, but the segment holds {} bytes",
        written.len()
    );
    assert!(
        written.windows(PAYLOAD.len()).any(|w| w == PAYLOAD),
        "the segment does not contain the appended payload"
    );
}

#[test]
fn a_checkpoint_can_fsync_its_directory() {
    let dir = tempdir();

    fsync_directory(dir.path()).expect("wasi preview1 has no directory fsync, not a failure");
    atomic_write_fsync(dir.path(), "payload.ckpt", b"hello wasi")
        .expect("atomic_write_fsync renames then fsyncs the directory");
    assert_eq!(
        fs::read(dir.path().join("payload.ckpt")).expect("read back"),
        b"hello wasi",
        "the checkpoint bytes did not survive the round trip"
    );
}

/// A failed write is reported as a failure and advances nothing, so the batch is
/// retried byte-for-byte at the same offset.
///
/// The injection is `wal::wasm_flush_write`, which sits *inside* the wasi write
/// arm: with the arm removed the injection cannot fire and this test fails, so
/// it is a chokepoint for the arm rather than for the wrapper above it. The
/// error it produces goes through the same `classify_write_error` call the real
/// `write_all` failure does, which is the only way to reach that classifier on a
/// runtime whose writes cannot be made to fail on demand. It carries the error
/// kind a full device produces, so what the assertion pins is the classifier's
/// rule — not the runtime's errno translation, which std owns and which a real
/// full device would exercise.
///
/// Needs the feature, and is compiled out without it rather than weakened:
/// without `failpoints` the injection expands to nothing and these assertions
/// would describe a successful flush.
///
/// ```text
/// CARGO_TARGET_WASM32_WASIP1_RUNNER="wasmtime --dir=." \
///   cargo test -p nodedb-wal --target wasm32-wasip1 --features failpoints \
///   --test wasi_append -- --nocapture
/// ```
#[cfg(feature = "failpoints")]
#[test]
fn a_failed_flush_reports_failure_and_advances_nothing() {
    use nodedb_wal::error::WalError;

    let dir = tempdir();
    let path = dir.path().join("failed.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).expect("open the segment");
    writer.append(1, 0, 0, 0, PAYLOAD).expect("append");
    let before = writer.file_offset();

    let guard = nodedb_types::fail_point::FailGuard::fail("wal::wasm_flush_write", "device full");
    let result = writer.sync();
    drop(guard);

    let err = result.expect_err("an armed write failpoint must not be reported as a flush");
    assert!(
        matches!(err, WalError::OutOfSpace { .. }),
        "a full device must be classified as OutOfSpace on this target too, got {err:?}"
    );
    assert_eq!(
        writer.file_offset(),
        before,
        "a failed flush advanced the offset, so a retry would skip the batch"
    );
    assert_eq!(
        fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
        0,
        "a failed flush left bytes in the segment"
    );
}
