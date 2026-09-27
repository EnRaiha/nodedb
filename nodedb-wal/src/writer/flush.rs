// SPDX-License-Identifier: Apache-2.0

use crate::error::Result;
// The unix `pwrite` arm and the wasm seek+write arm both construct an error
// value directly; elsewhere failures propagate as `Result` from calls that
// build their own.
#[cfg(any(unix, target_arch = "wasm32"))]
use crate::error::WalError;

use super::core::WalWriter;

// The write arms below cover unix and wasm32. A target in neither family would
// compile no arm at all and reinstate the silent success this file's wasi arm
// exists to prevent, so fail the build instead of writing nothing.
#[cfg(not(any(unix, target_arch = "wasm32")))]
compile_error!("nodedb-wal has no WAL write arm for this target family");

impl WalWriter {
    /// Flush the aligned buffer to the file.
    ///
    /// On failure the buffer and `file_offset` are left untouched, so the
    /// batch is retried byte-for-byte at the same offset. A partial write that
    /// then errors leaves a torn tail on disk, which recovery discards.
    pub(super) fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Fault injection: a full device. The buffer and `file_offset` must
        // survive untouched so the batch can be retried once space is freed.
        nodedb_types::fail_point_err!("wal::flush_out_of_space", |_: String| {
            const SITE: &str = "WAL segment append (failpoint)";
            let err = WalError::OutOfSpace { context: SITE };
            crate::diag::out_of_space(&err, SITE, self.file_offset, self.buffer.len() as u64);
            err
        });

        let data = if self.config.use_direct_io {
            // O_DIRECT requires aligned I/O size. Padding is framed as a
            // record first so the batch boundary stays replayable.
            crate::record::pad_buffer_to_alignment(
                &mut self.buffer,
                self.config.alignment,
                "write buffer has no room for its alignment padding record",
            )?;
            self.buffer.as_aligned_slice()
        } else {
            // Without O_DIRECT, write only the actual data.
            self.buffer.as_slice()
        };

        // Write at the exact offset. The unix path uses `pwrite` and retries
        // short writes. `cfg(unix)` is false on `wasm32-wasip1` — its
        // `target_family` is `wasm`, not `unix` — so wasi needs its own arm:
        // without one this function advanced `file_offset`, cleared the buffer
        // and reported the flush with nothing written at all.
        #[cfg(all(unix, not(target_arch = "wasm32")))]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let mut remaining = data;
            let mut write_offset = self.file_offset;
            while !remaining.is_empty() {
                let written = unsafe {
                    libc::pwrite(
                        fd,
                        remaining.as_ptr() as *const libc::c_void,
                        remaining.len(),
                        write_offset as libc::off_t,
                    )
                };
                if written < 0 {
                    return Err(classify_write_error(
                        std::io::Error::last_os_error(),
                        "WAL segment append",
                        write_offset,
                        remaining.len() as u64,
                    ));
                }
                let n = written as usize;
                if n == 0 {
                    // A zero-length write makes no progress; retrying would
                    // spin forever.
                    return Err(WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "WAL pwrite made no progress",
                    )));
                }
                remaining = &remaining[n..];
                write_offset += n as u64;
            }
        }

        // This arm covers every wasm32 target, including
        // `wasm32-unknown-unknown`, where libc defines no `pwrite` at all, and
        // std's wasi positional API (`std::os::wasi::fs`) is still unstable.
        // The segment is only ever appended to, so seeking to `file_offset` and
        // writing is equivalent to a positional write. An error returns before
        // the shared bookkeeping below, so a failed flush leaves the buffer and
        // `file_offset` untouched for a retry — the same property the `pwrite`
        // arm has.
        #[cfg(target_arch = "wasm32")]
        {
            use std::io::{Seek as _, SeekFrom, Write as _};
            self.file
                .seek(SeekFrom::Start(self.file_offset))
                .map_err(WalError::Io)?;
            // Crash injection: this arm's write fails with a full device. It
            // sits inside the arm, so a test that arms it proves the arm is what
            // ran — and it is classified exactly as the real failure below is,
            // by the same call, which is the only way to reach that classifier
            // on a runtime whose writes cannot be made to fail on demand.
            nodedb_types::fail_point_err!("wal::wasm_flush_write", |_detail: String| {
                classify_write_error(
                    std::io::Error::new(std::io::ErrorKind::StorageFull, "device full"),
                    "WAL segment append",
                    self.file_offset,
                    data.len() as u64,
                )
            });
            self.file.write_all(data).map_err(|err| {
                classify_write_error(
                    err,
                    "WAL segment append",
                    self.file_offset,
                    data.len() as u64,
                )
            })?;
        }

        self.file_offset += data.len() as u64;
        self.buffer.clear();

        // The bytes are in the file but only in the page cache. Recorded
        // before the buffer's contents are forgotten so a later `sync` still
        // knows an fsync is owed.
        self.durability.record_flush();
        Ok(())
    }
}

/// Classify a failed WAL write.
///
/// A full device is called out separately from generic I/O failure: it is not
/// transient, retrying cannot succeed, and the caller must stop acknowledging
/// writes rather than treat it as a passing error. `offset` and `pending` say
/// where the batch stalled and how much of it never reached the file, which is
/// what a report needs to describe the write that could not complete.
///
/// Takes the error rather than reading `errno` again: the wasm arm is handed a
/// real `io::Error` by `write_all`, and re-deriving it from the thread's errno
/// would classify whatever happened to be there last.
///
/// Keyed on `ErrorKind::StorageFull` rather than on `libc::ENOSPC`: std maps the
/// full-device errno to that kind on every target that has a filesystem, and
/// `libc` defines no constants at all for `wasm32-unknown-unknown`, which this
/// crate is also compiled for.
#[cfg(any(unix, target_arch = "wasm32"))]
fn classify_write_error(
    err: std::io::Error,
    context: &'static str,
    offset: u64,
    pending: u64,
) -> WalError {
    if err.kind() == std::io::ErrorKind::StorageFull {
        let out_of_space = WalError::OutOfSpace { context };
        crate::diag::out_of_space(&out_of_space, context, offset, pending);
        return out_of_space;
    }
    let _ = (context, offset, pending);
    WalError::Io(err)
}
