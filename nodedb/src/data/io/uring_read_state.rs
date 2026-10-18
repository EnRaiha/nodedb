// SPDX-License-Identifier: BUSL-1.1

//! Per-read bookkeeping for the batched io_uring reader, plus the cleanup
//! and fallback paths it takes when its ring stalls.

#![cfg(target_os = "linux")]

use std::path::Path;

use super::aligned_buf::{ALIGNMENT, AlignedBuf, round_up};

/// Progress of one read through the submit/complete cycle.
#[derive(Clone, Copy)]
pub(super) enum ReadState {
    /// Never accepted by the submission queue — the kernel never saw the buffer.
    NotSubmitted,
    /// Accepted by the submission queue, completion not yet reaped.
    InFlight,
    /// Completed successfully.
    Done,
    /// Completed with a kernel error.
    Failed,
}

/// Tracks which buffer a read uses.
#[derive(Clone, Copy)]
pub(super) enum BufSource {
    Pool(usize),
    Oversized(usize),
    None,
}

/// A pending read operation.
pub(super) struct PendingRead {
    pub(super) index: usize,
    pub(super) file: Option<std::fs::File>,
    pub(super) size: usize,
    pub(super) buf_source: BufSource,
}

impl PendingRead {
    pub(super) fn failed(index: usize) -> Self {
        Self {
            index,
            file: None,
            size: 0,
            buf_source: BufSource::None,
        }
    }
}

/// Round a read size up to ALIGNMENT.
#[inline]
pub(super) fn round_up_read(size: usize) -> usize {
    round_up(size, ALIGNMENT)
}

/// Leak every buffer the kernel can still write after a batch stalled.
///
/// An in-flight pool slot gets a fresh buffer and returns to `free`.
/// Without a fresh buffer the slot stays withheld from later reads.
/// Oversized buffers of in-flight reads are leaked, the rest drop.
pub(super) fn leak_kernel_owned_buffers(
    reads: &[PendingRead],
    states: &[ReadState],
    pool: &mut [AlignedBuf],
    free: &mut Vec<usize>,
    buf_size: usize,
    oversized: Vec<AlignedBuf>,
) {
    let mut kernel_owned = vec![false; oversized.len()];
    for read in reads {
        if !matches!(states.get(read.index), Some(ReadState::InFlight)) {
            continue;
        }
        match read.buf_source {
            BufSource::Pool(slot) => {
                if let (Some(stale), Ok(fresh)) = (pool.get_mut(slot), AlignedBuf::new(buf_size)) {
                    std::mem::forget(std::mem::replace(stale, fresh));
                    free.push(slot);
                }
            }
            BufSource::Oversized(idx) => {
                if let Some(owned) = kernel_owned.get_mut(idx) {
                    *owned = true;
                }
            }
            BufSource::None => {}
        }
    }
    for (buf, owned) in oversized.into_iter().zip(kernel_owned) {
        if owned {
            std::mem::forget(buf);
        }
    }
}

/// Read each file with blocking `std::fs::read`.
///
/// Used only while the reader has no usable ring. An unreadable file
/// yields an empty `Vec`, matching the io_uring path.
pub(super) fn read_sequential(paths: &[&Path]) -> Vec<Vec<u8>> {
    paths
        .iter()
        .map(|path| std::fs::read(path).unwrap_or_default())
        .collect()
}
