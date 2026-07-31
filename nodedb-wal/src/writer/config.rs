// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use crate::align::DEFAULT_ALIGNMENT;
use crate::double_write::DwbMode;

/// Default write buffer size: 2 MiB.
///
/// This is the batch size for group commit. Records accumulate here until
/// the buffer is full or `sync()` is called.
///
/// Matches `WalTuning::write_buffer_size` default. Override via
/// `WalWriterConfig::write_buffer_size` at construction time.
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 2 * 1024 * 1024;

/// Configuration for the WAL writer.
#[derive(Debug, Clone)]
pub struct WalWriterConfig {
    /// Size of the aligned write buffer (rounded up to alignment).
    pub write_buffer_size: usize,

    /// O_DIRECT alignment (typically 4096 for NVMe).
    pub alignment: usize,

    /// Whether to use O_DIRECT. Set to `false` for testing on filesystems
    /// that don't support it (e.g., tmpfs).
    pub use_direct_io: bool,

    /// Double-write buffer I/O mode. `None` means "mirror the parent" —
    /// `Direct` when `use_direct_io` is true, `Buffered` otherwise.
    /// `Some(DwbMode::Off)` disables the DWB entirely.
    pub dwb_mode: Option<DwbMode>,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            alignment: DEFAULT_ALIGNMENT,
            use_direct_io: true,
            dwb_mode: None,
        }
    }
}

pub(crate) fn resolve_dwb_mode(config: &WalWriterConfig) -> DwbMode {
    config
        .dwb_mode
        .unwrap_or_else(|| DwbMode::default_for_parent(config.use_direct_io))
}

pub(crate) fn open_dwb_for(
    config: &WalWriterConfig,
    path: &Path,
) -> Option<crate::double_write::DoubleWriteBuffer> {
    let mode = resolve_dwb_mode(config);
    if mode == DwbMode::Off {
        return None;
    }
    let dwb_path = path.with_extension("dwb");
    match crate::double_write::DoubleWriteBuffer::open(&dwb_path, mode) {
        Ok(d) => Some(d),
        Err(e) => {
            tracing::warn!(
                path = %dwb_path.display(),
                error = %e,
                mode = ?mode,
                "failed to open DWB — torn-write protection disabled for this writer"
            );
            None
        }
    }
}

/// Where a reopened segment resumes writing.
///
/// Every completed flush ends on an alignment boundary (the batch is padded
/// with a framed `Noop` record), so an unaligned recovery offset means the
/// final batch was torn by a crash mid-write. O_DIRECT cannot write at an
/// unaligned offset, and the torn block was never fsynced — so it was never
/// acknowledged to any client — so the block is rewritten from its boundary.
pub fn resume_offset(end_offset: u64, use_direct_io: bool, alignment: usize, path: &Path) -> u64 {
    if !use_direct_io {
        return end_offset;
    }
    let alignment = alignment as u64;
    let aligned = end_offset - (end_offset % alignment);
    if aligned != end_offset {
        tracing::warn!(
            path = %path.display(),
            end_offset,
            resume_offset = aligned,
            "WAL segment ends mid-block — discarding the torn final block (never fsynced, never acknowledged)"
        );
    }
    aligned
}
