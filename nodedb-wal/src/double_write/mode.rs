// SPDX-License-Identifier: Apache-2.0

//! I/O mode selection for the double-write buffer file.

/// I/O mode for the double-write buffer file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DwbMode {
    /// DWB disabled — no torn-write protection. `DoubleWriteBuffer::open`
    /// returns `Err(WalError::DwbOffNotOpenable)`.
    Off,
    /// Buffered I/O (page cache + `fsync`). Default when the parent WAL
    /// does not use `O_DIRECT`.
    Buffered,
    /// `O_DIRECT` I/O via an aligned buffer. The intended companion to an
    /// `O_DIRECT` WAL: keeps DWB bytes out of the page cache.
    Direct,
}

impl DwbMode {
    /// Choose the DWB mode that mirrors the parent writer's O_DIRECT setting
    /// when no explicit override is configured. With `O_DIRECT` on, the DWB
    /// should also be `O_DIRECT`, otherwise it undoes the cache-bypass.
    pub fn default_for_parent(parent_uses_direct_io: bool) -> Self {
        if parent_uses_direct_io {
            Self::Direct
        } else {
            Self::Buffered
        }
    }
}
