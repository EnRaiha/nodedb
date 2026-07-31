// SPDX-License-Identifier: Apache-2.0

//! Double-write buffer for torn write protection.
//!
//! NVMe drives guarantee atomic 4 KiB sector writes but NOT atomic writes
//! for larger pages (e.g., 16 KiB). If power fails mid-write on a 16 KiB
//! page, the WAL page can be partially written (torn).
//!
//! CRC32C detects torn writes during replay, but without the double-write
//! buffer, the record is lost — even though it was acknowledged to the client.
//!
//! The double-write buffer solves this:
//! 1. Before writing to WAL, write the record to the double-write file.
//! 2. `fsync` the double-write file.
//! 3. Write to the WAL file.
//! 4. `fsync` the WAL file.
//!
//! On recovery, if a WAL record's CRC fails:
//! - Check the double-write buffer for an intact copy (verify CRC).
//! - If found, use the double-write copy to reconstruct the WAL page.
//! - If not found, the record is truly lost (pre-fsync crash).
//!
//! The double-write file is a fixed-size circular buffer. Only the most
//! recent N records are kept — older ones are overwritten. This is fine
//! because torn writes can only happen on the most recent write.
//!
//! ## Why the WAL alone is not enough
//!
//! Losing the DWB is not a cosmetic downgrade. A CRC failure with no DWB copy
//! is indistinguishable, to [`crate::reader`] and [`crate::torn_tail`], from
//! the end of the committed prefix: nothing higher-LSN follows the last record
//! in a segment, so a torn final record is classified as an unfsynced tail and
//! dropped without a word. Every degradation of the DWB therefore has to be
//! observable — see [`status`] and [`metrics`].
//!
//! ## O_DIRECT mode
//!
//! When the parent WAL uses `O_DIRECT`, the DWB can also be opened with
//! `O_DIRECT` (`DwbMode::Direct`). This:
//! - Keeps the page cache free of DWB bytes — the O_DIRECT WAL was
//!   specifically designed not to warm the cache, and a buffered DWB
//!   undoes that by writing the exact same payload through the cache.
//! - Surfaces DWB bytes in block-layer iostat traffic alongside the WAL.
//!
//! The on-disk layout is the same in both modes (one aligned header block
//! followed by fixed-stride slots, all block-aligned) so a DWB written in
//! one mode can be read in the other.

pub mod buffer;
pub mod layout;
pub mod metrics;
pub mod mode;
pub(crate) mod raw_io;
pub mod recover;
pub mod status;

pub use buffer::DoubleWriteBuffer;
pub use layout::{slot_record_max, slot_stride};
pub use metrics::{
    wal_dwb_bytes_written_total, wal_dwb_degradations_total, wal_dwb_unprotected_records_total,
};
pub use mode::DwbMode;
pub use status::{DwbDegradation, DwbMirror, DwbProtection, DwbSkipReason};
