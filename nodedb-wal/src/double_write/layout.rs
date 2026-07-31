// SPDX-License-Identifier: Apache-2.0

//! On-disk layout of the double-write buffer file.
//!
//! ```text
//! offset 0                       one aligned block: file header
//!   [magic:4][count:4][write_pos:4][zero padding to DWB_HEADER_STRIDE]
//!
//! offset DWB_HEADER_STRIDE + i * DWB_SLOT_STRIDE      slot i
//!   [slot_magic:4][seq:8][total_size:4][prefix_crc:4][record header][payload]
//! ```
//!
//! ## Slot sequence numbers
//!
//! The ring is fixed-size, so one LSN can legitimately occupy two slots: a
//! crash between the DWB fsync and the WAL fsync leaves an LSN durable in the
//! DWB but absent from the WAL, recovery resumes at that LSN, and the next
//! record written under it lands in a different slot. Both copies pass CRC, so
//! nothing in the record itself says which one the WAL actually committed.
//! `seq` breaks that tie: it increases with every slot write, so the highest
//! `seq` is always the copy written last. Without it, recovery can hand back a
//! payload that was never acknowledged to any client.
//!
//! ## Format change
//!
//! Slots used to begin with a bare `[total_size:4]`. They now begin with
//! `slot_magic`, whose value is far outside the range a `total_size` could
//! ever hold, so a slot written by an older build fails the prefix check and
//! is treated as unusable rather than being misread as a sequence-numbered
//! slot. A DWB only ever holds records from the tail of the log — the worst
//! case is that torn-write recovery falls back to "no copy available" for
//! records written before the upgrade.

use crate::align::DEFAULT_ALIGNMENT;
use crate::record::HEADER_SIZE;

/// Maximum number of records kept in the double-write buffer.
/// Only the most recent records matter — torn writes affect the tail.
///
/// This is a compile-time constant used in slot offset arithmetic. It cannot
/// be made runtime-configurable without storing capacity in the struct and
/// adjusting all offset calculations accordingly. The value matches the
/// `WalTuning::dwb_capacity` default (64).
pub(crate) const DWB_CAPACITY: usize = 64;

/// Largest record (header + payload) that fits in one slot.
pub(crate) const DWB_SLOT_RECORD_MAX: usize = 64 * 1024;

/// Identifies a sequence-numbered slot. Deliberately larger than any value a
/// pre-sequence-number slot could carry in its leading `total_size` field.
const DWB_SLOT_MAGIC: u32 = 0x4457_5342; // "DWSB"

/// Bytes of slot metadata ahead of the WAL record header.
pub(crate) const SLOT_PREFIX_SIZE: usize = 20;

/// Raw slot content size: [prefix][header][payload-up-to-64KiB].
const DWB_SLOT_RAW: usize = SLOT_PREFIX_SIZE + HEADER_SIZE + DWB_SLOT_RECORD_MAX;

/// Per-slot on-disk stride, padded up to the O_DIRECT block size so every
/// slot offset is block-aligned.
pub(crate) const DWB_SLOT_STRIDE: usize = round_up_const(DWB_SLOT_RAW, DEFAULT_ALIGNMENT);

/// On-disk header occupies one aligned block (not the raw 12 bytes) so the
/// first slot starts at a block-aligned offset. The first 12 bytes of the
/// block carry the header fields; the remainder is zero-padded.
pub(crate) const DWB_HEADER_STRIDE: usize = DEFAULT_ALIGNMENT;
pub(crate) const DWB_HEADER_FIELDS: usize = 12;
pub(crate) const DWB_MAGIC: u32 = 0x4457_4246; // "DWBF"

pub(crate) const fn round_up_const(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Slot stride in bytes. Exposed for tests and for callers that want to
/// size DWB files ahead of time.
pub const fn slot_stride() -> usize {
    DWB_SLOT_STRIDE
}

/// Largest record — WAL header plus payload — the buffer can mirror. Records
/// above this size are appended to the WAL without torn-write protection.
pub const fn slot_record_max() -> usize {
    DWB_SLOT_RECORD_MAX
}

/// Byte offset of slot `idx` within the DWB file.
pub(crate) fn slot_offset(idx: u32) -> u64 {
    DWB_HEADER_STRIDE as u64 + (idx as u64 % DWB_CAPACITY as u64) * DWB_SLOT_STRIDE as u64
}

/// The self-describing metadata at the head of a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlotPrefix {
    /// Monotonic write ordering across the whole ring.
    pub seq: u64,
    /// WAL header bytes plus payload bytes stored in this slot.
    pub total_size: usize,
}

impl SlotPrefix {
    pub(crate) fn encode(&self) -> [u8; SLOT_PREFIX_SIZE] {
        let mut buf = [0u8; SLOT_PREFIX_SIZE];
        buf[0..4].copy_from_slice(&DWB_SLOT_MAGIC.to_le_bytes());
        buf[4..12].copy_from_slice(&self.seq.to_le_bytes());
        buf[12..16].copy_from_slice(&(self.total_size as u32).to_le_bytes());
        let crc = crc32c::crc32c(&buf[0..16]);
        buf[16..20].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Parse a slot prefix, returning `None` for anything that is not a
    /// well-formed, checksummed prefix of the current format.
    ///
    /// The prefix carries its own CRC because `seq` decides which of two
    /// same-LSN copies wins, and the record CRC does not cover it — a slot
    /// whose head was torn must lose the tie rather than win it with a
    /// garbage sequence number.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < SLOT_PREFIX_SIZE {
            return None;
        }
        let mut arr4 = [0u8; 4];
        arr4.copy_from_slice(&bytes[0..4]);
        if u32::from_le_bytes(arr4) != DWB_SLOT_MAGIC {
            return None;
        }
        arr4.copy_from_slice(&bytes[16..20]);
        if u32::from_le_bytes(arr4) != crc32c::crc32c(&bytes[0..16]) {
            return None;
        }
        let mut arr8 = [0u8; 8];
        arr8.copy_from_slice(&bytes[4..12]);
        let seq = u64::from_le_bytes(arr8);
        arr4.copy_from_slice(&bytes[12..16]);
        let total_size = u32::from_le_bytes(arr4) as usize;
        if !(HEADER_SIZE..=DWB_SLOT_RECORD_MAX).contains(&total_size) {
            return None;
        }
        Some(Self { seq, total_size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::align::is_aligned;

    #[test]
    fn slot_stride_is_o_direct_aligned() {
        // The DWB slot stride must be a multiple of the WAL alignment
        // (4 KiB) so the file can be opened with O_DIRECT alongside an
        // O_DIRECT WAL. With a non-aligned stride, every slot after the
        // first lands at an unaligned offset and the kernel rejects the
        // write with -EINVAL.
        assert!(
            is_aligned(DWB_SLOT_STRIDE, DEFAULT_ALIGNMENT),
            "DWB slot stride {DWB_SLOT_STRIDE} bytes is not a multiple of {DEFAULT_ALIGNMENT}"
        );
        assert!(is_aligned(DWB_HEADER_STRIDE, DEFAULT_ALIGNMENT));
        for i in 0..DWB_CAPACITY as u32 {
            assert!(is_aligned(slot_offset(i) as usize, DEFAULT_ALIGNMENT));
        }
    }

    #[test]
    fn prefix_round_trips() {
        let prefix = SlotPrefix {
            seq: 0x0102_0304_0506_0708,
            total_size: HEADER_SIZE + 11,
        };
        assert_eq!(SlotPrefix::decode(&prefix.encode()), Some(prefix));
    }

    #[test]
    fn torn_prefix_is_rejected() {
        let prefix = SlotPrefix {
            seq: 9,
            total_size: HEADER_SIZE,
        };
        let mut bytes = prefix.encode();
        bytes[6] ^= 0xff;
        assert_eq!(SlotPrefix::decode(&bytes), None);
    }

    #[test]
    fn pre_sequence_number_slot_fails_closed() {
        // An older build wrote `[total_size:4]` where the magic now lives.
        let mut bytes = [0u8; SLOT_PREFIX_SIZE];
        bytes[0..4].copy_from_slice(&((HEADER_SIZE + 32) as u32).to_le_bytes());
        assert_eq!(SlotPrefix::decode(&bytes), None);
    }
}
