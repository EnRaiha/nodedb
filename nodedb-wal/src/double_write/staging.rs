// SPDX-License-Identifier: Apache-2.0

//! Slot images staged in memory between flushes.
//!
//! `write_record_deferred` serializes each slot into the image for its ring
//! index and writes nothing to disk. `flush` writes the staged slots that
//! survive in the ring. A ring index staged twice before one flush keeps only
//! its newer image, so one flush writes at most `DWB_CAPACITY` slots however
//! many records it covers. Protection does not change: the ring overwrote
//! such a slot on disk before the fsync anyway.
//!
//! The ring holds one reusable aligned image per slot. It is allocated on the
//! first staged record, so a DWB that only recovers never pays for it.

use crate::align::{AlignedBuf, DEFAULT_ALIGNMENT};
use crate::error::Result;

use super::layout::{DWB_CAPACITY, DWB_SLOT_STRIDE};

/// One aligned slot image per ring index, plus how many records were staged
/// since the last flush.
pub(super) struct StagedRing {
    /// Exactly `DWB_CAPACITY` images, indexed by `ring index % DWB_CAPACITY`.
    images: Vec<AlignedBuf>,
    /// Records staged since the last flush, saturating.
    staged: u32,
}

impl StagedRing {
    pub(super) fn new() -> Result<Self> {
        let mut images = Vec::with_capacity(DWB_CAPACITY);
        for _ in 0..DWB_CAPACITY {
            images.push(AlignedBuf::new(DWB_SLOT_STRIDE, DEFAULT_ALIGNMENT)?);
        }
        Ok(Self { images, staged: 0 })
    }

    /// The cleared image for ring index `idx`, ready to receive a slot.
    pub(super) fn cleared_image(&mut self, idx: u32) -> &mut AlignedBuf {
        let image = self.image_mut(idx);
        image.clear();
        image
    }

    /// The staged image for ring index `idx`.
    pub(super) fn image_mut(&mut self, idx: u32) -> &mut AlignedBuf {
        &mut self.images[ring_index(idx)]
    }

    /// Count one record staged into the image just filled.
    pub(super) fn mark_staged(&mut self) {
        self.staged = self.staged.saturating_add(1);
    }

    /// Staged slots that survive in the ring: the last `DWB_CAPACITY`.
    pub(super) fn surviving(&self) -> u32 {
        self.staged.min(DWB_CAPACITY as u32)
    }

    /// Forget the staged slots once they are written.
    pub(super) fn clear(&mut self) {
        self.staged = 0;
    }
}

/// Position of ring index `idx` in the image table. `DWB_CAPACITY` divides
/// `2^32`, so a wrapping `write_pos` maps to the same slot as `slot_offset`.
fn ring_index(idx: u32) -> usize {
    idx as usize % DWB_CAPACITY
}
