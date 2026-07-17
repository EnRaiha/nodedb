// SPDX-License-Identifier: BUSL-1.1

//! On-disk type for the sync idempotency-gate checkpoint.

use serde::{Deserialize, Serialize};

/// On-disk format version.
///
/// A file stamped with any other version is refused rather than misparsed.
/// Refusing costs a WAL replay; misparsing would install a gate whose
/// high-watermarks are wrong in an unknown direction — too low re-applies
/// duplicates, too high silently discards new writes as already-seen.
pub(crate) const SYNC_HWM_CKPT_FORMAT_VERSION: u16 = 1;

/// A core's whole sync gate, as one file.
///
/// The maps are stored as sorted vectors rather than maps because the encoder
/// gives a stable byte layout for them, and a checkpoint that re-encodes
/// identical state to identical bytes is far easier to reason about on disk.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct SyncHwmCheckpointFile {
    /// Always [`SYNC_HWM_CKPT_FORMAT_VERSION`] when written; validated on load.
    pub format_version: u16,
    /// The LSN this state is durable THROUGH (inclusive) — the core watermark
    /// at flush time.
    ///
    /// This is what `execute_checkpoint` folds into the minimum it reports, and
    /// what a restart restores `sync_hwm_durable_lsn` from so a later failed
    /// flush clamps to a real point instead of pinning truncation at zero.
    pub durable_through_lsn: u64,
    /// `(producer_id, stream_id, last_applied_seq)` — the whole `sync_hwm` map.
    ///
    /// This is the deduplication half of the gate: `sync_admit` rejects a frame
    /// whose `seq <= hwm`.
    pub hwm: Vec<(u64, u64, u64)>,
    /// `(producer_id, highest_epoch_seen)` — the whole `producer_epoch_floor`
    /// map.
    ///
    /// Exported alongside the HWM rather than left to be rebuilt, because it is
    /// the other half of the same gate and has the same single durable copy: a
    /// restore that brought back the HWM but not the epoch floor would re-admit
    /// frames from a fenced-off previous generation of a producer, which is the
    /// same duplicate-application failure by another route.
    pub epoch_floor: Vec<(u64, u64)>,
}
