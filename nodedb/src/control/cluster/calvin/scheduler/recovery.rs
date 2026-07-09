// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler WAL recovery.
//!
//! Provides [`read_applied_recovery`] which scans the WAL for
//! `RecordType::CalvinApplied` records and returns, for a given vShard, the set
//! of applied `(epoch, position)` pairs together with a fully-applied watermark.
//! The scheduler uses this on startup to seed its exactly-once applied gate
//! (see [`super::applied_gate::AppliedGate`]).
//!
//! Each `CalvinApplied` marker is per `(epoch, position, vShard)` — one per
//! independent transaction position — so the scan preserves `position` rather
//! than collapsing an epoch to a single "applied" bit. Collapsing to the max
//! epoch would mark a whole epoch applied on the strength of its first committed
//! position and, on restart, skip every other position of that epoch: a lost /
//! torn transaction.
//!
//! The returned watermark is deliberately conservative: at recovery the
//! per-epoch expected position counts are not yet known (they arrive with the
//! sequencer's re-fan-out), so nothing can be *proven* fully applied and the
//! watermark stays at [`NOT_YET_APPLIED_EPOCH`]. Every marker rides in the tail,
//! where the applied-gate skip is exact; the watermark then advances as the
//! re-fan-out supplies the counts. `max_applied_epoch` is the highest epoch with
//! any marker — the rebuild-target cursor, independent of the exact gate.

use std::collections::BTreeSet;

use nodedb_wal::record::RecordType;
use nodedb_wal::{CalvinAppliedPayload, WalRecord};
use tracing::warn;

use crate::wal::manager::WalManager;

/// Sentinel used when no Calvin epoch has ever been fully applied / no marker
/// exists for this vShard.
pub const NOT_YET_APPLIED_EPOCH: u64 = u64::MAX;

/// Result of scanning the WAL for a vShard's `CalvinApplied` markers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedRecovery {
    /// Fully-applied watermark `W` to seed the applied gate. Conservative at
    /// recovery: [`NOT_YET_APPLIED_EPOCH`] unless proven otherwise (it isn't,
    /// without the per-epoch expected counts), so the tail carries every marker.
    pub fully_applied_epoch: u64,
    /// Applied `(epoch, position)` pairs found for this vShard.
    pub applied_tail: BTreeSet<(u64, u32)>,
    /// Highest epoch with any marker for this vShard, or
    /// [`NOT_YET_APPLIED_EPOCH`] if none. Used as the rebuild-target cursor.
    pub max_applied_epoch: u64,
}

/// Scan the WAL and collect this vShard's applied `(epoch, position)` markers.
///
/// Returns an empty tail and the [`NOT_YET_APPLIED_EPOCH`] sentinel for a
/// greenfield node (no `CalvinApplied` records exist).
///
/// Records that fail to decode are logged and skipped — a corrupt record does
/// not abort the scan.
pub fn read_applied_recovery(wal: &WalManager, vshard_id: u32) -> crate::Result<AppliedRecovery> {
    let records = wal.replay()?;
    let mut applied_tail = BTreeSet::new();
    let mut max_applied_epoch = NOT_YET_APPLIED_EPOCH;

    for record in &records {
        if !is_calvin_applied_record(record) {
            continue;
        }
        match CalvinAppliedPayload::from_bytes(&record.payload) {
            Ok(p) if p.vshard_id == vshard_id => {
                applied_tail.insert((p.epoch, p.position));
                if max_applied_epoch == NOT_YET_APPLIED_EPOCH || p.epoch > max_applied_epoch {
                    max_applied_epoch = p.epoch;
                }
            }
            Ok(_) => {
                // Different vshard — skip.
            }
            Err(e) => {
                warn!(
                    lsn = record.header.lsn,
                    error = %e,
                    "calvin recovery: failed to decode CalvinApplied payload; skipping"
                );
            }
        }
    }

    Ok(AppliedRecovery {
        // Conservative: the applied gate advances the watermark once the
        // re-fan-out supplies the per-epoch expected counts.
        fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
        applied_tail,
        max_applied_epoch,
    })
}

fn is_calvin_applied_record(record: &WalRecord) -> bool {
    // Strip the encryption flag (bit 31) before comparing record type.
    let raw_type = record.header.record_type & !nodedb_wal::record::ENCRYPTED_FLAG;
    matches!(
        RecordType::from_raw(raw_type),
        Some(RecordType::CalvinApplied)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::wal::manager::WalManager;

    fn open_wal(dir: &TempDir) -> WalManager {
        WalManager::open(dir.path(), false).expect("open wal")
    }

    #[test]
    fn greenfield_returns_sentinel_and_empty_tail() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        let rec = read_applied_recovery(&wal, 1).unwrap();
        assert_eq!(rec.fully_applied_epoch, NOT_YET_APPLIED_EPOCH);
        assert_eq!(rec.max_applied_epoch, NOT_YET_APPLIED_EPOCH);
        assert!(rec.applied_tail.is_empty());
    }

    #[test]
    fn tail_records_exact_positions_and_max_epoch() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);

        use crate::types::VShardId;
        // Epoch 5: position 0 applied, position 1 NOT applied. Epoch 2: pos 0.
        wal.append_calvin_applied(VShardId::new(1), 2, 0).unwrap();
        wal.append_calvin_applied(VShardId::new(1), 5, 0).unwrap();
        // A different vshard (must be ignored).
        wal.append_calvin_applied(VShardId::new(2), 99, 0).unwrap();
        wal.sync().unwrap();

        let rec = read_applied_recovery(&wal, 1).unwrap();
        // The recovery API reports (5,0) applied and (5,1) NOT applied — the
        // exact per-position distinction the old max-epoch collapse destroyed.
        assert!(rec.applied_tail.contains(&(5, 0)), "(5,0) is applied");
        assert!(!rec.applied_tail.contains(&(5, 1)), "(5,1) is NOT applied");
        assert!(rec.applied_tail.contains(&(2, 0)));
        assert_eq!(rec.max_applied_epoch, 5);
        // The watermark stays below E: nothing is proven fully applied yet, so
        // the exact skip lives entirely in the tail.
        assert_eq!(rec.fully_applied_epoch, NOT_YET_APPLIED_EPOCH);
        assert!(rec.fully_applied_epoch == NOT_YET_APPLIED_EPOCH || rec.fully_applied_epoch < 5);

        let rec2 = read_applied_recovery(&wal, 2).unwrap();
        assert!(rec2.applied_tail.contains(&(99, 0)));
        assert_eq!(rec2.max_applied_epoch, 99);
    }

    #[test]
    fn multi_position_epoch_is_not_collapsed() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        use crate::types::VShardId;
        let vshard = 3u32;

        // Epoch 7 carries two independent positions on this vShard; only
        // position 0 committed before the crash.
        wal.append_calvin_applied(VShardId::new(vshard), 7, 0)
            .unwrap();
        wal.sync().unwrap();

        let rec = read_applied_recovery(&wal, vshard).unwrap();
        assert!(rec.applied_tail.contains(&(7, 0)));
        assert!(
            !rec.applied_tail.contains(&(7, 1)),
            "position 1 of epoch 7 must be reported as NOT applied so it is \
             re-applied on restart rather than lost"
        );
    }
}
