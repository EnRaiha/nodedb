// SPDX-License-Identifier: BUSL-1.1

use nodedb_wal::record::{RecordType, RestorePointPayload};

use super::appender::WalAppender;
use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalAppender<'_> {
    /// Append one group's place at a cluster restore point. The record
    /// carries the point's HLC as its commit HLC: a restore to an earlier
    /// point drops it with every other write after that point.
    pub fn append_restore_point(&self, point: &RestorePointPayload) -> crate::Result<Lsn> {
        self.with_commit_hlc(point.hlc).append_record(
            RecordType::RestorePoint,
            TenantId::new(0),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &point.to_bytes(),
        )
    }
}

impl WalManager {
    /// Seal the active segment, so the archiver uploads every record
    /// appended so far: a restore point is usable once its records are
    /// archived.
    pub fn seal_active_segment(&self) -> crate::Result<()> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.seal_active_segment().map_err(crate::Error::Wal)
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::WalManager;
    use crate::wal::manager::NO_APPLY_KEY;

    use super::*;

    #[test]
    fn a_restore_point_record_replays_with_its_payload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let point = RestorePointPayload {
            id: 9,
            hlc: 1_000,
            group_id: 3,
            applied_index: 44,
            term: 2,
            next_epoch: 0,
            epoch_system_ms: 0,
            vshards: vec![1, 2],
        };
        wal.appender(NO_APPLY_KEY)
            .append_restore_point(&point)
            .expect("append");
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");
        let record = records
            .iter()
            .find(|r| {
                RecordType::from_raw(r.logical_record_type()) == Some(RecordType::RestorePoint)
            })
            .expect("the restore point record");
        assert_eq!(record.header.commit_hlc, point.hlc);
        assert_eq!(
            RestorePointPayload::from_bytes(&record.payload).expect("payload"),
            point
        );
    }

    #[test]
    fn sealing_moves_the_restore_point_out_of_the_active_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        wal.seal_active_segment()
            .expect("an empty segment seals as a no-op");
        let before = wal.next_lsn();
        wal.appender(NO_APPLY_KEY)
            .append_restore_point(&RestorePointPayload {
                id: 1,
                hlc: 10,
                group_id: 0,
                applied_index: 1,
                term: 1,
                next_epoch: 0,
                epoch_system_ms: 0,
                vshards: Vec::new(),
            })
            .expect("append");
        wal.seal_active_segment().expect("seal");
        let wal_lock = wal.wal.lock().unwrap_or_else(|p| p.into_inner());
        assert!(wal_lock.active_segment_first_lsn() > before.as_u64());
    }
}
