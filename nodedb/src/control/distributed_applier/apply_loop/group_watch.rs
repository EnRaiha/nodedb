// SPDX-License-Identifier: BUSL-1.1

//! Per-group state the apply loop keeps across batches: the highest index it
//! applied, to report a second apply of a committed entry, and the cut
//! floors, which raise the commit HLC of every entry after a cut barrier.

use std::collections::{BTreeMap, HashMap};

use crate::control::pitr::restore_point::RecordedCut;

/// Per-group apply state.
#[derive(Debug, Default)]
pub(super) struct GroupWatch {
    highest_applied: HashMap<u64, u64>,
    /// Per group, barrier log index to the lowest commit HLC an entry after
    /// that barrier records: one above the barrier's watermark.
    cut_floors: HashMap<u64, BTreeMap<u64, u64>>,
}

impl GroupWatch {
    /// A watch that knows the restore point cuts this node applied before a
    /// restart: the entries it applies again after one of them record above
    /// it, as they did the first time.
    pub(super) fn with_cuts(cuts: &[RecordedCut]) -> Self {
        let mut watch = Self::default();
        for cut in cuts {
            watch.raise_cut(cut.group_id, cut.barrier_index, cut.hlc);
        }
        watch
    }

    /// Note that the apply loop applies `(group_id, log_index)`. Reports an
    /// index at or below one it already applied as a second apply.
    pub(super) fn note_apply(&mut self, group_id: u64, log_index: u64) {
        let highest = self.highest_applied.entry(group_id).or_insert(0);
        if log_index <= *highest {
            crate::diag::raft_entry_reapplied(group_id, log_index, *highest);
            return;
        }
        *highest = log_index;
        self.fold_cuts_below(group_id, log_index);
    }

    /// Raise `group_id`'s floor above the watermark `cut_hlc` of the cut
    /// barrier at `barrier_index`.
    pub(super) fn raise_cut(&mut self, group_id: u64, barrier_index: u64, cut_hlc: u64) {
        let floor = self
            .cut_floors
            .entry(group_id)
            .or_default()
            .entry(barrier_index)
            .or_insert(0);
        *floor = (*floor).max(cut_hlc.saturating_add(1));
    }

    /// The commit HLC the entry of `group_id` at `log_index` stamped
    /// `write_hlc` records.
    ///
    /// An entry the log places after a cut barrier records at least the cut
    /// floor, however early its proposer stamped it: the cut did not contain
    /// it, so a restore of that cut refuses it. `0` means the entry carries no
    /// stamp; its apply stamps its own append, which already follows every
    /// barrier before it.
    pub(super) fn commit_hlc(&self, group_id: u64, log_index: u64, write_hlc: u64) -> u64 {
        if write_hlc == 0 {
            return 0;
        }
        let floor = self
            .cut_floors
            .get(&group_id)
            .and_then(|floors| floors.range(..log_index).map(|(_, floor)| *floor).max());
        floor.map_or(write_hlc, |floor| write_hlc.max(floor))
    }

    /// Fold every barrier below `log_index` into the highest of them: entries
    /// apply in log order, so no later entry sits between two of them.
    fn fold_cuts_below(&mut self, group_id: u64, log_index: u64) {
        let Some(floors) = self.cut_floors.get_mut(&group_id) else {
            return;
        };
        let below: Vec<(u64, u64)> = floors.range(..log_index).map(|(i, f)| (*i, *f)).collect();
        let Some(&(last_index, _)) = below.last() else {
            return;
        };
        if below.len() < 2 {
            return;
        }
        let folded = below.iter().map(|(_, floor)| *floor).max().unwrap_or(0);
        for (index, _) in &below {
            floors.remove(index);
        }
        floors.insert(last_index, folded);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entry_after_a_cut_records_above_the_cut() {
        let mut watch = GroupWatch::default();
        assert_eq!(watch.commit_hlc(1, 4, 50), 50);
        watch.raise_cut(1, 5, 100);
        assert_eq!(watch.commit_hlc(1, 6, 50), 101);
        assert_eq!(watch.commit_hlc(1, 6, 200), 200);
        assert_eq!(
            watch.commit_hlc(1, 5, 50),
            50,
            "the barrier's own place is below it"
        );
        assert_eq!(watch.commit_hlc(2, 6, 50), 50, "a cut binds only its group");
        assert_eq!(
            watch.commit_hlc(1, 6, 0),
            0,
            "an unstamped entry stamps its own append"
        );
    }

    #[test]
    fn an_entry_applied_again_after_a_restart_records_as_before() {
        let cuts = [
            RecordedCut {
                group_id: 1,
                barrier_index: 10,
                hlc: 100,
            },
            RecordedCut {
                group_id: 1,
                barrier_index: 20,
                hlc: 200,
            },
        ];
        let mut watch = GroupWatch::with_cuts(&cuts);
        // Delivery resumes between the two barriers.
        watch.note_apply(1, 15);
        assert_eq!(watch.commit_hlc(1, 15, 50), 101);
        watch.note_apply(1, 25);
        assert_eq!(watch.commit_hlc(1, 25, 50), 201);
        // Folding kept the highest floor below the applied index.
        assert_eq!(watch.cut_floors[&1].len(), 1);
    }

    /// A checkpoint truncates the WAL past the barrier's record, the node
    /// restarts, and the floor still binds the entries after the barrier.
    #[test]
    fn a_floor_outlives_the_wal_record_of_its_barrier() {
        use crate::control::pitr::restore_point::load_recorded_cuts;
        use crate::control::security::catalog::SystemCatalog;
        use crate::control::security::catalog::cut_floors::StoredBarrier;
        use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
        use crate::wal::WalManager;
        use crate::wal::manager::NO_APPLY_KEY;
        use nodedb_wal::record::{RecordType, RestorePointPayload};

        let dir = tempfile::tempdir().unwrap();
        let catalog_path = dir.path().join("system.redb");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).unwrap();
        {
            // The barrier at index 10 of group 1 applies with watermark 100.
            let catalog = SystemCatalog::open(&catalog_path).unwrap();
            wal.appender(NO_APPLY_KEY)
                .append_restore_point(&RestorePointPayload {
                    id: 9,
                    hlc: 100,
                    group_id: 1,
                    applied_index: 10,
                    term: 1,
                    next_epoch: 0,
                    epoch_system_ms: 0,
                    vshards: vec![3],
                })
                .unwrap();
            catalog
                .put_cut_floor(
                    1,
                    StoredBarrier {
                        index: 10,
                        watermark: 100,
                    },
                    0,
                )
                .unwrap();
        }
        wal.seal_active_segment().unwrap();
        wal.appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(3),
                DatabaseId::DEFAULT,
                b"row",
            )
            .unwrap();
        wal.sync().unwrap();
        wal.truncate_before(Lsn::new(wal.next_lsn().as_u64()))
            .unwrap();
        let barrier_left = wal.replay().unwrap().iter().any(|record| {
            RecordType::from_raw(record.logical_record_type()) == Some(RecordType::RestorePoint)
        });
        assert!(
            !barrier_left,
            "the checkpoint truncated the barrier's record"
        );

        // The restart reads the floors back from the catalog.
        let catalog = SystemCatalog::open(&catalog_path).unwrap();
        let mut watch = GroupWatch::with_cuts(&load_recorded_cuts(&catalog).unwrap());
        watch.note_apply(1, 11);
        assert_eq!(watch.commit_hlc(1, 11, 50), 101);
    }

    #[test]
    fn a_second_apply_of_an_index_is_counted() {
        let before = crate::diag::raft_entries_reapplied();
        let mut watch = GroupWatch::default();
        watch.note_apply(3, 5);
        watch.note_apply(3, 6);
        watch.note_apply(3, 6);
        assert!(crate::diag::raft_entries_reapplied() > before);
    }
}
