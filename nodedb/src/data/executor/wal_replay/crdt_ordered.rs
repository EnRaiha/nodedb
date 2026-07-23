// SPDX-License-Identifier: BUSL-1.1

//! Ordered startup replay for all CRDT WAL record classes.
//!
//! CRDT deltas, document intents, and list intents all mutate the same Loro
//! state. They must therefore be replayed in one global WAL order rather than
//! in separate record-type passes.

use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Replay standalone CRDT WAL records in stable global LSN order.
    ///
    /// `TransactionRedo` has no CRDT subrecords: CRDT writes are deliberately
    /// excluded from transaction redo encoding because raw applies require the
    /// serialized admission boundary and CRDT intent is independently durable.
    /// Consequently this method handles only the three standalone CRDT record
    /// classes and must run exactly once during startup.
    pub fn replay_crdt_wal_ordered(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let mut crdt_records: Vec<(usize, &nodedb_wal::WalRecord)> = records
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                matches!(
                    RecordType::from_raw(record.logical_record_type()),
                    Some(RecordType::CrdtDelta | RecordType::CrdtListOp | RecordType::CrdtDocOp)
                )
            })
            .collect();
        crdt_records.sort_by_key(|(original_index, record)| (record.header.lsn, *original_index));

        for (_, record) in crdt_records {
            match RecordType::from_raw(record.logical_record_type()) {
                Some(RecordType::CrdtDelta) => {
                    let _ = self.try_replay_crdt_delta(record, num_cores, tombstones);
                }
                Some(RecordType::CrdtListOp) => {
                    let _ = self.try_replay_crdt_list(record, num_cores, tombstones);
                }
                Some(RecordType::CrdtDocOp) => {
                    let _ = self.try_replay_crdt_doc(record, num_cores, tombstones);
                }
                // The filter above admits only these three record types. Keep
                // this defensive no-op rather than panicking on a future enum
                // change or malformed logical type.
                _ => continue,
            }
        }
    }
}
