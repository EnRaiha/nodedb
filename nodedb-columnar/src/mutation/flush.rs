// SPDX-License-Identifier: Apache-2.0

//! Post-write coordination: memtable flush.

use crate::error::ColumnarError;
use crate::pk_index::RowLocation;
use crate::wal_record::ColumnarWalRecord;

use super::engine::{MutationEngine, MutationResult};

impl MutationEngine {
    /// Notify the engine that the memtable was flushed to a new segment.
    ///
    /// Updates the PK index to remap memtable entries to the new segment.
    /// Returns the WAL record for the flush event, or `SegmentIdExhausted`
    /// if the u64 segment ID counter has wrapped past its maximum.
    pub fn on_memtable_flushed(
        &mut self,
        new_segment_id: u64,
    ) -> Result<MutationResult, ColumnarError> {
        let row_count = self.memtable_row_counter;

        // Remap PK index entries from virtual memtable segment to real segment.
        self.pk_index
            .remap_segment(self.memtable_segment_id, |old_row| {
                Some(RowLocation {
                    segment_id: new_segment_id,
                    row_index: old_row,
                })
            });

        // Advance the segment ID counter with overflow protection.
        let next = self
            .next_segment_id
            .checked_add(1)
            .ok_or(ColumnarError::SegmentIdExhausted)?;

        // Reset memtable tracking.
        self.memtable_segment_id = self.next_segment_id;
        self.next_segment_id = next;
        self.memtable_row_counter = 0;
        self.memtable_surrogates.clear();

        let wal = ColumnarWalRecord::MemtableFlushed {
            collection: self.collection.clone(),
            segment_id: new_segment_id,
            row_count: row_count as u64,
        };

        Ok(MutationResult {
            wal_records: vec![wal],
        })
    }
}
