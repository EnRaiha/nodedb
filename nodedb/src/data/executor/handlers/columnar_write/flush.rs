// SPDX-License-Identifier: BUSL-1.1

//! Post-insert memtable flush: drains the columnar memtable to a segment
//! once the flush threshold is reached, retaining encoded segment bytes
//! and their surrogate sidecar in memory.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Flush the columnar memtable at `engine_key` to a segment if the
    /// flush threshold has been reached. No-op otherwise.
    pub(in crate::data::executor) fn flush_columnar_memtable_if_needed(
        &mut self,
        task: &ExecutionTask,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        collection: &str,
    ) -> Result<(), Response> {
        let engine = match self.columnar_engines.get_mut(engine_key) {
            Some(e) => e,
            None => {
                return Err(self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar engine missing after insert loop".into(),
                    },
                ));
            }
        };

        // Flush memtable to a segment if the threshold has been reached.
        if engine.should_flush() {
            let new_segment_id = engine.next_segment_id();
            let (schema, columns, row_count) = engine.memtable_mut().drain_optimized();
            // Capture the memtable's per-row surrogates BEFORE `on_memtable_flushed`
            // clears them. `drain_optimized` drains the row data but leaves
            // `memtable_surrogates` intact; only `on_memtable_flushed` (below)
            // clears it. This snapshot is the pre-clear, index-aligned identity
            // table for the rows we are about to encode into the segment.
            let flushed_surrogates: Vec<Option<nodedb_types::Surrogate>> =
                engine.memtable_surrogates().to_vec();
            if row_count > 0 {
                let kek = self.segment_keks.columnar_segment_kek.as_ref();
                match nodedb_columnar::SegmentWriter::plain()
                    .write_segment(&schema, &columns, row_count, kek)
                {
                    Ok(bytes) => {
                        // Lockstep invariant: push to BOTH maps for the same key in
                        // the SAME order so the segment-bytes Vec and the surrogate
                        // sidecar stay equal-length and index-aligned (outer index
                        // == segment index; segment_id == index + 1). On the Err
                        // branch below we push to NEITHER, preserving lockstep.
                        self.columnar_flushed_segments
                            .entry(engine_key.clone())
                            .or_default()
                            .push(bytes);
                        self.columnar_flushed_surrogates
                            .entry(engine_key.clone())
                            .or_default()
                            .push(flushed_surrogates);
                        tracing::debug!(
                            core = self.core_id,
                            %collection,
                            new_segment_id,
                            row_count,
                            "columnar memtable flushed and segment bytes retained in memory"
                        );
                    }
                    Err(e) => {
                        // The memtable was already drained above, so these rows
                        // are no longer in memory and were NOT encoded to a
                        // segment. We must not continue and report success: on
                        // the sync path that would call `sync_commit` + return
                        // `AckStatus::Applied`, telling the client durably-lost
                        // data was applied and advancing the HWM so the retry is
                        // never re-admitted. Fail hard instead — the HWM stays
                        // put and the client (or SQL caller) retries.
                        tracing::error!(
                            core = self.core_id,
                            %collection,
                            new_segment_id,
                            row_count,
                            error = %e,
                            "columnar segment encode failed; drained rows not durable, failing the write"
                        );
                        return Err(self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "columnar segment encode failed, {row_count} rows not durable: {e}"
                                ),
                            },
                        ));
                    }
                }
            }
            if let Err(e) = engine.on_memtable_flushed(new_segment_id) {
                return Err(self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar flush: segment ID counter exhausted: {e}"),
                    },
                ));
            }
        }

        Ok(())
    }
}
