// SPDX-License-Identifier: BUSL-1.1

//! Columnar UPDATE and DELETE handlers for plain/spatial collections.
//!
//! Uses `nodedb-columnar`'s `MutationEngine` for full mutation support
//! (PK index, delete bitmaps, WAL records).

use nodedb_columnar::pk_index::encode_pk;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::filter::row_matches_filters;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle columnar UPDATE: scan memtable for matching rows, apply field updates.
    ///
    /// Currently operates on in-memory memtable rows only.
    /// Returns `{"affected": N}` as JSON payload.
    ///
    /// When `undo_log` is `Some` (the durable COMMIT-replay path inside a
    /// transaction batch), the pre-image of every mutated row is captured into
    /// a [`UndoEntry::ColumnarUpdate`] so a sibling sub-plan failing later in
    /// the same COMMIT can reverse this update. On the autocommit path it is
    /// `None` (no batch to roll back).
    pub(in crate::data::executor) fn execute_columnar_update(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        filter_bytes: &[u8],
        updates: &[(String, Vec<u8>)],
        rls_write_check: &[u8],
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar update");

        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let engine = match self.columnar_engines.get_mut(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        // Columnar UPDATE: scan memtable rows matching filter predicates,
        // then apply updates via PK-based MutationEngine (delete + re-insert).
        let schema = engine.schema().clone();
        let pk_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        if pk_cols.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar UPDATE requires a PRIMARY KEY column".into(),
                },
            );
        }

        let filter_predicates: Vec<ScanFilter> = if !filter_bytes.is_empty() {
            zerompk::from_msgpack(filter_bytes).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Scan memtable rows to find matches and apply updates.
        // Collect rows to update (can't mutate while iterating).
        let rows: Vec<Vec<nodedb_types::value::Value>> = engine.scan_memtable_rows().collect();

        // Undo capture (only on the durable COMMIT-replay path). `row_count_before`
        // is the memtable size before any replacement row is appended, so the
        // undo can truncate back to it; `inserted_pks`/`displaced` reverse the
        // insert half, `restored` re-materializes each tombstoned original.
        let track = undo_log.is_some();
        let row_count_before = engine.memtable().row_count();
        let mut inserted_pks: Vec<Vec<u8>> = Vec::new();
        let mut displaced: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)> = Vec::new();
        let mut restored: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)> = Vec::new();

        // Resolve every matching row's post-image, and let the write policy
        // decide all of them, BEFORE any row is mutated. The post-image is what
        // the policy governs and it exists only once the assignments have been
        // applied, so the check cannot happen earlier — and it must happen for
        // the whole statement before the first `engine.update`, or a rejection
        // partway through would leave the rows ahead of it already changed with
        // no way for the caller to see or undo that.
        let mut pending: Vec<(
            &Vec<nodedb_types::value::Value>,
            Vec<nodedb_types::value::Value>,
        )> = Vec::new();
        for row in &rows {
            // Skip rows that don't match WHERE filters.
            if !filter_predicates.is_empty() {
                match row_matches_filters(row, &schema, &filter_predicates) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(_e) => {
                        return self.response_error(task, ErrorCode::DivisionByZero);
                    }
                }
            }
            // Apply field updates to the row.
            let mut new_row = row.clone();
            for (field_name, value_bytes) in updates {
                if let Some(col_idx) = schema.columns.iter().position(|c| c.name == *field_name) {
                    let typed_val = match nodedb_types::value_from_msgpack(value_bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(
                                core = self.core_id,
                                %collection,
                                field = %field_name,
                                error = %e,
                                "columnar update: failed to decode field value as MessagePack; skipping row"
                            );
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!(
                                        "failed to decode update value for field '{field_name}': {e}"
                                    ),
                                },
                            );
                        }
                    };
                    new_row[col_idx] = typed_val;
                }
            }

            if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_columnar_row(
                rls_write_check,
                &new_row,
                &schema,
                task.request.tenant_id.as_u64(),
                collection,
            ) {
                return self.response_error(task, error);
            }

            pending.push((row, new_row));
        }

        let mut affected = 0u64;
        for (row, new_row) in &pending {
            // Extract old PK value.
            let old_pk = &row[pk_cols[0]];

            // Capture the pre-image BEFORE mutating (the update removes the old
            // PK binding and appends a new row): the tombstoned original's
            // location, the appended replacement's PK, and — for a PK-changing
            // update — the memtable row its insert half displaces.
            let capture = if track {
                let old_pk_bytes = encode_pk(old_pk);
                let old_location = engine.pk_index().get(&old_pk_bytes).copied();
                let new_pk_bytes = engine.encode_pk_from_row(new_row).ok();
                let displaced_entry = match &new_pk_bytes {
                    Some(nb) if *nb != old_pk_bytes => engine
                        .pk_index()
                        .get(nb)
                        .copied()
                        .filter(|loc| loc.segment_id == engine.memtable_segment_id())
                        .map(|loc| (nb.clone(), loc)),
                    _ => None,
                };
                Some((old_pk_bytes, old_location, new_pk_bytes, displaced_entry))
            } else {
                None
            };

            // Execute update via MutationEngine (delete + insert).
            match engine.update(old_pk, new_row) {
                Ok(_result) => {
                    affected += 1;
                    if let Some((old_pk_bytes, old_location, new_pk_bytes, displaced_entry)) =
                        capture
                    {
                        if let Some(nb) = new_pk_bytes {
                            inserted_pks.push(nb);
                        }
                        if let Some(loc) = old_location {
                            restored.push((old_pk_bytes, loc));
                        }
                        if let Some(d) = displaced_entry {
                            displaced.push(d);
                        }
                    }
                }
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar update row failed");
                }
            }
        }

        if let Some(log) = undo_log {
            log.push(UndoEntry::ColumnarUpdate {
                collection_key: key,
                row_count_before,
                inserted_pks,
                displaced,
                restored,
            });
        }

        // Advance the collection floor for this committed columnar write, exactly
        // as `execute_columnar_insert` does.
        //
        // The columnar checkpoint stamps its generation with the core watermark
        // and that stamp becomes the replay floor, so the watermark must mean
        // "every columnar record at or below this is folded into the engines".
        // An UPDATE that mutated rows without raising it would sit ABOVE the
        // stamp of a checkpoint that already contains it, and replay would
        // re-execute it — appending a duplicate row, since the update is
        // delete-old-PK + insert-new-row rather than an overwrite.
        //
        // Gated on `affected` for the same reason as the insert path: a
        // predicate that matched nothing wrote nothing, so it owes no floor, and
        // re-executing it against the identical restored state matches nothing
        // again.
        if affected > 0 {
            self.note_collection_write_lsn(task, collection);
        }

        debug!(core = self.core_id, %collection, affected, "columnar update complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Handle columnar DELETE: scan memtable for matching rows, delete them.
    ///
    /// Currently operates on in-memory memtable rows only.
    /// Returns `{"affected": N}` as JSON payload.
    ///
    /// When `undo_log` is `Some` (the durable COMMIT-replay path inside a
    /// transaction batch), the `(pk_bytes, RowLocation)` of every deleted row
    /// is captured into a [`UndoEntry::ColumnarDelete`] so a sibling sub-plan
    /// failing later in the same COMMIT can restore the rows. On the autocommit
    /// path it is `None`.
    pub(in crate::data::executor) fn execute_columnar_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        filter_bytes: &[u8],
        rls_write_check: &[u8],
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar delete");

        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let engine = match self.columnar_engines.get_mut(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        let schema = engine.schema().clone();
        let pk_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        if pk_cols.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar DELETE requires a PRIMARY KEY column".into(),
                },
            );
        }

        let filter_predicates: Vec<ScanFilter> = if !filter_bytes.is_empty() {
            zerompk::from_msgpack(filter_bytes).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Collect only the PK values of rows that match the WHERE filter
        // (can't mutate while iterating).
        let rows: Vec<Vec<nodedb_types::value::Value>> = engine.scan_memtable_rows().collect();
        let mut pk_values: Vec<nodedb_types::value::Value> = Vec::new();
        for row in &rows {
            if !filter_predicates.is_empty() {
                match row_matches_filters(row, &schema, &filter_predicates) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(_e) => {
                        return self.response_error(task, ErrorCode::DivisionByZero);
                    }
                }
            }
            // The image a delete is governed by is the row it removes, and that
            // row is only known here. Every matched row is decided before the
            // first `engine.delete`, so a rejection removes nothing at all
            // rather than leaving the rows ahead of it already tombstoned.
            if let Err(error) = crate::data::executor::handlers::rls_write_gate::admit_columnar_row(
                rls_write_check,
                row,
                &schema,
                task.request.tenant_id.as_u64(),
                collection,
            ) {
                return self.response_error(task, error);
            }
            pk_values.push(row[pk_cols[0]].clone());
        }

        // Undo capture (only on the durable COMMIT-replay path): the location
        // and PK bytes of each tombstoned row, so the undo can clear its
        // delete-bitmap bit and re-bind the PK index.
        let track = undo_log.is_some();
        let mut restored: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)> = Vec::new();

        let mut affected = 0u64;
        for pk in &pk_values {
            // Read the location BEFORE the delete removes the PK binding.
            let captured = if track {
                let pk_bytes = encode_pk(pk);
                engine
                    .pk_index()
                    .get(&pk_bytes)
                    .copied()
                    .map(|loc| (pk_bytes, loc))
            } else {
                None
            };
            match engine.delete(pk) {
                Ok(_) => {
                    affected += 1;
                    if let Some(entry) = captured {
                        restored.push(entry);
                    }
                }
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar delete row failed");
                }
            }
        }

        if let Some(log) = undo_log {
            log.push(UndoEntry::ColumnarDelete {
                collection_key: key,
                restored,
            });
        }

        // Advance the collection floor for this committed columnar write. DELETE
        // replays idempotently, so unlike UPDATE it is not the record whose
        // double-application corrupts — but the watermark is a single claim
        // across all columnar records, and a delete left unstamped understates
        // it, needlessly holding WAL segments and denying a concurrent
        // transaction the conflict it should see against the rows this removed.
        if affected > 0 {
            self.note_collection_write_lsn(task, collection);
        }

        debug!(core = self.core_id, %collection, affected, "columnar delete complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json_as_msgpack(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
