// SPDX-License-Identifier: BUSL-1.1

//! Columnar insert dispatcher: builds rows, applies ON CONFLICT semantics,
//! drives the per-row insert, flushes the memtable, updates spatial index.

use nodedb_columnar::MutationEngine;
use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::surrogate::Surrogate;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::upsert::apply_on_conflict_updates;
use crate::data::executor::response_codec;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::document::UpdateValue;

use super::schema::{
    infer_schema_from_value, ndb_field_to_value, prepend_bitemporal_columns, row_values_to_object,
};

impl CoreLoop {
    /// Execute a columnar insert: write rows from MessagePack payload to
    /// `MutationEngine`, applying intent-specific semantics on duplicate
    /// PK (upsert-overwrite for `Insert` and `Put`, silent skip for
    /// `InsertIfAbsent`, merge-via-`apply_on_conflict_updates` for `Put`
    /// with non-empty `on_conflict_updates`).
    ///
    /// When `provenance` is `Some`, the sync idempotency gate runs first.
    /// Duplicate / Fenced / Gap arms return `SyncAckResult` via
    /// `response_with_payload` without touching engine state.
    /// Apply → proceed with insert → call `sync_commit` → return
    /// `SyncAckResult{Applied}`.
    ///
    /// When `provenance` is `None` (SQL path), behave as before.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_columnar_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        payload: &[u8],
        _format: &str,
        intent: ColumnarInsertIntent,
        on_conflict_updates: &[(String, UpdateValue)],
        surrogates: &[Surrogate],
        schema_bytes: &[u8],
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        // ── Sync idempotency gate (Data-Plane side) ──────────────────────────
        if let Some(prov) = provenance {
            let admit = self.sync_admit(prov);
            match admit {
                SyncAdmit::Apply => {
                    // Fall through to the insert path below.
                }
                non_apply => {
                    let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&non_apply),
                        current_hwm,
                    );
                }
            }
        }
        // Parse payload: msgpack-encoded nodedb_types::Value (array or object).
        let ndb_rows: Vec<nodedb_types::Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(arr)) => arr,
            Ok(v @ nodedb_types::Value::Object(_)) => vec![v],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar insert: payload must be array or object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar insert: invalid payload: {e}"),
                    },
                );
            }
        };

        if ndb_rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        let engine_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let tid = task.request.tenant_id.as_u64();
        let bitemporal = self.is_bitemporal(tid, collection);
        // Ensure MutationEngine exists (auto-create on first write).
        if !self.columnar_engines.contains_key(&engine_key) {
            // Prefer the DDL schema carried on `schema_bytes` over inference
            // from the payload. Inference cannot distinguish JSON columns from
            // plain strings because both arrive as `Value::String`.
            let base_schema = if !schema_bytes.is_empty() {
                zerompk::from_msgpack::<ColumnarSchema>(schema_bytes)
                    .unwrap_or_else(|_| infer_schema_from_value(&ndb_rows[0]))
            } else {
                infer_schema_from_value(&ndb_rows[0])
            };
            let schema = if bitemporal {
                prepend_bitemporal_columns(base_schema)
            } else {
                base_schema
            };
            let engine = MutationEngine::with_flush_threshold(
                collection.to_string(),
                schema,
                self.query_tuning.columnar_flush_threshold,
            );
            self.columnar_engines.insert(engine_key.clone(), engine);
        }

        let schema = match self.columnar_engines.get(&engine_key) {
            Some(e) => e.schema().clone(),
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar engine missing after create".into(),
                    },
                );
            }
        };
        let mut accepted = 0u64;

        for (row_idx, row) in ndb_rows.iter().enumerate() {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };

            // Build Value slice in schema order. For bitemporal
            // collections, the three reserved columns are auto-populated
            // when absent from the user payload: `_ts_system` is always
            // clamped to the current wall-clock time (clients cannot
            // forge system time), `_ts_valid_from` / `_ts_valid_until`
            // default to the open interval `[i64::MIN, i64::MAX)` if
            // missing.
            let sys_now = if bitemporal {
                self.bitemporal_now_ms()
            } else {
                0
            };
            let values: Vec<Value> = match schema
                .columns
                .iter()
                .map(|col| match col.name.as_str() {
                    "_ts_system" if bitemporal => Ok(Value::Integer(sys_now)),
                    "_ts_valid_from" if bitemporal => Ok(match obj.get("_ts_valid_from") {
                        Some(Value::Integer(i)) => Value::Integer(*i),
                        _ => Value::Integer(i64::MIN),
                    }),
                    "_ts_valid_until" if bitemporal => Ok(match obj.get("_ts_valid_until") {
                        Some(Value::Integer(i)) => Value::Integer(*i),
                        _ => Value::Integer(i64::MAX),
                    }),
                    _ => ndb_field_to_value(obj.get(&col.name), &col.column_type),
                })
                .collect::<Result<Vec<Value>, crate::Error>>()
            {
                Ok(v) => v,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar insert coercion: {e}"),
                        },
                    );
                }
            };

            // Resolve the actual row to write (merged for ON CONFLICT DO
            // UPDATE, plain otherwise). This runs before the mutable
            // engine borrow needed by the insert call.
            let final_values: Vec<Value> = match intent {
                ColumnarInsertIntent::Put if !on_conflict_updates.is_empty() => {
                    let pk_bytes = {
                        let engine = match self.columnar_engines.get(&engine_key) {
                            Some(e) => e,
                            None => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: "columnar engine vanished during insert".into(),
                                    },
                                );
                            }
                        };
                        match engine.encode_pk_from_row(&values) {
                            Ok(b) => b,
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: format!("columnar insert: pk encode failed: {e}"),
                                    },
                                );
                            }
                        }
                    };

                    let prior_row = self
                        .columnar_engines
                        .get(&engine_key)
                        .and_then(|e| e.lookup_memtable_row_by_pk(&pk_bytes))
                        .or_else(|| self.read_flushed_row_by_pk(&engine_key, &pk_bytes));

                    match prior_row {
                        None => values,
                        Some(prior) => {
                            let existing_val = row_values_to_object(&schema, &prior);
                            let excluded_val = row_values_to_object(&schema, &values);
                            let merged = apply_on_conflict_updates(
                                existing_val,
                                &excluded_val,
                                on_conflict_updates,
                            );
                            let merged_obj = match merged {
                                nodedb_types::Value::Object(m) => m,
                                _ => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: "merged ON CONFLICT value was not an object"
                                                .into(),
                                        },
                                    );
                                }
                            };
                            match schema
                                .columns
                                .iter()
                                .map(|col| {
                                    ndb_field_to_value(merged_obj.get(&col.name), &col.column_type)
                                })
                                .collect::<Result<Vec<Value>, crate::Error>>()
                            {
                                Ok(v) => v,
                                Err(e) => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: format!("columnar ON CONFLICT coercion: {e}"),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
                _ => values,
            };

            let engine = match self.columnar_engines.get_mut(&engine_key) {
                Some(e) => e,
                None => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "columnar engine vanished during insert".into(),
                        },
                    );
                }
            };
            let row_surrogate = surrogates.get(row_idx).copied();
            let result = match intent {
                ColumnarInsertIntent::InsertIfAbsent => engine.insert_if_absent(&final_values),
                ColumnarInsertIntent::Insert | ColumnarInsertIntent::Put => match row_surrogate {
                    Some(s) => engine.insert_with_surrogate(&final_values, s),
                    None => engine.insert(&final_values),
                },
            };

            match result {
                Ok(_) => accepted += 1,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar insert failed: {e}"),
                        },
                    );
                }
            }
        }

        let engine = match self.columnar_engines.get_mut(&engine_key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar engine missing after insert loop".into(),
                    },
                );
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
                let kek = self.columnar_segment_kek.as_ref();
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
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "columnar segment encode failed, {row_count} rows not durable: {e}"
                                ),
                            },
                        );
                    }
                }
            }
            if let Err(e) = engine.on_memtable_flushed(new_segment_id) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar flush: segment ID counter exhausted: {e}"),
                    },
                );
            }
        }

        // Populate R-tree for geometry columns so spatial predicates work.
        {
            let db_id = task.request.database_id;
            let tid = task.request.tenant_id;
            let schema = engine.schema().clone();
            let geom_cols: Vec<usize> = schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.column_type == ColumnType::Geometry)
                .map(|(i, _)| i)
                .collect();

            if !geom_cols.is_empty() {
                for row in &ndb_rows {
                    let obj = match row {
                        nodedb_types::Value::Object(m) => m,
                        _ => continue,
                    };
                    let doc_id = obj
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if doc_id.is_empty() {
                        continue;
                    }
                    for &col_idx in &geom_cols {
                        let col_def = &schema.columns[col_idx];
                        let field_val = match obj.get(&col_def.name) {
                            Some(v) => v,
                            None => continue,
                        };
                        // Geometry may be stored as Value::Geometry or Value::String (GeoJSON).
                        let geom: nodedb_types::geometry::Geometry = match field_val {
                            Value::Geometry(g) => g.clone(),
                            Value::String(s) => match sonic_rs::from_str(s) {
                                Ok(g) => g,
                                Err(_) => continue,
                            },
                            _ => continue,
                        };
                        let bbox = nodedb_types::bbox::geometry_bbox(&geom);
                        let index_key = (db_id, tid, collection.to_string(), col_def.name.clone());
                        let entry_id = crate::util::fnv1a_hash(doc_id.as_bytes());
                        let rtree = self.spatial_indexes.entry(index_key.clone()).or_default();
                        rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                        self.spatial_doc_map.insert(
                            (
                                db_id,
                                tid,
                                collection.to_string(),
                                col_def.name.clone(),
                                entry_id,
                            ),
                            doc_id.clone(),
                        );
                    }
                }
            }
        }

        tracing::debug!(
            core = self.core_id,
            %collection,
            accepted,
            total = ndb_rows.len(),
            "columnar insert complete"
        );

        // Invalidate cached aggregate results for this collection so that
        // COUNT(*) and GROUP BY queries see the newly written rows.
        if accepted > 0 {
            let tid_key = task.request.tenant_id;
            let coll_prefix = format!("{collection}\0");
            self.aggregate_cache
                .retain(|(t, rest), _| !(*t == tid_key && rest.starts_with(&coll_prefix)));
        }

        self.checkpoint_coordinator
            .mark_dirty("columnar", accepted as usize);

        // On the sync path, advance the HWM and return SyncAckResult payload.
        if let Some(prov) = provenance {
            self.sync_commit(prov);
            let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
        }

        let result = serde_json::json!({
            "accepted": accepted,
            "collection": collection,
        });
        let json = match response_codec::encode_json(&result) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(json),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}
