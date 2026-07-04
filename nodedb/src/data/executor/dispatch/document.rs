// SPDX-License-Identifier: BUSL-1.1

//! Document operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_mem;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::SystemTimeScope;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::scan_params::VersionedScanParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_document(&mut self, task: &ExecutionTask, op: &DocumentOp) -> Response {
        let tid = task.request.tenant_id.as_u64();
        // Pressure guard for write operations.
        let is_write = matches!(
            op,
            DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::BatchInsert { .. }
                | DocumentOp::BulkUpdate { .. }
                | DocumentOp::BulkDelete { .. }
                | DocumentOp::UpdateFromJoin { .. }
                | DocumentOp::Upsert { .. }
                | DocumentOp::InsertSelect { .. }
                | DocumentOp::BackfillIndex { .. }
                | DocumentOp::Merge { .. }
        );
        if is_write {
            if let Some(r) =
                self.check_engine_pressure(task, nodedb_mem::EngineId::DocumentSchemaless)
            {
                return r;
            }
            // FTS indexing is a side effect of every document write.
            if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Fts) {
                return r;
            }
        }
        match op {
            DocumentOp::PointGet {
                collection,
                document_id,
                surrogate,
                pk_bytes: _,
                rls_filters,
                system_time,
                valid_at_ms,
            } => {
                let system_as_of_ms = match system_time {
                    SystemTimeScope::Current => None,
                    SystemTimeScope::AsOf(ms) => Some(*ms),
                    SystemTimeScope::AllVersions => {
                        return self.response_error(
                            task,
                            crate::bridge::envelope::ErrorCode::Unsupported {
                                detail: "AS OF SYSTEM TIME NULL (all-versions) is not \
                                         supported on point gets; use a table scan"
                                    .into(),
                            },
                        );
                    }
                };
                self.execute_point_get(
                    task,
                    super::super::handlers::point::get::PointGetParams {
                        tid,
                        collection,
                        document_id,
                        surrogate: *surrogate,
                        rls_filters,
                        system_as_of_ms,
                        valid_at_ms: *valid_at_ms,
                    },
                )
            }

            DocumentOp::PointPut {
                collection,
                document_id,
                value,
                surrogate,
                pk_bytes: _,
            } => self.execute_point_put(task, tid, collection, document_id, *surrogate, value),

            DocumentOp::PointInsert {
                collection,
                document_id,
                value,
                if_absent,
                surrogate,
            } => self.execute_point_insert(
                crate::data::executor::handlers::point::insert::PointInsertParams {
                    task,
                    tid,
                    collection,
                    document_id,
                    surrogate: *surrogate,
                    value,
                    if_absent: *if_absent,
                },
            ),

            DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                returning,
                ..
            } => self.execute_point_delete(
                task,
                tid,
                collection,
                document_id,
                *surrogate,
                returning.as_ref(),
            ),

            DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                pk_bytes: _,
                updates,
                returning,
            } => self.execute_point_update(
                task,
                tid,
                collection,
                document_id,
                *surrogate,
                updates,
                returning.as_ref(),
            ),

            DocumentOp::Scan {
                collection,
                limit,
                offset,
                sort_keys,
                filters,
                distinct,
                projection,
                computed_columns,
                window_functions,
                system_time,
                valid_at_ms,
                prefilter,
            } => {
                if system_time.is_all_versions() {
                    let params = VersionedScanParams {
                        collection,
                        limit: *limit,
                        offset: *offset,
                        filters,
                        projection,
                        valid_at_ms: *valid_at_ms,
                    };
                    self.execute_document_scan_all_versions(task, tid, params)
                } else if system_time.is_temporal() || valid_at_ms.is_some() {
                    let params = VersionedScanParams {
                        collection,
                        limit: *limit,
                        offset: *offset,
                        filters,
                        projection,
                        valid_at_ms: *valid_at_ms,
                    };
                    self.execute_document_scan_as_of(task, tid, params, system_time.as_of_ms())
                } else {
                    self.execute_document_scan(
                        task,
                        tid,
                        collection,
                        *limit,
                        *offset,
                        sort_keys,
                        filters,
                        *distinct,
                        projection,
                        computed_columns,
                        window_functions,
                        prefilter.as_ref(),
                    )
                }
            }

            DocumentOp::BatchInsert {
                collection,
                documents,
                surrogates,
            } => self.execute_document_batch_insert(task, tid, collection, documents, surrogates),

            DocumentOp::RangeScan {
                collection,
                field,
                lower,
                upper,
                limit,
            } => self.execute_range_scan(
                task,
                tid,
                collection,
                field,
                lower.as_deref(),
                upper.as_deref(),
                *limit,
            ),

            DocumentOp::UpdateFromJoin {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                updates,
                target_filters,
                returning,
            } => self.execute_update_from_join(
                task,
                tid,
                super::super::handlers::update_from_join::UpdateFromJoinParams {
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    updates,
                    target_filter_bytes: target_filters,
                    returning: returning.as_ref(),
                },
            ),

            DocumentOp::BulkUpdate {
                collection,
                filters,
                updates,
                returning,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
            } => self.execute_bulk_update(
                task,
                tid,
                super::super::handlers::bulk_dml::BulkUpdateParams {
                    collection,
                    filter_bytes: filters,
                    updates,
                    returning: returning.as_ref(),
                    ollp_predicted_surrogates: ollp_predicted_surrogates.as_deref(),
                    ollp_predicted_edges: ollp_predicted_edges.as_deref(),
                },
            ),

            DocumentOp::BulkDelete {
                collection,
                filters,
                returning,
                ollp_predicted_surrogates,
                ollp_predicted_edges,
            } => self.execute_bulk_delete(
                task,
                tid,
                collection,
                filters,
                returning.as_ref(),
                crate::data::executor::handlers::bulk_dml::OllpPrediction {
                    surrogates: ollp_predicted_surrogates.as_deref(),
                    edges: ollp_predicted_edges.as_deref(),
                },
            ),

            DocumentOp::Upsert {
                collection,
                document_id,
                value,
                on_conflict_updates,
                surrogate,
            } => self.execute_upsert(
                task,
                tid,
                collection,
                document_id,
                *surrogate,
                value,
                on_conflict_updates,
            ),

            DocumentOp::Truncate { collection, .. } => self.execute_truncate(task, tid, collection),

            DocumentOp::EstimateCount { collection, field } => {
                self.execute_estimate_count(task, tid, collection, field)
            }

            DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_filters,
                source_limit,
            } => self.execute_insert_select(
                task,
                tid,
                target_collection,
                source_collection,
                source_filters,
                *source_limit,
            ),

            DocumentOp::Register {
                collection,
                indexes,
                crdt_enabled,
                storage_mode,
                enforcement,
                bitemporal,
            } => self.execute_register_document_collection(
                task,
                tid,
                collection,
                indexes,
                *crdt_enabled,
                storage_mode,
                enforcement,
                *bitemporal,
            ),

            DocumentOp::IndexLookup {
                collection,
                path,
                value,
            } => self.execute_document_index_lookup(task, tid, collection, path, value),

            DocumentOp::IndexedFetch {
                collection,
                path,
                value,
                filters,
                projection,
                limit,
                offset,
            } => self.execute_document_indexed_fetch(
                task,
                super::super::handlers::document::index_fetch::IndexedFetchParams {
                    tid,
                    collection,
                    path,
                    value,
                    filters,
                    projection,
                    limit: *limit,
                    offset: *offset,
                },
            ),

            DocumentOp::DropIndex { collection, field } => {
                self.execute_drop_document_index(task, tid, collection, field)
            }

            DocumentOp::Merge {
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                clauses,
                returning: _,
            } => self.execute_merge(
                task,
                tid,
                super::super::handlers::merge::MergeParams {
                    target_collection,
                    source_collection,
                    source_alias,
                    target_join_col,
                    source_join_col,
                    clauses,
                },
            ),

            DocumentOp::BackfillIndex {
                collection,
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
            } => self.execute_backfill_index(
                task,
                tid,
                collection,
                path,
                *is_array,
                *unique,
                *case_insensitive,
                predicate.as_deref(),
            ),

            DocumentOp::MaterializeScan {
                collection,
                cursor,
                count,
                system_as_of_ms,
            } => self.execute_document_materialize_scan(
                task,
                tid,
                collection,
                cursor,
                *count,
                *system_as_of_ms,
            ),
        }
    }
}
