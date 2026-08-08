// SPDX-License-Identifier: BUSL-1.1

//! KV operation dispatch: routes `KvOp` variants to their handler methods.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

impl CoreLoop {
    /// Dispatch a KV operation to the appropriate handler.
    pub(in crate::data::executor) fn execute_kv(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        match op {
            KvOp::Get {
                collection,
                key,
                rls_filters,
                surrogate_ceiling,
            } => self.execute_kv_get(
                task,
                super::crud::KvGetParams {
                    did,
                    tid,
                    collection,
                    key,
                    rls_filters,
                    surrogate_ceiling: *surrogate_ceiling,
                },
            ),
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                returning,
                rls_filters,
            } => self.execute_kv_put(
                task,
                super::crud::KvWriteParams {
                    did,
                    tid,
                    collection,
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::Insert {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                returning,
                rls_filters,
            } => self.execute_kv_insert(
                task,
                super::crud::KvWriteParams {
                    did,
                    tid,
                    collection,
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::InsertIfAbsent {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
                returning,
                rls_filters,
            } => self.execute_kv_insert_if_absent(
                task,
                super::crud::KvWriteParams {
                    did,
                    tid,
                    collection,
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    surrogate: *surrogate,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::InsertOnConflictUpdate {
                collection,
                key,
                value,
                ttl_ms,
                updates,
                surrogate,
                rls_write_check,
                returning,
                rls_filters,
            } => self.execute_kv_insert_on_conflict_update(
                task,
                super::crud::KvInsertOnConflictUpdateParams {
                    did,
                    tid,
                    collection,
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    updates,
                    surrogate: *surrogate,
                    rls_write_check,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::Delete {
                collection,
                keys,
                rls_write_check,
            } => self.execute_kv_delete(task, did, tid, collection, keys, rls_write_check),
            KvOp::Scan {
                collection,
                cursor,
                count,
                filters,
                match_pattern,
                sort_keys,
                surrogate_ceiling,
            } => self.execute_kv_scan(
                task,
                super::scan::KvScanHandlerParams {
                    did,
                    tid,
                    collection,
                    cursor,
                    count: *count,
                    match_pattern: match_pattern.as_deref(),
                    filters,
                    sort_keys,
                    surrogate_ceiling: *surrogate_ceiling,
                },
            ),
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
                rls_write_check,
            } => self.execute_kv_expire(
                task,
                super::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection,
                    key,
                    rls_write_check,
                },
                *ttl_ms,
            ),
            KvOp::Persist {
                collection,
                key,
                rls_write_check,
            } => self.execute_kv_persist(
                task,
                super::ttl::KvTtlTarget {
                    did,
                    tid,
                    collection,
                    key,
                    rls_write_check,
                },
            ),
            KvOp::BatchGet {
                collection,
                keys,
                rls_filters,
            } => self.execute_kv_batch_get(task, did, tid, collection, keys, rls_filters),
            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
                surrogates,
                returning,
                rls_filters,
            } => self.execute_kv_batch_put(
                task,
                super::batch::KvBatchPutArgs {
                    did,
                    tid,
                    collection,
                    entries,
                    ttl_ms: *ttl_ms,
                    surrogates,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::RegisterIndex {
                collection,
                field,
                field_position,
                backfill,
            } => self.execute_kv_register_index(
                task,
                super::index::KvRegisterIndexParams {
                    did,
                    tid,
                    collection,
                    field,
                    field_position: *field_position,
                    backfill: *backfill,
                },
            ),
            KvOp::DropIndex { collection, field } => {
                self.execute_kv_drop_index(task, did, tid, collection, field)
            }
            KvOp::FieldGet {
                collection,
                key,
                fields,
                rls_filters,
            } => self.execute_kv_field_get(
                task,
                super::field::KvFieldGetArgs {
                    did,
                    tid,
                    collection,
                    key,
                    fields,
                    rls_filters,
                },
            ),
            KvOp::FieldSet {
                collection,
                key,
                updates,
                surrogate,
                rls_write_check,
            } => self.execute_kv_field_set(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                updates,
            ),
            KvOp::GetTtl { collection, key } => {
                self.execute_kv_get_ttl(task, did, tid, collection, key)
            }
            KvOp::Truncate { collection } => self.execute_kv_truncate(task, did, tid, collection),
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
                surrogate,
                rls_write_check,
            } => self.execute_kv_incr(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                *delta,
                *ttl_ms,
            ),
            KvOp::IncrFloat {
                collection,
                key,
                delta,
                surrogate,
                rls_write_check,
            } => self.execute_kv_incr_float(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                *delta,
            ),
            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
                surrogate,
                rls_write_check,
            } => self.execute_kv_cas(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                expected,
                new_value,
            ),
            KvOp::GetSet {
                collection,
                key,
                new_value,
                surrogate,
                rls_filters,
                rls_write_check,
            } => self.execute_kv_getset(
                super::atomic::KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection,
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                new_value,
                rls_filters,
            ),
            KvOp::RegisterSortedIndex {
                collection,
                index_name,
                sort_columns,
                key_column,
                window_type,
                window_timestamp_column,
                window_start_ms,
                window_end_ms,
            } => self.execute_kv_register_sorted_index(
                task,
                super::sorted::KvRegisterSortedIndexParams {
                    did,
                    tid,
                    collection,
                    index_name,
                    sort_columns,
                    key_column,
                    window_type,
                    window_timestamp_column,
                    window_start_ms: *window_start_ms,
                    window_end_ms: *window_end_ms,
                },
            ),
            KvOp::DropSortedIndex { index_name } => {
                self.execute_kv_drop_sorted_index(task, did, tid, index_name)
            }
            KvOp::SortedIndexRank {
                index_name,
                primary_key,
            } => self.execute_kv_sorted_index_rank(task, did, tid, index_name, primary_key),
            KvOp::SortedIndexTopK { index_name, k } => {
                self.execute_kv_sorted_index_top_k(task, did, tid, index_name, *k)
            }
            KvOp::SortedIndexRange {
                index_name,
                score_min,
                score_max,
            } => self.execute_kv_sorted_index_range(
                task,
                super::sorted::KvSortedIndexRangeParams {
                    did,
                    tid,
                    index_name,
                    score_min: score_min.as_deref(),
                    score_max: score_max.as_deref(),
                },
            ),
            KvOp::SortedIndexCount { index_name } => {
                self.execute_kv_sorted_index_count(task, did, tid, index_name)
            }
            KvOp::SortedIndexScore {
                index_name,
                primary_key,
            } => self.execute_kv_sorted_index_score(task, did, tid, index_name, primary_key),
            KvOp::Transfer {
                collection,
                source_key,
                dest_key,
                field,
                amount,
                debit_surrogate,
                credit_surrogate,
                rls_write_check,
            } => self.execute_kv_transfer(
                task,
                super::transfer::TransferParams {
                    did,
                    tid,
                    collection,
                    source_key,
                    dest_key,
                    field,
                    amount: *amount,
                    debit_surrogate: *debit_surrogate,
                    credit_surrogate: *credit_surrogate,
                    rls_write_check,
                },
            ),
            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate,
                source_rls_write_check,
                dest_rls_write_check,
            } => self.execute_kv_transfer_item(
                task,
                super::transfer::TransferItemParams {
                    did,
                    tid,
                    source_collection,
                    dest_collection,
                    item_key,
                    dest_key,
                    surrogate: *surrogate,
                    source_rls_write_check,
                    dest_rls_write_check,
                },
            ),
            KvOp::MaterializeScan {
                collection,
                cursor,
                count,
            } => self.execute_kv_materialize_scan(task, did, tid, collection, cursor, *count),
        }
    }
}
