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
                did,
                tid,
                collection,
                key,
                rls_filters,
                *surrogate_ceiling,
            ),
            KvOp::Put {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
            } => self.execute_kv_put(task, did, tid, collection, key, value, *ttl_ms, *surrogate),
            KvOp::Insert {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
            } => {
                self.execute_kv_insert(task, did, tid, collection, key, value, *ttl_ms, *surrogate)
            }
            KvOp::InsertIfAbsent {
                collection,
                key,
                value,
                ttl_ms,
                surrogate,
            } => self.execute_kv_insert_if_absent(
                task, did, tid, collection, key, value, *ttl_ms, *surrogate,
            ),
            KvOp::InsertOnConflictUpdate {
                collection,
                key,
                value,
                ttl_ms,
                updates,
                surrogate,
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
                },
            ),
            KvOp::Delete { collection, keys } => {
                self.execute_kv_delete(task, did, tid, collection, keys)
            }
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
            } => self.execute_kv_expire(task, did, tid, collection, key, *ttl_ms),
            KvOp::Persist { collection, key } => {
                self.execute_kv_persist(task, did, tid, collection, key)
            }
            KvOp::BatchGet { collection, keys } => {
                self.execute_kv_batch_get(task, did, tid, collection, keys)
            }
            KvOp::BatchPut {
                collection,
                entries,
                ttl_ms,
            } => self.execute_kv_batch_put(task, did, tid, collection, entries, *ttl_ms),
            KvOp::RegisterIndex {
                collection,
                field,
                field_position,
                backfill,
            } => self.execute_kv_register_index(
                task,
                did,
                tid,
                collection,
                field,
                *field_position,
                *backfill,
            ),
            KvOp::DropIndex { collection, field } => {
                self.execute_kv_drop_index(task, did, tid, collection, field)
            }
            KvOp::FieldGet {
                collection,
                key,
                fields,
            } => self.execute_kv_field_get(task, did, tid, collection, key, fields),
            KvOp::FieldSet {
                collection,
                key,
                updates,
            } => self.execute_kv_field_set(task, did, tid, collection, key, updates),
            KvOp::GetTtl { collection, key } => {
                self.execute_kv_get_ttl(task, did, tid, collection, key)
            }
            KvOp::Truncate { collection } => self.execute_kv_truncate(task, did, tid, collection),
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
            } => self.execute_kv_incr(task, did, tid, collection, key, *delta, *ttl_ms),
            KvOp::IncrFloat {
                collection,
                key,
                delta,
            } => self.execute_kv_incr_float(task, did, tid, collection, key, *delta),
            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
            } => self.execute_kv_cas(task, did, tid, collection, key, expected, new_value),
            KvOp::GetSet {
                collection,
                key,
                new_value,
            } => self.execute_kv_getset(task, did, tid, collection, key, new_value),
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
                did,
                tid,
                index_name,
                score_min.as_deref(),
                score_max.as_deref(),
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
                },
            ),
            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
            } => self.execute_kv_transfer_item(
                task,
                did,
                tid,
                source_collection,
                dest_collection,
                item_key,
                dest_key,
            ),
            KvOp::MaterializeScan {
                collection,
                cursor,
                count,
            } => self.execute_kv_materialize_scan(task, did, tid, collection, cursor, *count),
        }
    }
}
