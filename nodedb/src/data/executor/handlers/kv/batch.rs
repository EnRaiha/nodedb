// SPDX-License-Identifier: BUSL-1.1

//! KV batch operation handlers: BatchGet, BatchPut.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvBatchPutParams;
use crate::engine::kv::current_ms;

/// Parameters for [`CoreLoop::execute_kv_batch_put`].
///
/// Bundles the plain data args so the method stays argument-count clean;
/// `task` (response routing) is passed separately since it isn't part of
/// the KV batch-put payload.
pub(in crate::data::executor) struct KvBatchPutArgs<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
    pub ttl_ms: u64,
    pub surrogates: &'a [nodedb_types::Surrogate],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_batch_get(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv batch get");
        let now_ms = current_ms();

        // Read-your-own-writes: consult this transaction's staging overlay
        // per key before falling back to base storage. `kv_overlay_body`
        // returns `None` immediately (no txn, or no overlay entry for this
        // key) so an autocommit `BatchGet` takes the exact base-only path it
        // always has.
        let results: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(
                |key| match self.kv_overlay_body(task, tid, collection, key) {
                    Some(overlay_result) => overlay_result,
                    None => self.kv_engine.get(did, tid, collection, key, now_ms),
                },
            )
            .collect();

        let json_results: Vec<serde_json::Value> = results
            .into_iter()
            .map(|opt| match opt {
                Some(v) => serde_json::Value::String(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &v,
                )),
                None => serde_json::Value::Null,
            })
            .collect();
        match response_codec::encode_json_vec(&json_results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_batch_put(
        &mut self,
        task: &ExecutionTask,
        args: KvBatchPutArgs<'_>,
    ) -> Response {
        let KvBatchPutArgs {
            did,
            tid,
            collection,
            entries,
            ttl_ms,
            surrogates,
        } = args;
        debug!(core = self.core_id, %collection, count = entries.len(), "kv batch put");
        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        let new_count = self.kv_engine.batch_put(KvBatchPutParams {
            database_id: did,
            tenant_id: tid,
            collection,
            entries,
            ttl_ms,
            now_ms,
            surrogates,
        });
        // One WAL record covers the whole batch; record every written key's
        // version against that single LSN.
        if task.wal_lsn().is_some() {
            for (key, _) in entries {
                self.note_kv_write_lsn(task, did, tid, collection, key);
            }
        }
        match response_codec::encode_count("inserted", new_count) {
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
