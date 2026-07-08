// SPDX-License-Identifier: BUSL-1.1

//! KV `DELETE` and `TRUNCATE` handlers.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_delete(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = keys.len(), "kv delete");
        let now_ms = current_ms();
        let count = self.kv_engine.delete(did, tid, collection, keys, now_ms);
        if let Some(ref m) = self.metrics {
            m.record_kv_delete();
        }

        // Emit delete events to Event Plane (one per deleted key).
        if count > 0 {
            for key in keys {
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Delete,
                    &key_str,
                    None,
                    None,
                );
            }
        }

        match response_codec::encode_count("deleted", count) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_truncate(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv truncate");
        let count = self.kv_engine.truncate(did, tid, collection);
        match response_codec::encode_count("deleted", count) {
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
