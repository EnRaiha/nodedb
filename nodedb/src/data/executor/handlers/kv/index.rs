// SPDX-License-Identifier: BUSL-1.1

//! KV secondary index handlers: RegisterIndex, DropIndex.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_kv_register_index(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        field: &str,
        field_position: usize,
        backfill: bool,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, "kv register index");
        let now_ms = current_ms();
        let backfilled = self.kv_engine.register_index(
            did,
            tid,
            collection,
            field,
            field_position,
            backfill,
            now_ms,
        );
        match response_codec::encode_json(&serde_json::json!({
            "index": field,
            "backfilled": backfilled,
            "write_amp_estimate": format!("{:.0}%", 15.0 + 10.0 * self.kv_engine.index_count(did, tid, collection) as f64),
        })) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_drop_index(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, "kv drop index");
        let removed = self.kv_engine.drop_index(did, tid, collection, field);
        match response_codec::encode_json(&serde_json::json!({
            "index": field,
            "entries_removed": removed,
        })) {
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
