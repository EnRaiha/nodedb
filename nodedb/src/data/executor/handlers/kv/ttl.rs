// SPDX-License-Identifier: BUSL-1.1

//! KV TTL handlers: Expire, Persist, GetTtl.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_expire(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
        ttl_ms: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, ttl_ms, "kv expire");
        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        if self
            .kv_engine
            .expire(did, tid, collection, key, ttl_ms, now_ms)
        {
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    pub(in crate::data::executor) fn execute_kv_persist(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv persist");
        if self.kv_engine.persist(did, tid, collection, key) {
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }

    pub(in crate::data::executor) fn execute_kv_get_ttl(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "kv get_ttl");
        let now_ms = current_ms();
        let ttl_ms = self
            .kv_engine
            .get_ttl_ms(did, tid, collection, key, now_ms)
            .unwrap_or(-2); // -2 = key does not exist.
        match response_codec::encode_json(&serde_json::json!({ "ttl_ms": ttl_ms })) {
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
