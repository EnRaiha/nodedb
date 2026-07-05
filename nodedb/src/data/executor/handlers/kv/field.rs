// SPDX-License-Identifier: BUSL-1.1

//! KV field-level operation handlers: FieldGet, FieldSet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_field_get(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
        fields: &[String],
    ) -> Response {
        debug!(core = self.core_id, %collection, field_count = fields.len(), "kv field get");
        let now_ms = current_ms();

        // Read-your-own-writes: consult this transaction's staging overlay
        // before falling back to base storage (see `execute_kv_batch_get`).
        let value = match self.kv_overlay_body(task, tid, collection, key) {
            Some(overlay_result) => overlay_result,
            None => self.kv_engine.get(did, tid, collection, key, now_ms),
        };
        let Some(value) = value else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Decode as standard msgpack map.
        let doc = match nodedb_types::json_from_msgpack(&value) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "value is not a msgpack-encoded object".into(),
                    },
                );
            }
        };

        // Extract requested fields.
        let mut result = serde_json::Map::new();
        for f in fields {
            let v = doc.get(f).cloned().unwrap_or(serde_json::Value::Null);
            result.insert(f.clone(), v);
        }

        match response_codec::encode_json(&serde_json::Value::Object(result)) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_field_set(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        key: &[u8],
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, field_count = updates.len(), "kv field set");
        let now_ms = current_ms();

        // Read current value.
        let current = self.kv_engine.get(did, tid, collection, key, now_ms);

        // Merge field updates via the pure computation shared with the
        // in-transaction staging handler (`stage_kv_transfer.rs`), so a
        // staged value and its COMMIT-time durable replay never diverge.
        let computed = match super::field_compute::merge_field_updates(current.as_deref(), updates)
        {
            Ok(c) => c,
            Err(e) => return self.response_error(task, e),
        };

        self.kv_engine.put(
            did,
            tid,
            collection,
            key,
            &computed.new_value,
            0,
            now_ms,
            nodedb_types::Surrogate::ZERO,
        );
        match response_codec::encode_json(
            &serde_json::json!({ "fields_added": computed.fields_added }),
        ) {
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
