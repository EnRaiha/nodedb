// SPDX-License-Identifier: BUSL-1.1

//! KV TTL handlers: Expire, Persist, GetTtl.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl};
use crate::data::executor::handlers::transaction::stage_write::hex_key;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::types::TenantId;

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
            self.note_kv_write_lsn(task, did, tid, collection, key);
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
            self.note_kv_write_lsn(task, did, tid, collection, key);
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

        // Read-your-own-writes: an in-transaction GET_TTL consults this
        // transaction's staging overlay -- both the staged VALUE (for
        // tombstone / fresh-put visibility) and the staged KV TTL delta
        // (`StagedTtl`, populated by staged `Expire` / `Persist` / a
        // TTL-carrying `Incr` / `BatchPut`) -- before falling back to the
        // base KV engine.
        if let Some(txn_id) = task.request.txn_id {
            let coll_key = (
                task.request.database_id,
                TenantId::new(tid),
                collection.to_string(),
            );
            let doc_id = hex_key(key);
            if let Some(overlay) = self.txn_overlays.get(&txn_id) {
                let staged_value = overlay.get_by_doc_id(&coll_key, &doc_id);
                if matches!(staged_value, Some(Staged::Tombstone)) {
                    return self.kv_get_ttl_response(task, -2);
                }
                let staged_ttl = overlay.get_ttl_by_doc_id(&coll_key, &doc_id);
                match staged_ttl {
                    Some(StagedTtl::ExpireAt(expire_at_ms)) => {
                        let ttl_ms = if expire_at_ms <= now_ms {
                            -2 // Already expired: staged-absent.
                        } else {
                            (expire_at_ms - now_ms) as i64
                        };
                        return self.kv_get_ttl_response(task, ttl_ms);
                    }
                    Some(StagedTtl::Persist) => return self.kv_get_ttl_response(task, -1),
                    None => {
                        if matches!(staged_value, Some(Staged::Put(_))) {
                            // A fresh staged put with no TTL delta is
                            // persistent, matching a base PUT with
                            // `ttl_ms == 0`.
                            return self.kv_get_ttl_response(task, -1);
                        }
                        // Nothing staged for this key: fall through to base.
                    }
                }
            }
        }

        let ttl_ms = self
            .kv_engine
            .get_ttl_ms(did, tid, collection, key, now_ms)
            .unwrap_or(-2); // -2 = key does not exist.
        self.kv_get_ttl_response(task, ttl_ms)
    }

    fn kv_get_ttl_response(&self, task: &ExecutionTask, ttl_ms: i64) -> Response {
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
