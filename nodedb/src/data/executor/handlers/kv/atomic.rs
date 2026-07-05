// SPDX-License-Identifier: BUSL-1.1

//! KV atomic operation handlers: Incr, IncrFloat, Cas, GetSet.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::AtomicError;
use crate::engine::kv::current_ms;

/// Shared identity context for a single-key KV atomic operation
/// (INCR_FLOAT / GETSET) dispatched to this core.
pub(in crate::data::executor) struct KvAtomicCtx<'a> {
    pub(in crate::data::executor) task: &'a ExecutionTask,
    pub(in crate::data::executor) did: u64,
    pub(in crate::data::executor) tid: u64,
    pub(in crate::data::executor) collection: &'a str,
    pub(in crate::data::executor) key: &'a [u8],
    pub(in crate::data::executor) surrogate: nodedb_types::Surrogate,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_incr(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        delta: i64,
        ttl_ms: u64,
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
        } = ctx;
        debug!(core = self.core_id, %collection, delta, "kv incr");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        match self.kv_engine.incr(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            delta,
            ttl_ms,
        ) {
            Ok(new_value) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let new_bytes = zerompk::to_msgpack_vec(&new_value).unwrap_or_default();
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    &key_str,
                    Some(&new_bytes),
                    None,
                );
                match response_codec::encode_json(&serde_json::json!({ "value": new_value })) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(AtomicError::TypeMismatch { detail }) => self.response_error(
                task,
                ErrorCode::TypeMismatch {
                    collection: collection.to_string(),
                    detail,
                },
            ),
            Err(AtomicError::Overflow) => self.response_error(
                task,
                ErrorCode::OverflowError {
                    collection: collection.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_incr_float(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        delta: f64,
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
        } = ctx;
        debug!(core = self.core_id, %collection, delta, "kv incr_float");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        match self.kv_engine.incr_float(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            delta,
        ) {
            Ok(new_value) => {
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let new_bytes = zerompk::to_msgpack_vec(&new_value).unwrap_or_default();
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection,
                    crate::event::WriteOp::Update,
                    &key_str,
                    Some(&new_bytes),
                    None,
                );
                match response_codec::encode_json(&serde_json::json!({ "value": new_value })) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(AtomicError::TypeMismatch { detail }) => self.response_error(
                task,
                ErrorCode::TypeMismatch {
                    collection: collection.to_string(),
                    detail,
                },
            ),
            Err(AtomicError::Overflow) => self.response_error(
                task,
                ErrorCode::OverflowError {
                    collection: collection.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_kv_cas(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        expected: &[u8],
        new_value: &[u8],
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv cas");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        let result = self.kv_engine.cas(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            expected,
            new_value,
        );

        if result.success {
            if let Some(ref m) = self.metrics {
                m.record_kv_put();
            }
            let key_str = String::from_utf8_lossy(key);
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Update,
                &key_str,
                Some(new_value),
                None,
            );
        }

        let current_b64 = result
            .current_value
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        match response_codec::encode_json(&serde_json::json!({
            "success": result.success,
            "current_value": current_b64,
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

    pub(in crate::data::executor) fn execute_kv_getset(
        &mut self,
        ctx: KvAtomicCtx<'_>,
        new_value: &[u8],
    ) -> Response {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
        } = ctx;
        debug!(core = self.core_id, %collection, "kv getset");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms: u64 = self
            .epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms);
        let old = self.kv_engine.getset(
            crate::engine::kv::AtomicKeyCtx {
                database_id: did,
                tenant_id: tid,
                collection,
                key,
                now_ms,
                surrogate,
            },
            new_value,
        );

        if let Some(ref m) = self.metrics {
            m.record_kv_put();
        }
        let key_str = String::from_utf8_lossy(key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            &key_str,
            Some(new_value),
            old.as_deref(),
        );

        let old_b64 = old
            .as_ref()
            .map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v));
        match response_codec::encode_json(&serde_json::json!({ "old_value": old_b64 })) {
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
