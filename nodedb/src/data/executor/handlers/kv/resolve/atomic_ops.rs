// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the KV atomics: `Incr`, `IncrFloat`, `Cas`, `GetSet`. Each
//! post-image comes from `engine_atomic_compute`, the same pure functions
//! `KvEngine::{incr, incr_float, cas, getset}` call — recomputing here would
//! let resolve and apply disagree.

use nodedb_physical::physical_plan::KvResolveOutcome;

use super::context::{ResolveResult, ResolvedPut, expiry_from_ttl, one, put_mutation};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::atomic::KvAtomicCtx;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::response_codec;
use crate::engine::kv::current_ms;
use crate::engine::kv::engine_atomic_compute as compute;

/// Render a stored body for the `current_value` / `old_value` slot of an
/// atomic's reply, exactly as the live handlers do.
fn base64_body(body: Option<&[u8]>) -> Option<String> {
    body.map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v))
}

/// Translate an `AtomicError` into the `ErrorCode` the live handler returns
/// for it.
fn atomic_error_code(error: crate::engine::kv::AtomicError, collection: &str) -> ErrorCode {
    match error {
        crate::engine::kv::AtomicError::TypeMismatch { detail } => ErrorCode::TypeMismatch {
            collection: collection.to_string(),
            detail,
        },
        crate::engine::kv::AtomicError::Overflow => ErrorCode::OverflowError {
            collection: collection.to_string(),
        },
        crate::engine::kv::AtomicError::Encode { detail } => ErrorCode::Internal { detail },
        // The gate is consulted out here, not inside the engine, so the
        // engine's own rejection path is unreachable from a compute call.
        crate::engine::kv::AtomicError::Rejected(error) => (*error).into(),
    }
}

impl CoreLoop {
    /// Resolve `INCR`. `ttl_ms > 0` installs a fresh expiry; `ttl_ms == 0`
    /// preserves the existing one, matching `atomic_put` (not `KvEngine::put`).
    pub(super) fn resolve_kv_incr(
        &self,
        ctx: KvAtomicCtx<'_>,
        delta: i64,
        ttl_ms: u64,
    ) -> ResolveResult {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        let now_ms = self.kv_ttl_now_ms(task);
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let (new_value, new_bytes) = compute::incr(current.as_deref(), delta)
            .map_err(|e| atomic_error_code(e, collection))?;
        admit_kv_row(rls_write_check, &new_bytes, key, tid, collection)?;

        let expire_at_ms = if ttl_ms > 0 {
            expiry_from_ttl(ttl_ms, now_ms)
        } else {
            self.kv_resolve_preserved_expiry(did, tid, collection, key)
        };
        let response_payload =
            response_codec::encode_json_as_msgpack(&serde_json::json!({ "value": new_value }))?;
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: new_bytes,
                ttl_ms,
                expire_at_ms,
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }

    /// Resolve `INCR_FLOAT`. Always preserves the key's existing TTL, the
    /// same `ttl_ms = 0` call `KvEngine::incr_float` makes.
    pub(super) fn resolve_kv_incr_float(&self, ctx: KvAtomicCtx<'_>, delta: f64) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        let now_ms = self.kv_atomic_now_ms();
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let (new_value, new_bytes) = compute::incr_float(current.as_deref(), delta)
            .map_err(|e| atomic_error_code(e, collection))?;
        admit_kv_row(rls_write_check, &new_bytes, key, tid, collection)?;

        let response_payload =
            response_codec::encode_json_as_msgpack(&serde_json::json!({ "value": new_value }))?;
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: new_bytes,
                ttl_ms: 0,
                expire_at_ms: self.kv_resolve_preserved_expiry(did, tid, collection, key),
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }

    /// Resolve `CAS`. A mismatch resolves to zero mutations and a reply
    /// reporting failure; it still goes through the propose loop as a no-op
    /// entry so replicas agree on a statement that wrote nothing.
    pub(super) fn resolve_kv_cas(
        &self,
        ctx: KvAtomicCtx<'_>,
        expected: &[u8],
        new_value: &[u8],
    ) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        // Decided before the swap, same as `execute_kv_cas`: `new_value` is
        // caller-supplied, so the post-swap row is known up front.
        admit_kv_row(rls_write_check, new_value, key, tid, collection)?;

        let now_ms = self.kv_atomic_now_ms();
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let (matches, write_bytes) = compute::cas(current.as_deref(), expected, new_value);

        let response_payload = response_codec::encode_json_as_msgpack(&serde_json::json!({
            "success": matches,
            "current_value": base64_body(current.as_deref()),
        }))?;

        if !matches {
            return Ok(KvResolveOutcome {
                mutations: Vec::new(),
                response_payload,
            });
        }
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: write_bytes,
                ttl_ms: 0,
                expire_at_ms: self.kv_resolve_preserved_expiry(did, tid, collection, key),
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }

    /// Resolve `GETSET`. The old value is gated by `rls_filters` here — a row
    /// the policy hides comes back absent, matching `execute_kv_getset`.
    pub(super) fn resolve_kv_getset(
        &self,
        ctx: KvAtomicCtx<'_>,
        new_value: &[u8],
        rls_filters: &[u8],
    ) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        admit_kv_row(rls_write_check, new_value, key, tid, collection)?;

        let now_ms = self.kv_atomic_now_ms();
        let old = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let write_bytes = compute::getset(old.as_deref(), new_value);

        let disclosable_old = match &old {
            Some(bytes) => match self.row_passes_rls(bytes, rls_filters) {
                Ok(true) => old.as_deref(),
                Ok(false) => None,
                Err(e) => {
                    return Err(ErrorCode::Internal {
                        detail: e.to_string(),
                    });
                }
            },
            None => None,
        };
        let response_payload = response_codec::encode_json_as_msgpack(
            &serde_json::json!({ "old_value": base64_body(disclosable_old) }),
        )?;

        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: write_bytes,
                ttl_ms: 0,
                expire_at_ms: self.kv_resolve_preserved_expiry(did, tid, collection, key),
                surrogate,
                precondition: old,
            }),
            response_payload,
        ))
    }

    /// The instant `execute_kv_incr_float` / `execute_kv_cas` /
    /// `execute_kv_getset` read for expiry evaluation.
    fn kv_atomic_now_ms(&self) -> u64 {
        self.epoch_system_ms
            .map(|ms| ms as u64)
            .unwrap_or_else(current_ms)
    }
}
