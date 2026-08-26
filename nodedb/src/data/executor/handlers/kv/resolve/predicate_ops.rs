// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the KV predicate writes: `PredicateUpdate`,
//! `PredicateDelete`. Each reads via the same [`CoreLoop::kv_predicate_matches`]
//! scan the live handler uses, computes each post-image with the same merge,
//! and reports the mutations instead of applying them.

use nodedb_types::{RlsWriteCheck, Surrogate};

use super::context::{ResolveResult, ResolvedPut, delete_mutation, put_mutation};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::field_compute::merge_field_updates;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::response_codec;
use crate::engine::kv::current_ms;
use nodedb_physical::physical_plan::KvResolveOutcome;

impl CoreLoop {
    /// Resolve a predicate `UPDATE`. Each matched row's stored body becomes
    /// its mutation's `precondition`, so a moved-past resolution applies nothing.
    pub(super) fn resolve_kv_predicate_update(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        filters: &[u8],
        updates: &[(String, Vec<u8>)],
        rls_write_check: &RlsWriteCheck,
    ) -> ResolveResult {
        let now_ms = current_ms();
        let matched = self.kv_predicate_matches(did, tid, collection, filters, now_ms)?;

        let mut mutations = Vec::with_capacity(matched.len());
        for (key, body) in matched {
            let computed = merge_field_updates(Some(body.as_slice()), updates)?;
            admit_kv_row(rls_write_check, &computed.new_value, &key, tid, collection)?;
            mutations.push(put_mutation(ResolvedPut {
                collection,
                key: &key,
                value: computed.new_value,
                // `execute_kv_predicate_update` writes with `ttl_ms: 0`, the
                // keyed field merge's behaviour. Preserved verbatim.
                ttl_ms: 0,
                expire_at_ms: 0,
                // The row exists, so its bound surrogate must survive the
                // merge — `ZERO` leaves it alone.
                surrogate: Surrogate::ZERO,
                precondition: Some(body),
            }));
        }

        let response_payload = response_codec::encode_count("affected", mutations.len())?;
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Resolve a predicate `DELETE`. Counts and replies exactly as
    /// `resolve_kv_delete` does for a keyed one.
    pub(super) fn resolve_kv_predicate_delete(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        filters: &[u8],
        rls_write_check: &RlsWriteCheck,
    ) -> ResolveResult {
        let now_ms = current_ms();
        let matched = self.kv_predicate_matches(did, tid, collection, filters, now_ms)?;

        let mut mutations = Vec::with_capacity(matched.len());
        for (key, body) in matched {
            admit_kv_row(rls_write_check, &body, &key, tid, collection)?;
            mutations.push(delete_mutation(collection, &key, Some(body)));
        }

        let response_payload = response_codec::encode_count("deleted", mutations.len())?;
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }
}
