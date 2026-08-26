// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the two atomic transfers: `Transfer` (fungible balance move)
//! and `TransferItem` (non-fungible row move between two collections). Both
//! decide every governing policy before producing a single mutation — a
//! transfer is one write, so a rejection on either side resolves neither half.

use nodedb_physical::physical_plan::KvResolveOutcome;

use super::context::{ResolveResult, ResolvedPut, delete_mutation, put_mutation};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::handlers::kv::transfer::{TransferItemParams, TransferParams};
use crate::data::executor::handlers::kv::transfer_compute::{TransferError, compute_transfer};
use crate::data::executor::response_codec;
use crate::engine::kv::current_ms;

impl CoreLoop {
    /// Resolve an atomic fungible `Transfer`.
    ///
    /// The two puts are emitted in ascending key order, the lock ordering
    /// `execute_kv_transfer` documents and follows.
    pub(super) fn resolve_kv_transfer(&self, params: TransferParams<'_>) -> ResolveResult {
        let TransferParams {
            did,
            tid,
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            rls_write_check,
        } = params;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        let Some(source_bytes) = self.kv_resolve_read(did, tid, collection, source_key, now_ms)
        else {
            return Err(ErrorCode::NotFound);
        };
        let dest_bytes = self.kv_resolve_read(did, tid, collection, dest_key, now_ms);

        let computed = compute_transfer(
            &source_bytes,
            dest_bytes.as_deref().filter(|b| !b.is_empty()),
            field,
            amount,
        )
        .map_err(|e| match e {
            TransferError::TypeMismatch(detail) => ErrorCode::TypeMismatch {
                collection: collection.to_string(),
                detail,
            },
            TransferError::InsufficientBalance { have, need } => ErrorCode::InsufficientBalance {
                collection: collection.to_string(),
                detail: format!("source has {have}, need {need}"),
            },
        })?;

        admit_kv_row(
            rls_write_check,
            &computed.new_source,
            source_key,
            tid,
            collection,
        )?;
        admit_kv_row(
            rls_write_check,
            &computed.new_dest,
            dest_key,
            tid,
            collection,
        )?;

        // `execute_kv_transfer` puts both rows with `ttl_ms: 0`, clearing any
        // TTL either held. Preserved verbatim.
        let debit = put_mutation(ResolvedPut {
            collection,
            key: source_key,
            value: computed.new_source,
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: debit_surrogate,
            precondition: Some(source_bytes),
        });
        let credit = put_mutation(ResolvedPut {
            collection,
            key: dest_key,
            value: computed.new_dest,
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: credit_surrogate,
            precondition: dest_bytes,
        });
        let mutations = if source_key <= dest_key {
            vec![debit, credit]
        } else {
            vec![credit, debit]
        };

        let response_payload = response_codec::encode_json_as_msgpack(&serde_json::json!({
            "source_key": String::from_utf8_lossy(source_key),
            "dest_key": String::from_utf8_lossy(dest_key),
            "field": field,
            "amount": amount,
            "source_balance": computed.source_balance_after,
            "dest_balance": computed.dest_balance_after,
        }))?;
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Resolve an atomic non-fungible `TransferItem`. The row leaving the
    /// source and the row arriving at the destination are two different
    /// images governed by two independent collections — both decided here.
    pub(super) fn resolve_kv_transfer_item(&self, params: TransferItemParams<'_>) -> ResolveResult {
        let TransferItemParams {
            did,
            tid,
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            source_rls_write_check,
            dest_rls_write_check,
        } = params;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        let Some(item_data) = self.kv_resolve_read(did, tid, source_collection, item_key, now_ms)
        else {
            return Err(ErrorCode::NotFound);
        };
        admit_kv_row(
            source_rls_write_check,
            &item_data,
            item_key,
            tid,
            source_collection,
        )?;
        admit_kv_row(
            dest_rls_write_check,
            &item_data,
            dest_key,
            tid,
            dest_collection,
        )?;

        // The destination is read only to pin its drift precondition: the live
        // handler overwrites whatever is there, and so does the apply.
        let dest_existing = self.kv_resolve_read(did, tid, dest_collection, dest_key, now_ms);

        let response_payload = response_codec::encode_json_as_msgpack(&serde_json::json!({
            "item_key": String::from_utf8_lossy(item_key),
            "dest_key": String::from_utf8_lossy(dest_key),
            "source_collection": source_collection,
            "dest_collection": dest_collection,
        }))?;

        Ok(KvResolveOutcome {
            mutations: vec![
                delete_mutation(source_collection, item_key, Some(item_data.clone())),
                put_mutation(ResolvedPut {
                    collection: dest_collection,
                    key: dest_key,
                    value: item_data,
                    ttl_ms: 0,
                    expire_at_ms: 0,
                    surrogate,
                    precondition: dest_existing,
                }),
            ],
            response_payload,
        })
    }
}
