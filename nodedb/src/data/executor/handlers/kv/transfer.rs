// SPDX-License-Identifier: BUSL-1.1

//! Atomic transfer handlers: Transfer (fungible) and TransferItem (non-fungible).
//!
//! These execute entirely within a single TPC core pass — no TOCTOU race.
//! Read + validate + write happens atomically because the TPC core is
//! single-threaded and owns all keys in its hash table.

use tracing::debug;

use super::transfer_compute::{TransferError, compute_transfer};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;

/// Parameters for an atomic fungible transfer.
pub(in crate::data::executor) struct TransferParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub source_key: &'a [u8],
    pub dest_key: &'a [u8],
    pub field: &'a str,
    pub amount: f64,
}

impl CoreLoop {
    /// Atomic fungible transfer: source.field -= amount, dest.field += amount.
    ///
    /// Entire read-validate-write is one Data Plane pass. No TOCTOU.
    pub(in crate::data::executor) fn execute_kv_transfer(
        &mut self,
        task: &ExecutionTask,
        params: TransferParams<'_>,
    ) -> Response {
        let TransferParams {
            did,
            tid,
            collection,
            source_key,
            dest_key,
            field,
            amount,
        } = params;
        debug!(core = self.core_id, %collection, %field, amount, "kv transfer");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();

        // Step 1: Read both values atomically (same core, no interleaving).
        let source_val = self.kv_engine.get(did, tid, collection, source_key, now_ms);
        let dest_val = self.kv_engine.get(did, tid, collection, dest_key, now_ms);

        let Some(source_bytes) = source_val else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Step 2 + 3: validate + compute new values via the pure computation
        // shared with the in-transaction staging handler
        // (`stage_kv_transfer.rs`), so a staged pair of writes and their
        // COMMIT-time durable replay never diverge.
        let dest_bytes = dest_val.unwrap_or_default();
        let dest_ref = if dest_bytes.is_empty() {
            None
        } else {
            Some(dest_bytes.as_slice())
        };
        let computed = match compute_transfer(&source_bytes, dest_ref, field, amount) {
            Ok(c) => c,
            Err(TransferError::TypeMismatch(detail)) => {
                return self.response_error(
                    task,
                    ErrorCode::TypeMismatch {
                        collection: collection.to_string(),
                        detail,
                    },
                );
            }
            Err(TransferError::InsufficientBalance { have, need }) => {
                return self.response_error(
                    task,
                    ErrorCode::InsufficientBalance {
                        collection: collection.to_string(),
                        detail: format!("source has {have}, need {need}"),
                    },
                );
            }
        };
        let new_source = computed.new_source;
        let new_dest = computed.new_dest;
        let source_balance_after = computed.source_balance_after;
        let dest_balance_after = computed.dest_balance_after;

        // Step 4: Write both atomically (deterministic order for consistency).
        // Write lower key first to match the documented lock ordering.
        if source_key <= dest_key {
            self.kv_engine.put(
                did,
                tid,
                collection,
                source_key,
                &new_source,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
            self.kv_engine.put(
                did,
                tid,
                collection,
                dest_key,
                &new_dest,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
        } else {
            self.kv_engine.put(
                did,
                tid,
                collection,
                dest_key,
                &new_dest,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
            self.kv_engine.put(
                did,
                tid,
                collection,
                source_key,
                &new_source,
                0,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
        }

        if let Some(ref m) = self.metrics {
            m.record_kv_put();
            m.record_kv_put();
        }

        // Emit CDC events.
        let src_str = String::from_utf8_lossy(source_key);
        let dst_str = String::from_utf8_lossy(dest_key);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            &src_str,
            Some(&new_source),
            Some(&source_bytes),
        );
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Update,
            &dst_str,
            Some(&new_dest),
            if dest_bytes.is_empty() {
                None
            } else {
                Some(&dest_bytes)
            },
        );

        match response_codec::encode_json(&serde_json::json!({
            "source_key": src_str,
            "dest_key": dst_str,
            "field": field,
            "amount": amount,
            "source_balance": source_balance_after,
            "dest_balance": dest_balance_after,
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

    /// Atomic non-fungible item transfer: verify + delete + insert in one pass.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_kv_transfer_item(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        source_collection: &str,
        dest_collection: &str,
        item_key: &[u8],
        dest_key: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %source_collection, %dest_collection, "kv transfer item");

        if self.kv_engine.is_over_budget() {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();

        // Step 1: Verify source owns the item.
        let Some(item_data) = self
            .kv_engine
            .get(did, tid, source_collection, item_key, now_ms)
        else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Step 2: Delete from source, insert at dest — atomic (single core).
        self.kv_engine
            .delete(did, tid, source_collection, &[item_key.to_vec()], now_ms);
        self.kv_engine.put(
            did,
            tid,
            dest_collection,
            dest_key,
            &item_data,
            0,
            now_ms,
            nodedb_types::Surrogate::ZERO,
        );

        if let Some(ref m) = self.metrics {
            m.record_kv_delete();
            m.record_kv_put();
        }

        // Emit CDC events.
        let item_str = String::from_utf8_lossy(item_key);
        let dest_str = String::from_utf8_lossy(dest_key);
        self.emit_write_event(
            task,
            source_collection,
            crate::event::WriteOp::Delete,
            &item_str,
            None,
            Some(&item_data),
        );
        self.emit_write_event(
            task,
            dest_collection,
            crate::event::WriteOp::Insert,
            &dest_str,
            Some(&item_data),
            None,
        );

        match response_codec::encode_json(&serde_json::json!({
            "item_key": item_str,
            "dest_key": dest_str,
            "source_collection": source_collection,
            "dest_collection": dest_collection,
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
