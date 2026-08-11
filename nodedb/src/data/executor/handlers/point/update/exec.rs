// SPDX-License-Identifier: BUSL-1.1

//! The PointUpdate statement itself: read the row, decide, write, answer.
//!
//! Holds only the sequencing — read the current row, decide what the update
//! makes it, gate the write policy on that decision, persist, then re-index,
//! emit, and project. The two halves it delegates to are the ones with their
//! own rules: `post_image` computes bytes and may not touch storage, `persist`
//! touches storage and may not reinterpret bytes. Keeping the order here, in
//! one readable pass, is what makes the "nothing is written before the policy
//! gate" property checkable at a glance.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response, WriteSetEntry};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_doc;
use crate::data::executor::handlers::returning_rows;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use nodedb_physical::physical_plan::{ResolvedSumTarget, ReturningSpec, StorageMode, UpdateValue};
use nodedb_types::Surrogate;

use super::super::update_reindex_secondary::UpdateSecondaryReindex;
use super::persist::PointUpdatePersist;
use super::post_image::PointUpdateImage;

/// Parameters for `execute_point_update`.
pub(in crate::data::executor) struct PointUpdateParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled RLS read policy gating the `RETURNING` rows. Empty = no policy.
    pub rls_filters: &'a [u8],
    /// Compiled RLS write policy gating the PERSIST, decided against the
    /// post-update image. A separate slot from `rls_filters`: that one bounds
    /// what may be shown back, this one bounds what may be written. Empty = no
    /// write policy.
    pub rls_write_check: &'a [u8],
    /// Join-key VALUE → target row surrogate for every materialized-sum target
    /// this update may touch — both sides of a join-key change. Resolved on the
    /// Control Plane at plan time.
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_update(
        &mut self,
        task: &ExecutionTask,
        params: PointUpdateParams<'_>,
    ) -> Response {
        let PointUpdateParams {
            tid,
            collection,
            document_id,
            surrogate,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = params;
        let row_key = surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        debug!(
            core = self.core_id,
            %collection,
            %document_id,
            fields = updates.len(),
            has_returning = returning.is_some(),
            "point update"
        );

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // `Some` exactly when the collection stores Binary Tuples. Held (not
        // just a bool) because the RETURNING projection below has to decode the
        // re-encoded post-image, and the MessagePack decoder accepts a Binary
        // Tuple without erroring — it would return a document with none of the
        // row's real columns rather than fail.
        let strict_schema = self
            .doc_configs
            .get(&config_key)
            .and_then(|c| match &c.storage_mode {
                StorageMode::Strict { schema } => Some(schema.clone()),
                StorageMode::Schemaless => None,
            });
        let is_strict = strict_schema.is_some();

        // Reject direct updates to generated columns.
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = crate::data::executor::handlers::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )
        {
            return self.response_error(task, e);
        }

        // Refuse the statement outright on a collection that declared its rows
        // immutable. A hash-chained collection is refused here for the reason
        // its links exist: each link covers its predecessor's hash, so rewriting
        // a row makes `verify_chain` report the row AFTER it as broken, and the
        // tamper-evidence would accuse an untampered row.
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Err(e) = crate::data::executor::enforcement::append_only::check_point_update(
                collection,
                &config.enforcement,
            )
        {
            return self.response_error(task, e);
        }

        // Any non-literal assignment forces the slow decode→eval→re-encode path,
        // because we need the current document to evaluate against.
        let has_expr = updates
            .iter()
            .any(|(_, v)| matches!(v, UpdateValue::Expr(_)));

        let bitemporal = self.is_bitemporal(task.request.database_id.as_u64(), tid, collection);
        let sys_from_for_encode = if bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };
        let database_id = task.request.database_id.as_u64();
        let get_result = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, row_key)
        } else {
            self.sparse.get(database_id, tid, collection, row_key)
        };
        match get_result {
            Ok(Some(current_bytes)) => {
                let has_generated = self.doc_configs.get(&config_key).is_some_and(|c| {
                    !c.enforcement.generated_columns.is_empty()
                        && crate::data::executor::handlers::generated::needs_recomputation(
                            updates,
                            &c.enforcement.generated_columns,
                        )
                });

                let updated_bytes = match self.build_point_update_image(PointUpdateImage {
                    config_key: &config_key,
                    current_bytes: &current_bytes,
                    updates,
                    is_strict,
                    has_generated,
                    has_expr,
                    bitemporal,
                    sys_from_ms: sys_from_for_encode,
                }) {
                    Ok(bytes) => bytes,
                    Err(e) => return self.response_error(task, e),
                };

                // Gate the persist on the collection's write policy, decided
                // against the post-update image the row will actually hold.
                // Placed after the generated columns are recomputed — a policy
                // may reference one — and before any store or index is touched,
                // so a rejected row leaves nothing behind.
                if let Err(e) = rls_write_gate::admit_stored_row(
                    rls_write_check,
                    &updated_bytes,
                    document_id,
                    strict_schema.as_ref(),
                    tid,
                    collection,
                ) {
                    return self.response_error(task, e);
                }

                let write_result = self.persist_point_update(PointUpdatePersist {
                    config_key: &config_key,
                    database_id,
                    tid,
                    collection,
                    row_key,
                    current_bytes: &current_bytes,
                    updated_bytes: &updated_bytes,
                    bitemporal,
                    sys_from_ms: sys_from_for_encode,
                    wal_lsn: task.wal_lsn(),
                    resolved_sum_targets,
                });
                match write_result {
                    Ok(target_write_set) => {
                        self.doc_cache.put(
                            task.request.database_id.as_u64(),
                            tid,
                            collection,
                            row_key,
                            &updated_bytes,
                        );

                        let has_vectors = self.collection_has_vectors(database_id, tid, collection);
                        if let Err(e) =
                            self.update_reindex_vector_and_sparse(UpdateSecondaryReindex {
                                database_id,
                                tid,
                                collection,
                                row_key,
                                surrogate,
                                new_body: &updated_bytes,
                                is_strict,
                                has_vectors,
                            })
                        {
                            return self.response_error(task, e);
                        }

                        // Emit update event to Event Plane. `current_bytes`
                        // is the pre-update row already read above; the
                        // helper derives `WriteOp::Update` from the Some
                        // prior + Some new pair and handles strict→msgpack
                        // conversion on both sides.
                        self.emit_put_event(
                            task,
                            tid,
                            collection,
                            row_key,
                            &updated_bytes,
                            Some(&current_bytes),
                        );

                        // Build the response for both the RETURNING and
                        // non-RETURNING branches first, then — only when the
                        // collection carries a secondary vector index — carry the
                        // surrogate + post-image back in the write-set so the
                        // Control Plane can mint a post-apply `Put` redo record.
                        // The autocommit WAL path mints none for a PointUpdate, so
                        // without this a WAL-only restart rebuilds the HNSW from the
                        // pre-update body and resurrects the old embedding.
                        // `updated_bytes` is moved in as its last use.
                        let mut response = if let Some(spec) = returning {
                            // Post-update image, decoded in the collection's
                            // storage mode; the user-visible key only fills in
                            // as `id` when the row declares none of its own.
                            let doc = match returning_doc::from_stored(
                                &updated_bytes,
                                document_id,
                                strict_schema.as_ref(),
                            ) {
                                Ok(doc) => doc,
                                Err(e) => return self.response_error(task, e),
                            };
                            match returning_rows::build_rows_payload(spec, rls_filters, &[doc]) {
                                Ok(payload) => self.response_with_payload(task, payload),
                                Err(e) => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: format!("RETURNING encode: {e}"),
                                        },
                                    );
                                }
                            }
                        } else {
                            let mut payload = Vec::with_capacity(16);
                            nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
                            nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", 1);
                            self.response_with_payload(task, payload)
                        };
                        if has_vectors {
                            response.write_set = vec![WriteSetEntry {
                                surrogate: surrogate.as_u32(),
                                is_delete: false,
                                value: updated_bytes,
                                collection: None,
                            }];
                        }
                        // Derived target rows live in a DIFFERENT collection
                        // than this statement's, so each carries its own
                        // `Some(collection)` and homes to that collection's
                        // vShard. Appended rather than replacing: the row's own
                        // vector redo above and these are both required.
                        response.write_set.extend(target_write_set);
                        response
                    }
                    Err(e) => self.response_error(task, e),
                }
            }
            Ok(None) => {
                let mut payload = Vec::with_capacity(16);
                nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
                nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", 0);
                self.response_with_payload(task, payload)
            }
            Err(e) => self.response_error(task, e),
        }
    }
}
