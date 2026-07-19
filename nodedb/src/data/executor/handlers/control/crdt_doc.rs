// SPDX-License-Identifier: BUSL-1.1

//! CRDT document-row handlers: field-carrying upsert / delete for SQL DML on
//! `crdt='true'` document collections. The Data Plane builds the Loro mutation
//! server-side, then materializes the merged row into the sparse store with
//! `EventSource::User` + text indexing so scans, secondary/spatial/vector
//! indexes, AFTER triggers, and CDC all observe it.

use loro::LoroValue;
use tracing::debug;

use nodedb_types::Surrogate;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_delete::PointDeleteParams;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;

impl CoreLoop {
    /// Insert-or-replace (`partial = false`) or partial-merge (`partial = true`)
    /// a document row's scalar fields, server-built from `fields_json`.
    pub(in crate::data::executor) fn execute_crdt_doc_upsert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        fields_json: &str,
        surrogate: Surrogate,
        partial: bool,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, partial, "crdt doc upsert");
        let tenant_id = task.request.tenant_id;
        let Ok(json_map) =
            sonic_rs::from_str::<serde_json::Map<String, serde_json::Value>>(fields_json)
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("crdt doc upsert: invalid fields_json for {document_id}"),
                },
            );
        };
        let fields: Vec<(&str, LoroValue)> = json_map
            .iter()
            .map(|(k, v)| (k.as_str(), super::convert::json_to_loro_value(v)))
            .collect();

        let materialized = {
            let engine = match self.get_crdt_engine(tenant_id) {
                Ok(e) => e,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };
            let res = if partial {
                engine.doc_set_fields(collection, document_id, &fields)
            } else {
                engine.doc_upsert(collection, document_id, &fields)
            };
            if let Err(e) = res {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
            if surrogate != Surrogate::ZERO {
                Self::encode_crdt_row(engine, collection, document_id)
            } else {
                None
            }
        };

        if let Some(bytes) = materialized {
            self.materialize_document_write(
                task,
                tenant_id.as_u64(),
                collection,
                surrogate,
                &bytes,
                true,
            );
        }
        self.checkpoint_coordinator.mark_dirty("crdt", 1);
        self.response_ok(task)
    }

    /// Delete a document row: tombstone in the collection's Loro doc, then
    /// remove it from the sparse store with the full index cascade + CDC delete
    /// event (mirrors the point-delete apply path with `enforce = false`, since
    /// the write was already admitted on its origin).
    pub(in crate::data::executor) fn execute_crdt_doc_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        surrogate: Surrogate,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt doc delete");
        let tenant_id = task.request.tenant_id;
        {
            let engine = match self.get_crdt_engine(tenant_id) {
                Ok(e) => e,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };
            if let Err(e) = engine.doc_delete(collection, document_id) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        }

        let tid = tenant_id.as_u64();
        let storage_key = surrogate_to_doc_id(surrogate);
        let outcome = match self.apply_point_delete(PointDeleteParams {
            database_id: task.request.database_id.as_u64(),
            tid,
            collection,
            document_id: storage_key.as_str(),
            surrogate,
            user_roles: &task.request.user_roles,
            enforce: false,
        }) {
            Ok(outcome) => outcome,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Emit the delete to the Event Plane only when a row was actually
        // removed, threading the pre-delete bytes through as `old_value` so
        // CDC/change-stream consumers observe the prior state.
        if let Some(prior_bytes) = outcome.prior_value.as_deref() {
            let old_converted = self.resolve_event_payload(tid, collection, prior_bytes);
            self.emit_write_event(
                task,
                collection,
                crate::event::WriteOp::Delete,
                storage_key.as_str(),
                None,
                Some(old_converted.as_deref().unwrap_or(prior_bytes)),
            );
        }

        self.checkpoint_coordinator.mark_dirty("crdt", 1);
        self.response_ok(task)
    }
}
