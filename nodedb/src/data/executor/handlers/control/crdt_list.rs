// SPDX-License-Identifier: BUSL-1.1

//! CRDT block-list handlers: insert / delete / move a block within a
//! document's Loro list container.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Insert a block (LoroMap) into a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list insert");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
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
        let fields =
            match sonic_rs::from_str::<serde_json::Map<String, serde_json::Value>>(fields_json) {
                Ok(fields) => fields
                    .iter()
                    .map(|(key, value)| (key.clone(), super::convert::json_to_loro_value(value)))
                    .collect::<Vec<_>>(),
                // Preserve the existing list-insert contract: malformed optional
                // field JSON creates an empty block map rather than changing the
                // structural list mutation into a typed request failure.
                Err(_) => Vec::new(),
            };
        match engine.list_insert_fields(collection, document_id, list_path, index, &fields) {
            Ok(()) => self.response_ok(task),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: error.to_string(),
                },
            ),
        }
    }

    /// Delete a block from a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list delete");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
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
        match engine.list_delete(collection, document_id, list_path, index) {
            Ok(()) => self.response_ok(task),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: error.to_string(),
                },
            ),
        }
    }

    /// Move a block within a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_move(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, from_index, to_index, "crdt list move");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
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
        match engine.list_move(collection, document_id, list_path, from_index, to_index) {
            Ok(()) => self.response_ok(task),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: error.to_string(),
                },
            ),
        }
    }
}
