// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handlers for synonym group meta operations.
//!
//! - `MetaOp::PutSynonymGroup`    — persist a synonym group to the FTS backend
//! - `MetaOp::DeleteSynonymGroup` — remove a synonym group from the FTS backend

use nodedb_fts::SynonymGroupRecord;
use nodedb_types::TenantId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `MetaOp::PutSynonymGroup`: write a synonym group to the FTS backend.
    pub(in crate::data::executor) fn execute_put_synonym_group(
        &self,
        task: &ExecutionTask,
        tenant_id: u64,
        record_json: &str,
    ) -> Response {
        let record: SynonymGroupRecord = match sonic_rs::from_str(record_json) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("put_synonym_group: deserialize: {e}"),
                    },
                );
            }
        };

        let tid = TenantId::new(tenant_id);
        let database_id = task.request.database_id.as_u64();
        if let Err(e) = self.inverted.put_synonym_group(database_id, tid, &record) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("put_synonym_group: fts backend: {e}"),
                },
            );
        }

        self.response_ok(task)
    }

    /// Handle `MetaOp::DeleteSynonymGroup`: remove a synonym group from the FTS backend.
    pub(in crate::data::executor) fn execute_delete_synonym_group(
        &self,
        task: &ExecutionTask,
        tenant_id: u64,
        name: &str,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let database_id = task.request.database_id.as_u64();
        if let Err(e) = self.inverted.delete_synonym_group(database_id, tid, name) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("delete_synonym_group: fts backend: {e}"),
                },
            );
        }
        self.response_ok(task)
    }
}
