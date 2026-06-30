// SPDX-License-Identifier: BUSL-1.1

//! CRDT operation dispatch.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::CrdtOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_crdt(&mut self, task: &ExecutionTask, op: &CrdtOp) -> Response {
        match op {
            CrdtOp::Read {
                collection,
                document_id,
            } => self.execute_crdt_read(task, collection, document_id),

            CrdtOp::Apply {
                collection,
                document_id: _,
                delta,
                peer_id: _,
                mutation_id: _,
                surrogate: _,
                provenance,
            } => self.execute_crdt_apply(task, collection, delta, provenance.as_ref()),

            CrdtOp::ImportSnapshot {
                tenant_id,
                collection,
                bytes,
            } => self.execute_crdt_import_snapshot(task, *tenant_id, collection, bytes),

            CrdtOp::SetPolicy {
                collection,
                policy_json,
            } => self.execute_set_collection_policy(task, collection, policy_json),

            CrdtOp::GetPolicy { collection } => {
                self.execute_get_collection_policy(task, collection)
            }

            CrdtOp::SetConstraints {
                collection,
                descriptor_version,
                constraints,
            } => self.execute_crdt_set_constraints(
                task,
                collection,
                *descriptor_version,
                constraints,
            ),

            CrdtOp::DropConstraints {
                collection,
                descriptor_version,
            } => self.execute_crdt_drop_constraints(task, collection, *descriptor_version),

            CrdtOp::ReadConstraints { collection } => {
                self.execute_crdt_read_constraints(task, collection)
            }

            CrdtOp::ReadAtVersion {
                collection,
                document_id,
                version_vector_json,
            } => self.execute_crdt_read_at_version(
                task,
                collection,
                document_id,
                version_vector_json,
            ),

            CrdtOp::GetVersionVector { collection } => {
                self.execute_crdt_get_version_vector(task, collection)
            }

            CrdtOp::ExportDelta {
                collection,
                from_version_json,
            } => self.execute_crdt_export_delta(task, collection, from_version_json),

            CrdtOp::RestoreToVersion {
                collection,
                document_id,
                target_version_json,
                surrogate: _,
            } => self.execute_crdt_restore(task, collection, document_id, target_version_json),

            CrdtOp::CompactAtVersion {
                collection,
                target_version_json,
            } => self.execute_crdt_compact(task, collection, target_version_json),

            CrdtOp::ListInsert {
                collection,
                document_id,
                list_path,
                index,
                fields_json,
                surrogate: _,
            } => self.execute_crdt_list_insert(
                task,
                collection,
                document_id,
                list_path,
                *index,
                fields_json,
            ),

            CrdtOp::ListDelete {
                collection,
                document_id,
                list_path,
                index,
                surrogate: _,
            } => self.execute_crdt_list_delete(task, collection, document_id, list_path, *index),

            CrdtOp::ListMove {
                collection,
                document_id,
                list_path,
                from_index,
                to_index,
                surrogate: _,
            } => self.execute_crdt_list_move(
                task,
                collection,
                document_id,
                list_path,
                *from_index,
                *to_index,
            ),
        }
    }
}
