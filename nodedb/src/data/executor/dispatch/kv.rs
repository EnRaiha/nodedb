// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for KvOp variants: engine pressure check, refusal of an unbound
//! row write, then delegation to execute_kv.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::KvOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_kv(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        let is_kv_write = matches!(
            op,
            KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Delete { .. }
                | KvOp::BatchPut { .. }
                | KvOp::Expire { .. }
                | KvOp::FieldSet { .. }
                | KvOp::Incr { .. }
                | KvOp::IncrFloat { .. }
                | KvOp::Cas { .. }
                | KvOp::GetSet { .. }
                | KvOp::Transfer { .. }
                | KvOp::TransferItem { .. }
                | KvOp::PredicateUpdate { .. }
                | KvOp::PredicateDelete { .. }
        );
        if is_kv_write && let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Kv) {
            return r;
        }
        if let Some(refusal) =
            crate::data::executor::handlers::kv::unbound::refuse_unbound_kv_write(op)
        {
            return self.response_error(task, refusal);
        }
        self.execute_kv(task, did, tid, op)
    }
}
