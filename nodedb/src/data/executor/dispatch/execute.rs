// SPDX-License-Identifier: BUSL-1.1

//! `CoreLoop::execute` / `execute_plan`: matches on `PhysicalPlan` variant
//! and delegates to the appropriate per-engine sub-dispatcher.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::super::core_loop::CoreLoop;
use super::super::task::ExecutionTask;
use super::visitor;

impl CoreLoop {
    /// Execute a physical plan. Dispatches to the appropriate sub-dispatcher.
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        self.execute_plan(task, task.plan())
    }

    /// Execute an arbitrary physical plan (used for inline sub-plans in multi-way joins).
    pub(in crate::data::executor) fn execute_plan(
        &mut self,
        task: &ExecutionTask,
        plan: &PhysicalPlan,
    ) -> Response {
        let tid = task.request.tenant_id.as_u64();
        let mut v = visitor::DataPlaneVisitor {
            core_loop: self,
            task,
            tid,
        };
        match nodedb_physical::dispatch(&mut v, plan) {
            Ok(response) => response,
            Err(never) => match never {},
        }
    }
}
