// SPDX-License-Identifier: BUSL-1.1

//! Entry point for a CRDT delta apply: routes to the sync-gated peer path or
//! the local (SQL / native client) path.
//!
//! The two differ in more than plumbing. The gated path answers a *sender* that
//! is holding the write until it hears back, so every outcome has to be
//! expressed as a disposition that sender can act on — apply, retry, or
//! compensate. The local path has no such sender, so a refusal is simply an
//! error returned to the caller.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::params::CrdtApplyParams;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_crdt_apply(
        &mut self,
        task: &ExecutionTask,
        params: CrdtApplyParams<'_>,
    ) -> Response {
        match params.provenance {
            Some(provenance) => self.apply_crdt_sync_gated(task, params, provenance),
            None => self.apply_crdt_local(task, params),
        }
    }
}
