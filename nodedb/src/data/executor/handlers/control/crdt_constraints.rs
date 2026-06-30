// SPDX-License-Identifier: BUSL-1.1

//! CRDT constraint-install handlers.
//!
//! A committed `ConstraintChange` on the per-vshard data Raft log decodes to a
//! `SetConstraints` / `DropConstraints` op and lands here so every replica
//! installs the same constraint set into its per-core (`!Send`) CRDT validator,
//! keyed by collection. The installed set is in-memory: it is rebuilt on
//! restart from Raft-log replay of these entries.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Install a collection's constraint set into the tenant CRDT validator.
    ///
    /// Each blob is a zerompk-encoded `nodedb_crdt::Constraint`. Decode is
    /// loud: a malformed blob fails the whole op rather than silently dropping
    /// a constraint, which would weaken the invariant set on this replica.
    pub(in crate::data::executor) fn execute_crdt_set_constraints(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        constraints: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, count = constraints.len(), "crdt set constraints");
        let mut decoded = Vec::with_capacity(constraints.len());
        for blob in constraints {
            match zerompk::from_msgpack::<nodedb_crdt::Constraint>(blob) {
                Ok(c) => decoded.push(c),
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "crdt constraint decode failed");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("constraint decode failed: {e}"),
                        },
                    );
                }
            }
        }
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        engine.set_collection_constraints(collection, decoded);
        self.checkpoint_coordinator.mark_dirty("crdt", 1);
        self.response_ok(task)
    }

    /// Remove every constraint scoped to `collection` from the tenant CRDT
    /// validator.
    pub(in crate::data::executor) fn execute_crdt_drop_constraints(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "crdt drop constraints");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        engine.drop_collection_constraints(collection);
        self.checkpoint_coordinator.mark_dirty("crdt", 1);
        self.response_ok(task)
    }
}
