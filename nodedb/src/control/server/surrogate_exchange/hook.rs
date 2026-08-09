// SPDX-License-Identifier: BUSL-1.1

//! `RegistryAssignRemoteSurrogate` — bridges the cluster `AssignSurrogate`
//! trigger to a node-local `SurrogateAssigner::assign` (F1b).
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the assign logic
//! lives here and is exposed to the transport via the
//! [`nodedb_cluster::AssignRemoteSurrogate`] hook. The `RaftLoop` is built
//! `with_assign_remote_surrogate(Arc::new(RegistryAssignRemoteSurrogate { .. }))`.
//!
//! # What it does
//!
//! This handler only ever runs on the home vShard's LEADER (the coordinator
//! routes the `AssignSurrogateRequest` there precisely so the assign is local to
//! the data's home node).
//!
//! A request carrying `lookup_only: Some(true)` takes the READ-ONLY branch:
//! `SurrogateAssigner::lookup`, which never allocates and never writes. A miss
//! comes back as `found: Some(false)` with no error — the key simply names no
//! existing row, which is an answer, not a failure. Allocating on that path
//! would mint identity for a row that does not exist.
//!
//! Otherwise, on `on_assign_surrogate`, the leader:
//! 1. runs `SurrogateAssigner::assign(database_id, tenant_id, collection, pk)` —
//!    a LOCAL assign that allocates from this node's HiLo batch on the first call
//!    and returns the persisted binding on every later call;
//! 2. because the leader IS the home node, that value is the AUTHORITATIVE
//!    surrogate the home node stores under: first-wins, idempotent, the same one
//!    every coordinator that routes here will receive;
//! 3. maps `Ok(surrogate)` → [`AssignSurrogateResponse`] with `error: None` and
//!    `Err` → a typed [`TypedClusterError::Internal`] (surrogate `0`) — never a
//!    silent drop.
//!
//! # Plane discipline
//!
//! This runs on the leader's Control Plane (the Tokio transport reactor). The
//! `SurrogateAssigner` is a synchronous `Send + Sync` facade; the call neither
//! touches storage I/O / io_uring directly nor spawns a Data-Plane task.

use std::sync::Arc;

use nodedb_cluster::{AssignSurrogateRequest, AssignSurrogateResponse, TypedClusterError};

use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// `nodedb`-side implementation of [`nodedb_cluster::AssignRemoteSurrogate`].
///
/// Holds the node's [`SharedState`] so it can reach the `SurrogateAssigner`. The
/// assign runs against THIS node's allocator; the coordinator only routes here
/// when this node is the endpoint key's home vShard leader, so the local value is
/// the authoritative one.
pub struct RegistryAssignRemoteSurrogate {
    /// Shared node state — the source of the local `SurrogateAssigner`.
    state: Arc<SharedState>,
}

impl RegistryAssignRemoteSurrogate {
    /// Build an assigner hook over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::AssignRemoteSurrogate for RegistryAssignRemoteSurrogate {
    async fn on_assign_surrogate(&self, req: AssignSurrogateRequest) -> AssignSurrogateResponse {
        let database_id = DatabaseId::from(req.database_id);
        let tenant_id = TenantId::new(req.tenant_id);

        if req.lookup_only == Some(true) {
            return match self.state.surrogate_assigner.lookup(
                database_id,
                tenant_id,
                &req.collection,
                &req.pk,
            ) {
                Ok(Some(surrogate)) => AssignSurrogateResponse {
                    surrogate: surrogate.as_u32(),
                    error: None,
                    found: Some(true),
                },
                Ok(None) => AssignSurrogateResponse {
                    surrogate: 0,
                    error: None,
                    found: Some(false),
                },
                Err(e) => AssignSurrogateResponse {
                    surrogate: 0,
                    error: Some(TypedClusterError::Internal {
                        code: 0,
                        message: format!("assign-remote-surrogate local lookup failed: {e}"),
                    }),
                    found: None,
                },
            };
        }

        match self
            .state
            .surrogate_assigner
            .assign(database_id, tenant_id, &req.collection, &req.pk)
        {
            Ok(surrogate) => AssignSurrogateResponse {
                surrogate: surrogate.as_u32(),
                error: None,
                found: None,
            },
            Err(e) => AssignSurrogateResponse {
                surrogate: 0,
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: format!("assign-remote-surrogate local assign failed: {e}"),
                }),
                found: None,
            },
        }
    }
}
