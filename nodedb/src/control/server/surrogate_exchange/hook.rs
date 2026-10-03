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
//! `SurrogateAssigner::lookup_bound`, which never allocates and never writes. A miss
//! comes back as `found: Some(false)` with no error — the key names no
//! existing row, which is an answer, not a failure. Allocating on that path
//! will mint identity for a row that does not exist.
//!
//! Otherwise, on `on_assign_surrogate`, the leader:
//! 1. runs `authority::assign_at_home`: the binding its catalog holds, or a
//!    fresh value bound through the home vShard's Raft log, read back after
//!    it applies;
//! 2. because the leader leads the key's collection home, that value is the
//!    AUTHORITATIVE surrogate every owner stores under: first-wins, idempotent,
//!    the same one every coordinator that routes here will receive;
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
        // The request carries the bare catalog name.
        let key = nodedb_types::CollectionKey::from_bare(database_id, &req.collection);

        if req.lookup_only == Some(true) {
            return match self
                .state
                .surrogate_assigner
                .lookup_bound(key, tenant_id, &req.pk)
            {
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

        match super::authority::assign_at_home(
            &self.state,
            crate::types::VShardId::new(req.vshard_id),
            key,
            tenant_id,
            &req.pk,
        )
        .await
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
