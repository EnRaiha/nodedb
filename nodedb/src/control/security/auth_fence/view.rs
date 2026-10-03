// SPDX-License-Identifier: BUSL-1.1

//! The read of authorization state that every planning path takes.
//!
//! Planning uses local state only, and every check here is local:
//!
//! 1. Tree-definition changes the metadata applier queued move into the
//!    permission cache.
//! 2. A stale cache reloads from this node's own cores: no reload covers it
//!    yet (startup, or a tree definition changed), or a core lost an event.
//!    A writer's acknowledgement never reloads, so a write covered only by a
//!    reload binds this statement through this step.
//! 3. A tenant whose tree rows live in a Raft group this node does not
//!    replicate is refused: this node never covers that group.
//! 4. In a cluster, the node must hold a valid authorization lease. A writer
//!    acknowledges an authorization change only after every lease holder
//!    covered it or its lease expired, so a valid lease means the state read
//!    here holds every change acknowledged before this point. A node that
//!    leads the metadata group as its only voter holds a pinned lease, which
//!    never expires: every barrier waits for its coverage instead.
//!
//! A lease lapses when one renewal round runs long, though the next round
//! is a renewal interval away. Before it takes the cache guard, a statement
//! waits up to the lease's lapse grace for a round to grant it again. The
//! wait holds no guard, so the leader's own floor load can take the cache's
//! write lock meanwhile. The check under the guard then decides.
//!
//! The lease is checked after the cache guard is taken, so the guarded cache
//! holds every change acknowledged before the check. A change acknowledged
//! after the check was acknowledged after the statement started planning.
//! The cache only moves forward, so a later read of it holds the fenced
//! state or newer. A planning path that awaits a request (a surrogate at its
//! collection home) runs the fence with [`admit_permission_view`], holds no
//! guard across that request, and reads the live cache once it resumes.

use std::time::Instant;

use tokio::sync::RwLockReadGuard;

use crate::control::security::auth_lease::{lease_status, planning_admitted_within};
use crate::control::security::permission_tree::{PermissionCache, reload};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::cluster::{behind, group_of_vshard, hosts_group};

/// Read the permission cache for planning a statement of `tenant_id`.
pub async fn permission_view(
    state: &SharedState,
    tenant_id: TenantId,
) -> crate::Result<RwLockReadGuard<'_, PermissionCache>> {
    apply_committed_tree_defs(state).await;
    reload::reload_if_stale(state).await?;

    if let Some(timing) = state.authorization_fence.timing() {
        // The check under the guard below decides. This wait only lets a
        // round that is about to renew finish first.
        planning_admitted_within(state, timing.lapse_grace()).await;
    }
    let cache = state.permission_cache.read().await;
    if state.cluster_routing.is_some() && cache.has_tree_defs_for_tenant(tenant_id.as_u64()) {
        for source in cache
            .tree_sources()
            .into_iter()
            .filter(|source| source.key.scope.tenant_id == tenant_id.as_u64())
        {
            let group_id = group_of_vshard(state, source.key.vshard().as_u32())?;
            if !hosts_group(state, group_id) {
                return Err(behind(format!(
                    "this node does not replicate raft group {group_id}, which homes \
                     permission source '{}'; run the statement on a node that does",
                    source.key.qualified()
                )));
            }
        }
    }
    if !lease_status(state, Instant::now()).admits_planning() {
        return Err(behind(
            "this node holds no valid authorization lease; it has not confirmed the latest \
             authorization changes",
        ));
    }
    Ok(cache)
}

/// Run the fence [`permission_view`] runs for a statement of `tenant_id`,
/// and release the view. The statement's planning reads the live cache after
/// its last await.
pub async fn admit_permission_view(state: &SharedState, tenant_id: TenantId) -> crate::Result<()> {
    permission_view(state, tenant_id).await.map(drop)
}

/// Move the tree-definition changes the metadata applier committed into the
/// cache. The applier queues each change before it advances the applied
/// index, so the queue holds every change this node applied.
pub(crate) async fn apply_committed_tree_defs(state: &SharedState) {
    let pending = state.authorization_fence.tree_defs();
    if pending.is_empty() {
        return;
    }
    let mut cache = state.permission_cache.write().await;
    pending.apply_to(&mut cache);
}
