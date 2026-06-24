// SPDX-License-Identifier: BUSL-1.1

//! Variable-length truncation resume queue for the cross-shard MATCH
//! coordinator.
//!
//! When a shard's variable-length expansion hits a hard cap it emits a
//! [`VarLenResume`] cursor instead of silently truncating. This module owns the
//! Control-Plane-side queue of those cursors: each cursor is routed back to the
//! node that owns its surviving frontier so a `MatchVarLenResume` plan can be
//! re-dispatched there to drain the remaining matches. The cursor type itself is
//! a nodedb-crate type (`VarLenResume`), so it lives here in the Control Plane
//! rather than in `nodedb-cluster`'s frontier-continuation coordinator.

use crate::control::gateway::RouteDecision;
use crate::control::server::graph_dispatch::cluster_resolve::resolve_for_vshard;
use crate::control::state::SharedState;
use crate::engine::graph::pattern::executor::VarLenResume;
use crate::types::VShardId;

/// A resume cursor paired with the resolved route to the node that owns its
/// surviving frontier. `remote_coords == None` means the owner is the local
/// node (dispatch via the all-cores broadcast); `Some((node_id, vshard_id))`
/// means a remote dispatch.
pub(super) struct PendingResume {
    /// `None` => local owner; `Some((node_id, vshard_id))` => remote owner.
    pub(super) remote_coords: Option<(u64, u64)>,
    /// The cursor to re-dispatch as a `MatchVarLenResume` plan.
    pub(super) resume: VarLenResume,
}

/// Resolve a single resume cursor to the node owning its surviving frontier and
/// build a [`PendingResume`].
///
/// The cursor's frontier names are owned by the cores of the node that emitted
/// it, so `VShardId::from_key(frontier.first().name)` resolves back to that node — a
/// `MatchVarLenResume` re-dispatched there continues the BFS where it capped.
///
/// A cursor with an EMPTY frontier is degenerate (nothing left to expand) and
/// yields `Ok(None)` so the caller skips it. `LeaderUnknown` / `Broadcast`
/// resolution surfaces as a typed error, exactly as the frontier-continuation
/// path does — a resume cursor is never silently dropped.
pub(super) fn resume_to_pending(
    state: &SharedState,
    resume: VarLenResume,
) -> crate::Result<Option<PendingResume>> {
    let Some((node_name, _path)) = resume.frontier.first() else {
        // Empty frontier: nothing left to resume.
        return Ok(None);
    };
    let target_vshard = VShardId::from_key(node_name.as_bytes()).as_u32();
    let remote_coords = match resolve_for_vshard(state, target_vshard) {
        RouteDecision::Local => None,
        RouteDecision::Remote { node_id, vshard_id } => Some((node_id, vshard_id)),
        RouteDecision::LeaderUnknown { vshard_id } => {
            return Err(crate::Error::NotLeader {
                vshard_id: VShardId::new((vshard_id % VShardId::COUNT as u64) as u32),
                leader_node: 0,
                leader_addr: String::new(),
            });
        }
        RouteDecision::Broadcast { .. } => {
            return Err(crate::Error::Internal {
                detail: "match scatter: resolve_for_vshard returned Broadcast for a \
                         single vShard"
                    .into(),
            });
        }
    };
    Ok(Some(PendingResume {
        remote_coords,
        resume,
    }))
}
