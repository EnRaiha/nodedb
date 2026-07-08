// SPDX-License-Identifier: BUSL-1.1

//! Inbound Raft RPC dispatch — `impl RaftRpcHandler for RaftLoop`.
//!
//! Each RPC variant is either handled inline (Raft consensus RPCs that
//! just lock `MultiRaft`) or delegated to a helper module — health,
//! forwarding, VShard envelopes, or (for `JoinRequest`) the async
//! orchestration in [`super::join`].
//!
//! Split by concern:
//! - [`consensus`]: AppendEntries / RequestVote / InstallSnapshot RPC
//!   bodies and the TimeoutNow election trigger.
//! - [`membership`]: the join-leader decision (`JoinDecision`,
//!   `decide_join`, `TOPOLOGY_GROUP_ID`), Ping, and TopologyUpdate.
//! - [`plan_dispatch`]: physical-plan execution + metadata/data propose
//!   forwarding + VShardEnvelope routing.
//! - [`shuffle_calvin`]: cross-node shuffle + Calvin submit RPC bodies.
//! - [`dispatch`]: the single `impl RaftRpcHandler for RaftLoop` block —
//!   thin, each method delegates to one of the helpers above.

mod consensus;
mod dispatch;
mod membership;
mod plan_dispatch;
mod shuffle_calvin;

#[cfg(test)]
mod tests;

pub(super) use membership::{JoinDecision, TOPOLOGY_GROUP_ID, decide_join};
