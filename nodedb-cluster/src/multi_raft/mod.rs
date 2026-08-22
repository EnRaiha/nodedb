// SPDX-License-Identifier: BUSL-1.1

//! Multi-Raft coordinator: owns every Raft group hosted on this node,
//! routes inbound RPCs, and surfaces aggregated `Ready` output to the
//! tick loop.
//!
//! Split across files:
//! - [`core`]: struct, constructors, group lifecycle (`add_group`,
//!   `add_group_as_learner`), tick pipeline, routing accessors,
//!   observability (`group_statuses`).
//! - [`rpc_dispatch`]: inbound RPC routing to the correct group and the
//!   corresponding response handlers.
//! - [`conf_change`]: `propose_conf_change` / `apply_conf_change` with
//!   proper voter / learner / promotion semantics.
//! - [`membership`]: learner catch-up / promotion helpers
//!   (`commit_index_for`, `ready_learners`, etc.) driven by the tick loop.
//! - [`read_index`]: leadership confirmation for linearizable reads.

pub mod conf_change;
pub mod core;
pub mod membership;
pub mod read_index;
pub mod rpc_dispatch;

pub use core::{GroupStatus, MultiRaft, MultiRaftReady};
