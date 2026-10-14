// SPDX-License-Identifier: BUSL-1.1

//! Multi-node cluster orchestration.
//!
//! [`TestCluster`] + [`ClusterSpawnConfig`] type definitions live in
//! [`types`]; spawn-variant convenience wrappers in [`spawn_variants`];
//! the bringup body in [`bringup`]; the readiness barriers in [`ready`];
//! the apply-convergence barriers in [`convergence`];
//! the authorization-lease barrier in [`lease`];
//! the in-place restart in [`restart`]; post-spawn membership + DDL helpers
//! in [`membership`]; data-group leadership moves and waits in
//! [`leadership`].

mod bringup;
mod convergence;
mod leadership;
mod lease;
mod membership;
mod ready;
mod restart;
mod spawn_variants;
mod types;

pub use convergence::GroupPosition;
pub use leadership::GroupLeader;
pub use restart::{StoppedCluster, StoppedMember, StoppedNodeInfo};
pub use types::TestCluster;
pub(crate) use types::{ClusterSpawnConfig, DEFAULT_NUM_GROUPS};
