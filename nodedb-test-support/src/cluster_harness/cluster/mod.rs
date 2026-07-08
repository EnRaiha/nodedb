// SPDX-License-Identifier: BUSL-1.1

//! Multi-node cluster orchestration.
//!
//! [`TestCluster`] + [`ClusterSpawnConfig`] type definitions live in
//! [`types`]; spawn-variant convenience wrappers in [`spawn_variants`];
//! the heavy bringup/convergence body in [`bringup`]; post-spawn
//! membership + DDL helpers in [`membership`].

mod bringup;
mod membership;
mod spawn_variants;
mod types;

pub(crate) use types::ClusterSpawnConfig;
pub use types::TestCluster;
