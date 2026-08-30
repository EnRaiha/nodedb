// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT orchestrator logic.
//!
//! Validates a backup envelope, merges all sections into a single
//! `TenantDataSnapshot`, then splits the merged snapshot into per-node
//! sub-snapshots according to the *current* cluster topology and
//! dispatches `MetaOp::RestoreTenantSnapshot` to each owning node.
//!
//! Durable re-issue of columnar/timeseries/vector rows lives in [`reissue`];
//! post-install surrogate rebinding and tombstone warnings live in
//! [`rebind`].

mod rebind;
mod reissue;
mod restore;
mod stats;

pub use restore::restore_tenant;
pub use stats::RestoreStats;
