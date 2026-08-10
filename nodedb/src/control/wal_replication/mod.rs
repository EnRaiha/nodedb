// SPDX-License-Identifier: BUSL-1.1

//! Distributed WAL write path — propose writes through Raft, apply after commit.
//!
//! Split into:
//! - [`types`]: `ReplicatedWrite`, `ReplicatedEntry`, `RaftProposer`, `RaftCompactor`,
//!   `RaftAppliedIndexSink`, variant defaults.
//! - [`encode`]: `to_replicated_entry` (PhysicalPlan → ReplicatedEntry).
//! - [`decode`]: `from_replicated_entry` (bytes → PhysicalPlan) + internal conversions.

pub mod decode;
mod decode_sync_engines;
pub mod encode;
mod legacy_entry;
pub mod propose;
pub mod types;

pub use decode::from_replicated_entry;
pub use encode::to_replicated_entry;
pub(crate) use propose::propose_replicated_entry;
pub use types::{
    AsyncRaftProposer, ConstraintChangeOp, RaftAppliedIndexSink, RaftCompactor, RaftProposer,
    ReplicatedEntry, ReplicatedSumTarget, ReplicatedWrite,
};

pub use crate::control::distributed_applier::{
    AppliedWrite, ApplyBatch, DistributedApplier, ProposeResult, ProposeTracker,
    create_distributed_applier, run_apply_loop,
};

#[cfg(test)]
mod kv_ttl_tests;
#[cfg(test)]
mod tests;
