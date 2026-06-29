// SPDX-License-Identifier: BUSL-1.1

//! Load-based automatic rebalancer.
//!
//! This module is the *signal* side of the rebalancer: given a
//! per-node snapshot of load metrics (vshards led, bytes stored,
//! writes/sec, reads/sec) it computes whether the cluster is
//! imbalanced enough to warrant moves, and emits a bounded plan of
//! vshard migrations from the hottest nodes to the coldest ones.
//!
//! The actual driver loop (`loop_driver.rs`) and the bridge to
//! `MigrationExecutor` land in a follow-up sub-batch. Everything
//! shipped here is pure, side-effect-free, and fully deterministic
//! so it can be unit-tested exhaustively before any tokio task is
//! spawned against it.
//!
//! ## Why a new module
//!
//! The existing [`crate::rebalance_scheduler::RebalanceScheduler`]
//! triggers on CPU utilization, SPSC queue pressure, and shard-count
//! ratio. Those are fast-path overload signals and belong where they
//! are. This module is a distinct, storage-shape-driven rebalancer
//! (bytes + qps + vshard count) with bounded in-flight moves and a
//! 30 s cadence, complementing the overload path.

pub mod driver;
pub mod elastic;
pub mod metrics;
pub mod placement;
pub mod plan;

pub use driver::{
    AlwaysReadyGate, ElectionGate, MigrationDispatcher, RebalancerLoop, RebalancerLoopConfig,
};
pub use elastic::RebalancerKickHook;
pub use metrics::{LoadMetrics, LoadMetricsProvider, LoadWeights, normalized_score};
pub use plan::{RebalancerPlanConfig, compute_load_based_plan};
