// SPDX-License-Identifier: BUSL-1.1

//! Raft event loop — drives MultiRaft ticks and dispatches messages over the transport.
//!
//! Split across files:
//! - [`loop_core`]: `RaftLoop` struct, constructors, builders, public API
//!   (`propose`, `propose_conf_change`, `group_statuses`), and the main
//!   `run` shutdown-driven loop.
//! - [`tick`]: one iteration of the tick pipeline — drive MultiRaft,
//!   dispatch outbound AE/RV, apply committed entries, promote
//!   caught-up learners.
//! - [`handle_rpc`]: inbound RPC routing (`impl RaftRpcHandler`). The
//!   `JoinRequest` arm delegates to [`join`].
//! - [`join`]: async server-side `JoinRequest` orchestration — register
//!   peer, propose `AddLearner` on every group, wait for commit,
//!   broadcast topology, persist catalog, build the wire response.

mod builder;
pub mod handle_rpc;
pub mod hooks;
pub mod join;
pub mod loop_core;
pub mod proposals;
pub mod tick;

pub use hooks::{
    ShuffleAggregator, ShuffleConsumer, ShuffleProducer, ShuffleReceiver, SnapshotQuarantineHook,
};
pub use loop_core::{CommitApplier, RaftLoop, VShardEnvelopeHandler};
