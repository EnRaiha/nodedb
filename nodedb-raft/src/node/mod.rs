// SPDX-License-Identifier: BUSL-1.1

//! Raft single-group state machine.
//!
//! Split across files:
//! - [`config`]: `RaftConfig` (including voter/learner lists).
//! - [`core`]: `RaftNode` struct, constructors, simple accessors, `tick`,
//!   `propose`, and the `Ready` output type.
//! - [`durability`]: Applied-index durability floor and log compaction.
//! - [`internal`]: Internal state transitions (elections, replication,
//!   commit advancement) and timeout math.
//! - [`membership`]: Dynamic configuration changes — add/remove voters,
//!   add/remove/promote learners.
//! - [`read_index`]: Confirming leadership against a quorum before serving a
//!   linearizable read.
//! - [`rpc`]: Incoming RPC handlers (`AppendEntries`, `PreVote`,
//!   `RequestVote`, `InstallSnapshot`, `TimeoutNow`, and their response
//!   handlers).
//! - [`staleness`]: How far behind the leader a replica is, for
//!   bounded-staleness reads.

pub mod config;
pub mod core;
pub mod durability;
mod internal;
pub mod membership;
pub mod read_index;
pub mod rpc;
pub mod staleness;

pub use self::config::RaftConfig;
pub use self::core::{RaftNode, Ready};
pub use self::read_index::{ReadIndexProbe, ReadIndexStatus};
pub use self::staleness::StalenessVerdict;
