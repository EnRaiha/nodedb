// SPDX-License-Identifier: BUSL-1.1

//! Scatter-gather coordinator for cross-shard graph traversals.
//!
//! Cross-shard hops are NOT handled by forwarding the traversal to
//! the remote shard. Instead, the Data Plane returns partial results (the set
//! of cross-shard edge targets) to the Control Plane, which batches and
//! dispatches them to the appropriate target cores.
//!
//! This keeps the Data Plane stateless per-request and avoids distributed
//! deadlocks from recursive cross-shard calls.
//!
//! ## Vectorized Scatter Envelopes
//!
//! The Data Plane MUST NOT emit one SPSC message per unresolved cross-shard edge.
//! Instead, for each hop level, cross-shard destinations are accumulated into a
//! single vectorized envelope grouped by target shard:
//! `{ shard_id -> [node_id, ...] }`.

pub mod envelope;
pub mod fan_out;
pub mod hop;
pub mod merge_results;
pub mod remote_sql;

pub use envelope::{ScatterBatch, ScatterEnvelope, partition_local_remote};
pub use fan_out::{FanOutDecision, apply_fan_out_limits};
pub use hop::{CrossShardHopParams, coordinate_cross_shard_hop};
pub use merge_results::merge_traversal_results;
