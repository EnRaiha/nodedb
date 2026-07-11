// SPDX-License-Identifier: BUSL-1.1

//! Array CRDT apply helpers invoked by the distributed Raft apply loop.
//!
//! These run on the Control Plane after Raft commit. They decode the replicated
//! entry, dispatch the resulting Data Plane plan via SPSC, and update the
//! authoritative op-log / schema registry. See [`crate::control::distributed_applier`]
//! for the loop that calls these.
//!
//! Split by concern:
//! - [`op`]: the committed `ArrayOp` apply path.
//! - [`schema`]: the committed `ArraySchema` apply path.
//! - [`common`]: shared scaffolding (position id, request builder, response
//!   await, array-open bootstrap, vShard derivation) reused across both.

mod common;
mod op;
mod schema;

pub(crate) use common::AppliedPosition;
pub(crate) use op::apply_array_op;
pub(crate) use schema::{ArraySchemaPayload, apply_array_schema};
