// SPDX-License-Identifier: Apache-2.0

//! KV engine operations dispatched to the Data Plane.

pub mod collection;
pub mod op;
pub mod resolved_mutation;

pub use op::KvOp;
pub use resolved_mutation::{KvResolveOutcome, KvResolvedMutation};
