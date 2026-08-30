// SPDX-License-Identifier: BUSL-1.1

//! MATCH execution functions — top-level entry points and triple evaluation.

mod binding;
mod clause;
mod ctx;
mod entry;
mod join;
mod msgpack;
pub(super) mod triple;

pub use ctx::MatchExecCtx;
pub use entry::execute;
pub use msgpack::rows_to_msgpack;

pub(in crate::engine::graph::pattern::executor) use binding::{bind_node, binding_compatible};
pub(super) use clause::execute_clause;
pub(in crate::engine::graph::pattern::executor) use triple::execute_triple;
