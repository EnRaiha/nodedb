// SPDX-License-Identifier: BUSL-1.1

//! Undo log types and rollback logic for transaction batches.

pub(super) mod apply;
pub(super) mod balanced;
pub(super) mod document;
pub(super) mod entry;
pub(super) mod graph_node;
pub(super) mod rollback;
pub(super) mod spatial;
pub(super) mod stats;

#[cfg(test)]
mod parity_tests;
#[cfg(test)]
mod tests;

pub(in crate::data::executor) use entry::UndoEntry;
