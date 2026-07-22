// SPDX-License-Identifier: BUSL-1.1

//! Undo log types and rollback logic for transaction batches.

pub(super) mod apply;
pub(super) mod balanced;
pub(super) mod document;
pub(super) mod document_fts;
pub(super) mod entry;
pub(super) mod graph_node;
pub(super) mod kv;
pub(super) mod rollback;
pub(super) mod spatial;
pub(super) mod stats;

#[cfg(test)]
mod fts_strict_tests;
#[cfg(test)]
mod kv_ttl_sorted_tests;
#[cfg(test)]
mod parity_tests;
#[cfg(test)]
mod tests;

pub(in crate::data::executor) use entry::{TimeseriesIngestUndo, UndoEntry};
