// SPDX-License-Identifier: BUSL-1.1

//! Document operation handlers — module root.
//! Submodules: read (scan), write (batch insert, register),
//! index_maintenance (backfill, drop index), sort (external sort, sort
//! helpers), text_extract (FTS indexing).

pub mod index_fetch;
pub mod index_maintenance;
pub mod read;
pub mod sort;
pub mod text_extract;
pub mod write;

pub use text_extract::extract_indexable_text;
