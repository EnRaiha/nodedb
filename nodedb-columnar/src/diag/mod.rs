// SPDX-License-Identifier: Apache-2.0

//! Black-box recorder wiring for corrupt columnar state.
//!
//! A corrupt memtable cell, WAL row, or string cell is refused with an
//! error. The report filed here records the corruption at the site that
//! detects it, so it survives a restart that clears the memtable. The real implementation compiles only
//! under the `diagnostics` feature and off wasm32. Otherwise every entry
//! point is a no-op with the same signature, so call sites need no `cfg`.
//!
//! This crate never calls `faultbox::init`. The host binary owns the
//! recorder, so everything here is inert until the host initializes it.

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
mod context;
#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
mod recording;

#[cfg(not(all(feature = "diagnostics", not(target_arch = "wasm32"))))]
mod inert;

#[cfg(all(feature = "diagnostics", not(target_arch = "wasm32")))]
pub use recording::{memtable_cell_corrupt, string_cell_not_utf8, wal_row_corrupt};

#[cfg(not(all(feature = "diagnostics", not(target_arch = "wasm32"))))]
pub use inert::{memtable_cell_corrupt, string_cell_not_utf8, wal_row_corrupt};
