// SPDX-License-Identifier: Apache-2.0

//! The non-recording implementation of the columnar report sites.
//!
//! Compiled when the `diagnostics` feature is off, and on wasm32. Every entry
//! point keeps the signature of its recording counterpart and does nothing.

use crate::error::ColumnarError;

#[inline]
pub fn memtable_cell_corrupt(_err: &ColumnarError, _collection: &str) {}

#[inline]
pub fn wal_row_corrupt(_err: &ColumnarError) {}

#[inline]
pub fn string_cell_not_utf8(_err: &ColumnarError) {}
