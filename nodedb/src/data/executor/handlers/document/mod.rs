// SPDX-License-Identifier: BUSL-1.1

//! Document operation handlers — module root.
//! Submodules: read (scan), write (batch insert, register),
//! index_maintenance (backfill, drop index), sort (external sort, sort
//! helpers).

pub mod apply_balance_delta;
pub mod index_fetch;
pub mod index_maintenance;
pub mod read;
pub mod sort;
pub mod write;
