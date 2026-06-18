// SPDX-License-Identifier: BUSL-1.1

//! Uniform row-source abstraction for join sides.
//!
//! A [`RowSource`] represents one side of a join and allows the grace-hash-join
//! driver to consume rows through a single `for_each` call instead of inline
//! `scan_collection_for_each` calls scattered through `drive_grace_build`.
//!
//! `LocalScan` is the first (and currently only) variant — it is a pure
//! pass-through wrapper around `CoreLoop::scan_collection_for_each` that is
//! byte-identical to the previous inline calls.
//!
//! The match inside `for_each` is the seam for a future `ShuffleStream` variant
//! (network-fed rows from a distributed exchange). That variant will dispatch at
//! the same match arm without touching any of the call sites in `grace_drive.rs`.
//! Do not add it until it is needed.

use crate::data::executor::core_loop::CoreLoop;

/// One side of a join consumed through a uniform interface.
///
/// Currently the only variant is [`RowSource::LocalScan`], which is a
/// pass-through to `CoreLoop::scan_collection_for_each`. A future
/// `ShuffleStream` variant for network-fed exchange rows will be added here —
/// the dispatch seam is the `match self` inside [`RowSource::for_each`].
pub(super) enum RowSource {
    /// Scan rows directly from a local collection on this core.
    LocalScan {
        database_id: u64,
        tenant_id: u64,
        collection: String,
    },
}

impl RowSource {
    /// Iterate every row in this source, calling `f(id, bytes)` for each.
    ///
    /// Errors from `f` and from the underlying scan are propagated via `?`.
    pub(super) fn for_each<F>(&self, core: &CoreLoop, f: F) -> crate::Result<()>
    where
        F: FnMut(&str, &[u8]) -> crate::Result<()>,
    {
        match self {
            RowSource::LocalScan {
                database_id,
                tenant_id,
                collection,
            } => core.scan_collection_for_each(*database_id, *tenant_id, collection, f),
        }
    }
}
