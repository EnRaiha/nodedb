// SPDX-License-Identifier: BUSL-1.1

//! Landing one bulk-UPDATE row: the write transaction the row's body and its
//! secondary-index diff share.
//!
//! Its own file because the transaction boundary is the concern — the bulk
//! handler decides WHICH rows change and what they become, and this decides
//! when that becomes durable. Every sparse-database write a row produces is
//! staged into the transaction opened here and lands on its commit, so a row
//! that fails at any step drops the transaction un-committed and is skipped
//! whole rather than left with a body the index no longer describes.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::update_reindex::NonbitemporalUpdateReindex;

impl CoreLoop {
    /// Write one bulk-UPDATE row's post-image and reconcile its plain
    /// `INDEXES` entries, committing both together.
    ///
    /// Returns the `(field, value)` tuples the index diff touched, for the
    /// caller to publish into the per-index write-value substrate — that
    /// recording describes a durable write, so it belongs after this returns
    /// `Ok`, never before.
    pub(super) fn persist_bulk_update_row(
        &mut self,
        p: NonbitemporalUpdateReindex<'_>,
    ) -> crate::Result<Vec<(String, String)>> {
        let txn = self.sparse.begin_write()?;
        let touched = self.nonbitemporal_update_reindex(&txn, p)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("nonbitemporal update reindex commit: {e}"),
        })?;
        Ok(touched)
    }
}
