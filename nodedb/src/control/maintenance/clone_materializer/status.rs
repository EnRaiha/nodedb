// SPDX-License-Identifier: BUSL-1.1

//! Clone status writes and checks shared by every engine's row copy.

use nodedb_types::{CloneStatus, Lsn};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;

/// Flip the status to `Materializing` if still `Shadowed`, so concurrent
/// readers see in-progress state. A crash after this resumes from
/// `progress_lsn = 0`.
pub(super) async fn mark_materializing(
    state: &SharedState,
    coll: &StoredCollection,
) -> crate::Result<()> {
    if !matches!(coll.clone_status, CloneStatus::Shadowed) {
        return Ok(());
    }
    let mut updated = coll.clone();
    updated.clone_status = CloneStatus::Materializing {
        progress_lsn: Lsn::new(0),
        bytes_done: 0,
        bytes_total: 0,
    };
    propose_catalog_entry_async(state, &CatalogEntry::PutCollection(Box::new(updated))).await?;
    Ok(())
}

/// Persist a `Materializing { progress_lsn, .. }` checkpoint between scan pages.
pub(super) async fn checkpoint_progress(
    state: &SharedState,
    coll: &StoredCollection,
    as_of_lsn: Lsn,
    copied: u64,
    total_seen: u64,
) -> crate::Result<()> {
    let mut updated = coll.clone();
    updated.clone_status = CloneStatus::Materializing {
        progress_lsn: as_of_lsn,
        bytes_done: copied,
        bytes_total: total_seen,
    };
    propose_catalog_entry_async(state, &CatalogEntry::PutCollection(Box::new(updated))).await?;
    Ok(())
}

/// Check that the home of `target_qualified` bound one surrogate per row.
///
/// `zip` drops the rows past a short answer. Every row is copied under its own
/// bound surrogate, or the copy fails here.
pub(super) fn check_bound_surrogates(
    target_qualified: &str,
    bound: usize,
    rows: usize,
) -> crate::Result<()> {
    if bound == rows {
        return Ok(());
    }
    Err(crate::Error::Storage {
        engine: "clone_materializer".into(),
        detail: format!(
            "the home of '{target_qualified}' answered {bound} surrogates for {rows} rows"
        ),
    })
}
