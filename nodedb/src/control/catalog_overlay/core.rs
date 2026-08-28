// SPDX-License-Identifier: BUSL-1.1

//! Shared replay engine behind every per-kind overlay in this module.
//!
//! Every kind here follows the collections' exact shape: one `Put<Kind>`
//! `CatalogEntry` variant upserts the full row, a sibling `Delete<Kind>`
//! removes it by identity. [`resolve`] replays a connection's buffered DDL
//! over one committed read using two kind-supplied closures — `targets`
//! (does this entry mutate the identity being resolved) and `step` (apply
//! one matching entry to the state resolved so far) — so each kind module
//! only defines those two closures and its own `Put`/`Delete` pattern match.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::server::shared::session::ddl_buffer;

/// Resolve one row through this connection's uncommitted DDL.
///
/// `committed` is what the catalog itself holds; it is returned unchanged
/// outside a transaction and outside any connection scope. `targets` picks
/// out the buffered entries that mutate this exact row; `step` replays one
/// such entry over the state resolved so far.
pub(super) fn resolve<T: Clone>(
    committed: Option<T>,
    targets: impl Fn(&CatalogEntry) -> bool,
    step: impl Fn(Option<T>, &CatalogEntry) -> Option<T>,
) -> Option<T> {
    let overlaid = ddl_buffer::with_buffered(|buffered| {
        let mut touched = false;
        let mut current = None;
        for item in buffered {
            if !targets(&item.entry) {
                continue;
            }
            if !touched {
                // Cloned only once the row is actually buffered, so an
                // untouched row pays nothing beyond the scan.
                current = committed.clone();
                touched = true;
            }
            current = step(current, &item.entry);
        }
        touched.then_some(current)
    });
    match overlaid {
        Some(Some(resolved)) => resolved,
        Some(None) | None => committed,
    }
}
