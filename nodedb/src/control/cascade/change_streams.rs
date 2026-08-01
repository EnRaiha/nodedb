// SPDX-License-Identifier: BUSL-1.1

//! Change-stream enumeration for `PurgeCollection` cascade.
//!
//! Every change stream targets a specific collection via its
//! `collection` field. Wildcard / all-collections streams (if any)
//! are not enumerated here — they're expressed as a sentinel name
//! (empty string or `*`) and must survive a single-collection
//! cascade.

use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

/// Enumerate change streams targeting `(tenant_id, collection)`.
/// Returns stream names only, sorted.
pub fn find_change_streams_on(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let all = catalog.load_all_change_streams()?;
    let mut out: Vec<String> = all
        .into_iter()
        .filter(|s| {
            s.database_id == database_id
                && s.tenant_id == tenant_id
                && s.collection == collection
                && !s.collection.is_empty()
        })
        .map(|s| s.name)
        .collect();
    out.sort();
    Ok(out)
}
