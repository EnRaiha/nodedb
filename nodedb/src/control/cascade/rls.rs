// SPDX-License-Identifier: BUSL-1.1

//! Row-Level Security policy enumeration for `PurgeCollection` cascade.
//!
//! RLS policies are scoped to a single collection. When the collection
//! is hard-deleted the policies must go too — an orphan policy would
//! sit in the catalog forever and silently short-circuit future
//! queries if a collection with the same name were re-created.

use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

/// Enumerate RLS policies bound to `(tenant_id, collection)`.
/// Returns policy names only.
///
/// Policies are stored keyed by `db_qualified(database_id, collection)`, the
/// same key enforcement looks up — match on that, not the bare name.
pub fn find_rls_policies_on(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let qualified = db_qualified(database_id, collection);
    let all = catalog.load_all_rls_policies()?;
    let mut out: Vec<String> = all
        .into_iter()
        .filter(|p| p.tenant_id == tenant_id && p.collection == qualified)
        .map(|p| p.name)
        .collect();
    out.sort();
    Ok(out)
}
