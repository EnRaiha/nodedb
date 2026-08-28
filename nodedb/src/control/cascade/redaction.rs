// SPDX-License-Identifier: BUSL-1.1

//! Column-redaction policy enumeration for the `PurgeCollection` cascade.
//!
//! A redaction policy is scoped to a single collection. When the collection is
//! hard-deleted the policies must go too — an orphan policy would sit in the
//! catalog forever and silently redact (or refuse aggregates over) a later
//! collection re-created under the same name.
//!
//! Unlike the RLS twin this does NOT feed the `collect_dependents` blocking
//! check: redaction policies are swept automatically by the shared collection
//! reclaim path and by the tenant teardown, so there is never an orphan to
//! refuse a drop over — and refusing would leave that sweep unreachable.

use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

/// Enumerate redaction policies bound to `(tenant_id, collection)`.
///
/// Returns the ruled role of each policy: identity is the
/// `(tenant, collection, for_role)` triple, so the role — not the policy label
/// — is what a caller needs in order to delete one.
///
/// Policies are stored keyed by `db_qualified(database_id, collection)`, the
/// same key enforcement looks up — match on that, not the bare name.
pub fn find_redaction_policies_on(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<String>> {
    let qualified = db_qualified(database_id, collection);
    let all = catalog.load_all_redaction_policies()?;
    let mut out: Vec<String> = all
        .into_iter()
        .filter(|p| p.tenant_id == tenant_id && p.collection == qualified)
        .map(|p| p.for_role)
        .collect();
    out.sort();
    Ok(out)
}
