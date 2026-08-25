// SPDX-License-Identifier: BUSL-1.1

//! RLS policy-existence gate for clone materialization.
//!
//! `dispatch_local` (see `dispatch.rs`) issues every scan and write with
//! `user_id: None` and `user_roles: Vec::new()` and bypasses Raft entirely —
//! the materializer has no `AuthenticatedIdentity`, so a `$auth.*` predicate
//! has nothing to bind against and the encode-boundary guard never sees
//! these writes. A clone target also does not inherit the source's
//! policies: `RlsPolicyStore` keys on `(tenant_id, collection_name)` only,
//! so the target is governed solely by policies created against its own
//! name.
//!
//! Rather than silently admit every row under that gap, [`refuse_if_rls_policy_applies`]
//! refuses the whole materialization up front — before a single row is
//! streamed — whenever a policy exists on either side of the copy. Called
//! once per collection, never per row.

use nodedb_types::DatabaseId;

use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;

/// Refuse materialization of `coll` when its source carries a read policy,
/// or its target carries a write policy — either would need `$auth.*`
/// evaluated against an identity this path does not have.
pub(super) fn refuse_if_rls_policy_applies(
    state: &SharedState,
    db_id: DatabaseId,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let Some(ref origin) = coll.cloned_from else {
        return Ok(());
    };

    let target_qualified = db_qualified(db_id, &coll.name);
    let source_qualified = db_qualified(origin.source_database, &origin.source_collection);
    let tenant_id = coll.tenant_id;

    let source_has_read = !state
        .rls
        .read_policies(tenant_id, &source_qualified)
        .is_empty();
    let target_has_write = !state
        .rls
        .write_policies(tenant_id, &target_qualified)
        .is_empty();

    if !source_has_read && !target_has_write {
        return Ok(());
    }

    let side = if source_has_read && target_has_write {
        format!(
            "source read policy on '{source_qualified}' and target write policy on \
             '{target_qualified}'"
        )
    } else if source_has_read {
        format!("source read policy on '{source_qualified}'")
    } else {
        format!("target write policy on '{target_qualified}'")
    };

    Err(crate::Error::BadRequest {
        detail: format!(
            "clone materialization cannot evaluate a row-level-security policy because it runs \
             without a writing identity: {side}"
        ),
    })
}
