// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION <name> OWNER TO <user>` — transfer collection ownership.
//!
//! The result type is the protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! The ownership change is applied by mutating the parent `StoredCollection`
//! and re-proposing it (NOT via a standalone `PutOwner`): the `OWNERS` redb
//! table is rewritten from `stored.owner` by the `PutCollection` `post_apply`
//! on every node, so a separate `PutOwner` will be silently overwritten the
//! next time anyone re-proposed the collection. The authorization gate, new-
//! owner existence check, the propose + single-node fallback
//! (`put_collection` + `install_replicated_owner`), and the audit are
//! unchanged, as is the `ALTER COLLECTION` command tag.

use nodedb_types::DatabaseId;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::support::{err, load_active_collection, status};

/// ALTER COLLECTION <name> OWNER TO <user>
pub(super) async fn alter_collection_owner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    new_owner: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // Check authorization: current owner or admin. Owner rows are keyed by
    // database, so a collection outside the default database is only found
    // when the lookup carries the same `database_id` the row was written with.
    let current_owner = state.permissions.get_owner(
        "collection",
        database_id.as_u64(),
        identity.tenant_id,
        collection,
    );

    let is_current_owner = current_owner
        .as_ref()
        .is_some_and(|o| o == &identity.username);

    if !is_current_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(err(
            "42501",
            "permission denied: only the current owner, superuser, or tenant_admin can transfer ownership",
        ));
    }

    // Verify new owner exists.
    if state.credentials.get_user(new_owner).is_none() {
        return Err(err("42704", format!("user '{new_owner}' not found")));
    }

    // Mutate the parent `StoredCollection` and re-propose it: the
    // `OWNERS` redb table is canonical at boot time
    // (`PermissionStore::load_from`), but the `post_apply` for
    // `PutCollection` rewrites it from `stored.owner` on every
    // node — so the only way to keep an owner change durable
    // through subsequent ALTER COLLECTION calls is to also mutate
    // the parent record. A separate `PutOwner` will be silently
    // overwritten the next time anyone re-proposed the collection.
    let mut stored =
        load_active_collection(state, database_id, identity.tenant_id.as_u64(), collection)?;
    stored.owner = new_owner.to_string();
    let entry = CatalogEntry::PutCollection(Box::new(stored));
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "transferred ownership of collection '{collection}' from '{}' to '{new_owner}'",
            current_owner.unwrap_or_else(|| "<none>".to_string())
        ),
    );

    Ok(status("ALTER COLLECTION"))
}
