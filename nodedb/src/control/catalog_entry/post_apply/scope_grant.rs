// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for scope grant `CatalogEntry` variants.
//! After the synchronous applier has written the redb row, install the
//! grant in the in-memory `ScopeGrantStore` on every node so scope
//! enrichment resolves the same effective scopes everywhere.

use std::sync::Arc;

use crate::control::security::catalog::StoredScopeGrant;
use crate::control::state::SharedState;

pub fn put(stored: StoredScopeGrant, shared: Arc<SharedState>) {
    shared.scope_grants.install_replicated_grant(&stored);
    tracing::debug!(
        scope = %stored.scope_name,
        grantee_type = %stored.grantee_type,
        grantee_id = %stored.grantee_id,
        expires_at = stored.expires_at,
        "post_apply: scope grant replicated"
    );
}

pub fn delete(
    scope_name: String,
    grantee_type: String,
    grantee_id: String,
    shared: Arc<SharedState>,
) {
    let removed =
        shared
            .scope_grants
            .install_replicated_revoke(&scope_name, &grantee_type, &grantee_id);
    tracing::debug!(
        scope = %scope_name, %grantee_type, %grantee_id, removed,
        "post_apply: scope revoke replicated"
    );
}
