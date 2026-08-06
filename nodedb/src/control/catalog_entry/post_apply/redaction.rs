// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for redaction policy `CatalogEntry` variants.
//!
//! After the synchronous `apply::redaction` step has written the redb row,
//! this rehydrates the runtime `RedactionPolicy` (deserializing the
//! sonic_rs-encoded rule list) and installs it into the in-memory
//! `RedactionStore` on every node so the post-scan redaction pass sees the
//! new policy on its next request.

use std::sync::Arc;

use tracing::warn;

use crate::control::security::catalog::StoredRedactionPolicy;
use crate::control::state::SharedState;

pub fn put(stored: StoredRedactionPolicy, shared: Arc<SharedState>) {
    match stored.to_runtime() {
        Ok(runtime) => {
            shared.redaction.install_replicated_policy(runtime);
            tracing::debug!(
                policy = %stored.name,
                collection = %stored.collection,
                tenant = stored.tenant_id,
                "post_apply: redaction policy replicated"
            );
        }
        Err(e) => {
            warn!(
                policy = %stored.name,
                collection = %stored.collection,
                tenant = stored.tenant_id,
                error = %e,
                "post_apply: redaction policy rehydration failed"
            );
        }
    }
}

/// Delete every redaction policy bound to a purged collection, from both the
/// catalog and the in-memory registry.
///
/// A policy key carries no collection generation, so a survivor would apply
/// again the moment a collection is re-created under the same name — masking
/// (and refusing aggregates over) columns nobody asked to protect. Called from
/// the shared `PurgeCollection` reclaim path, so it runs on every node and on
/// the single-node inline purge alike.
pub fn purge_for_collection(shared: &SharedState, tenant_id: u64, collection: &str) {
    let catalog = shared.credentials.catalog();
    let roles = match crate::control::cascade::redaction::find_redaction_policies_on(
        catalog, tenant_id, collection,
    ) {
        Ok(roles) => roles,
        Err(e) => {
            warn!(
                collection = %collection,
                tenant = tenant_id,
                error = %e,
                "post_apply: redaction policy purge skipped (catalog read failed)"
            );
            return;
        }
    };
    for for_role in roles {
        if let Err(e) = catalog.delete_redaction_policy(tenant_id, collection, &for_role) {
            warn!(
                collection = %collection,
                for_role = %for_role,
                tenant = tenant_id,
                error = %e,
                "post_apply: redaction policy row delete failed on collection purge"
            );
        }
        shared
            .redaction
            .install_replicated_drop_policy(tenant_id, collection, &for_role);
    }
}

pub fn delete(tenant_id: u64, collection: String, for_role: String, shared: Arc<SharedState>) {
    let removed =
        shared
            .redaction
            .install_replicated_drop_policy(tenant_id, &collection, &for_role);
    tracing::debug!(
        collection = %collection,
        for_role = %for_role,
        tenant = tenant_id,
        removed,
        "post_apply: redaction policy drop replicated"
    );
}
