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
