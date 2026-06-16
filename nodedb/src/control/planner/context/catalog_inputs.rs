// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use crate::control::security::credential::CredentialStore;

/// Inputs needed to construct an `OriginCatalog` per plan call.
///
/// Tenant is intentionally **not** stored here: every plan call passes the
/// effective tenant to `build_adapter`, so a single `QueryContext` shared
/// across a pgwire handler can serve queries from connections belonging to
/// different tenants without cross-tenant catalog resolution.
#[derive(Clone)]
pub(super) struct CatalogInputs {
    pub(super) credentials: Arc<CredentialStore>,
    pub(super) shared: Option<std::sync::Weak<crate::control::state::SharedState>>,
    pub(super) retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
}

impl CatalogInputs {
    pub(super) fn build_adapter(
        &self,
        tenant_id: u64,
        database_id: crate::types::DatabaseId,
    ) -> super::super::catalog_adapter::OriginCatalog {
        if let Some(weak) = &self.shared
            && let Some(shared) = weak.upgrade()
        {
            super::super::catalog_adapter::OriginCatalog::new_with_lease(
                &shared,
                tenant_id,
                database_id,
                self.retention_policy_registry.clone(),
            )
        } else {
            super::super::catalog_adapter::OriginCatalog::new(
                Arc::clone(&self.credentials),
                tenant_id,
                database_id,
                self.retention_policy_registry.clone(),
            )
        }
    }
}
