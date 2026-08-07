// SPDX-License-Identifier: BUSL-1.1

//! RLS filter fetch, merge, and deny-error construction.

use crate::control::security::auth_context::AuthContext;
use crate::control::security::rls::RlsPolicyStore;
use crate::types::TenantId;

/// Fetch RLS bytes for a (tenant, collection) pair.
pub(super) fn get_rls(
    rls_store: &RlsPolicyStore,
    tenant_id: u64,
    collection: &str,
    auth: &AuthContext,
) -> crate::Result<Vec<u8>> {
    rls_store
        .combined_read_predicate_with_auth(tenant_id, collection, auth)
        .ok_or_else(|| rls_deny_error(tenant_id, collection))
}

/// Fetch the compiled write-policy bytes for a (tenant, collection) pair.
///
/// Fails closed on an unresolvable `$auth.*` reference through the same deny
/// error the read fetch raises, so a write can never proceed on a predicate
/// that could not be resolved.
pub(super) fn get_rls_write(
    rls_store: &RlsPolicyStore,
    tenant_id: u64,
    collection: &str,
    auth: &AuthContext,
) -> crate::Result<Vec<u8>> {
    rls_store
        .combined_write_predicate_with_auth(tenant_id, collection, auth)
        .ok_or_else(|| rls_deny_error(tenant_id, collection))
}

/// Merge RLS filter bytes into existing filter bytes.
///
/// If existing filters are empty, replace. Otherwise deserialize both,
/// concatenate (AND-combine), and re-serialize.
///
/// Returns `Err` on serialization failure — fail-closed to prevent
/// silently dropping security filters.
pub(super) fn merge_filters(existing: &mut Vec<u8>, rls_bytes: &[u8]) -> crate::Result<()> {
    if existing.is_empty() {
        *existing = rls_bytes.to_vec();
        return Ok(());
    }

    let mut all: Vec<crate::bridge::scan_filter::ScanFilter> = zerompk::from_msgpack(existing)
        .map_err(|e| crate::Error::PlanError {
            detail: format!("RLS filter deserialization failed (existing): {e}"),
        })?;
    let rls: Vec<crate::bridge::scan_filter::ScanFilter> = zerompk::from_msgpack(rls_bytes)
        .map_err(|e| crate::Error::PlanError {
            detail: format!("RLS filter deserialization failed (new): {e}"),
        })?;
    all.extend(rls);
    *existing = zerompk::to_msgpack_vec(&all).map_err(|e| crate::Error::PlanError {
        detail: format!("RLS filter serialization failed: {e}"),
    })?;
    Ok(())
}

/// Create a deny error for unresolved RLS auth references.
fn rls_deny_error(tenant_id: u64, collection: &str) -> crate::Error {
    crate::Error::RejectedAuthz {
        tenant_id: TenantId::new(tenant_id),
        resource: format!(
            "RLS policy on '{}': unresolved session variable (deny by default)",
            collection
        ),
    }
}
