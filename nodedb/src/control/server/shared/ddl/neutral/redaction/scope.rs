// SPDX-License-Identifier: BUSL-1.1

//! Shared gates for redaction-policy DDL: tenant authorization, the
//! array-collection rejection, and the status-result builder.

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// Build a single-tag status result.
pub(super) fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Resolve and authorize the tenant scope for redaction administration.
///
/// Identical gate to RLS policy administration: tenant administrators may
/// manage only their authenticated tenant, and an explicit cross-tenant target
/// is reserved for superusers.
pub(super) fn authorize_redaction_scope(
    identity: &AuthenticatedIdentity,
    tenant_id_override: Option<u64>,
) -> Result<u64, DdlError> {
    if !identity.is_superuser && !identity.roles.contains(&Role::TenantAdmin) {
        return Err(DdlError {
            sqlstate: "42501".to_string(),
            message: "permission denied: requires superuser or tenant_admin".to_string(),
        });
    }

    let own_tenant_id = identity.tenant_id.as_u64();
    let tenant_id = tenant_id_override.unwrap_or(own_tenant_id);
    if tenant_id != own_tenant_id && !identity.is_superuser {
        return Err(DdlError {
            sqlstate: "42501".to_string(),
            message: "permission denied: cross-tenant redaction administration requires superuser"
                .to_string(),
        });
    }
    Ok(tenant_id)
}

/// Reject a policy whose target is an array.
///
/// A redaction policy is keyed on `(tenant, collection, field)` and is enforced
/// by the Control-Plane result-shaping hooks, which rewrite the decoded rows of
/// a collection read. Array cells reach a client through the `array_delivery`
/// fan-out instead, which carries no subscriber identity and therefore cannot
/// resolve the roles a policy is keyed on. Accepting a policy that names an
/// array attribute would persist a rule that is never applied — a silent
/// fail-open — so the DDL refuses it up front.
///
/// Arrays are registered per `(tenant, database, name)` while a redaction
/// policy carries no database, so the check is tenant-wide across databases:
/// a name that is an array anywhere in the tenant is refused.
pub(super) fn reject_array_collection(
    state: &SharedState,
    tenant_id: u64,
    collection: &str,
) -> Result<(), DdlError> {
    let is_array = {
        let registry = state
            .array_catalog
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.all_entries().into_iter().any(|entry| {
            entry.array_id.tenant_id.as_u64() == tenant_id && entry.array_id.name == collection
        })
    };
    if is_array {
        return Err(DdlError {
            sqlstate: "0A000".to_string(),
            message: format!(
                "'{collection}' is an array: column redaction does not cover array attributes, \
                 whose cells are delivered without a subscriber identity to resolve the policy \
                 role against. Refusing rather than persisting a policy that would never apply."
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    fn identity(tenant_id: u64, roles: Vec<Role>, is_superuser: bool) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            1,
            "test",
            TenantId::new(tenant_id),
            roles,
            is_superuser,
            None,
            AuthenticatedIdentity::default_database_set(is_superuser),
        )
    }

    #[test]
    fn redaction_scope_requires_admin_and_reserves_cross_tenant_for_superusers() {
        let ordinary = identity(7, Vec::new(), false);
        let admin = identity(7, vec![Role::TenantAdmin], false);
        let superuser = identity(7, Vec::new(), true);

        assert_eq!(
            authorize_redaction_scope(&ordinary, None)
                .unwrap_err()
                .sqlstate,
            "42501"
        );
        assert_eq!(
            authorize_redaction_scope(&admin, None).expect("own tenant"),
            7
        );
        assert_eq!(
            authorize_redaction_scope(&admin, Some(8))
                .unwrap_err()
                .sqlstate,
            "42501"
        );
        assert_eq!(
            authorize_redaction_scope(&superuser, Some(8)).expect("superuser"),
            8
        );
    }
}
