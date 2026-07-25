// SPDX-License-Identifier: BUSL-1.1

//! Single conversion boundary for identities derived from external claims.

use std::sync::Once;

use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};

static EXTERNAL_SUPERUSER_WARNING: Once = Once::new();

/// Non-authoritative identity fields extracted from a verified external token.
pub(crate) struct ExternalClaims<'a> {
    pub(crate) user_id: u64,
    pub(crate) subject: &'a str,
    pub(crate) role_names: &'a [String],
    pub(crate) asserted_superuser: bool,
}

/// Server-owned scope selected by the verified identity provider.
///
/// Fields are private so token-decoding code cannot substitute a claim-derived
/// tenant or database set after provider selection.
pub(crate) struct ExternalProviderBinding {
    tenant_id: TenantId,
    default_database: Option<DatabaseId>,
    accessible_databases: DatabaseSet,
}

impl ExternalProviderBinding {
    pub(crate) fn default_database(tenant_id: TenantId) -> Self {
        Self {
            tenant_id,
            default_database: None,
            accessible_databases: AuthenticatedIdentity::default_database_set(false),
        }
    }

    pub(crate) fn mapped_databases(
        tenant_id: TenantId,
        default_database: DatabaseId,
        accessible_databases: DatabaseSet,
    ) -> Self {
        Self {
            tenant_id,
            default_database: Some(default_database),
            accessible_databases,
        }
    }
}

/// Convert verified external claims into a non-superuser identity.
///
/// This is the only external-claim conversion. Tenant/database authority comes
/// exclusively from the server-owned provider binding; claim roles are capped
/// below NodeDB's catalog-owned superuser authority.
pub(crate) fn identity_from_external_claims(
    claims: ExternalClaims<'_>,
    binding: ExternalProviderBinding,
) -> AuthenticatedIdentity {
    let roles = roles_from_external_claims(claims.role_names, claims.asserted_superuser);
    let username = if claims.subject.is_empty() {
        format!("external_user_{}", claims.user_id)
    } else {
        claims.subject.to_owned()
    };
    AuthenticatedIdentity::new_regular(
        claims.user_id,
        username,
        binding.tenant_id,
        AuthMethod::OidcBearer,
        roles,
        binding.default_database,
        binding.accessible_databases,
    )
}

/// Parse externally supplied role names while enforcing NodeDB's privilege ceiling.
fn roles_from_external_claims(role_names: &[String], asserted_superuser: bool) -> Vec<Role> {
    let mut stripped_superuser = asserted_superuser;
    let roles = role_names
        .iter()
        .filter_map(|name| {
            let role = parse_role_infallible(name);
            if matches!(role, Role::Superuser) {
                stripped_superuser = true;
                None
            } else {
                Some(role)
            }
        })
        .collect();

    if stripped_superuser {
        EXTERNAL_SUPERUSER_WARNING.call_once(|| {
            tracing::warn!(
                "ignored superuser authority asserted by an external identity source; grant superuser through NodeDB credential administration"
            );
        });
    }

    roles
}

fn parse_role_infallible(name: &str) -> Role {
    match name.parse::<Role>() {
        Ok(role) => role,
        Err(never) => match never {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_conversion_strips_authority_and_uses_provider_tenant() {
        let roles = vec!["superuser".into(), "readwrite".into(), "custom".into()];
        let identity = identity_from_external_claims(
            ExternalClaims {
                user_id: 7,
                subject: "external",
                role_names: &roles,
                asserted_superuser: true,
            },
            ExternalProviderBinding::default_database(TenantId::new(42)),
        );

        assert_eq!(identity.tenant_id, TenantId::new(42));
        assert!(!identity.is_superuser());
        assert!(!identity.roles.contains(&Role::Superuser));
        assert!(identity.roles.contains(&Role::ReadWrite));
        assert!(identity.roles.contains(&Role::Custom("custom".into())));
    }
}
