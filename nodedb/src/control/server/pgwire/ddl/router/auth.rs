// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    upper: &str,
    parts: &[&str],
    database_id: DatabaseId,
) -> Option<PgWireResult<Vec<Response>>> {
    // User and role management (CREATE / ALTER / DROP USER, CREATE / ALTER /
    // DROP ROLE) are handled by the protocol-neutral DDL router, which runs
    // before this pgwire delegation.

    // Service account management (CREATE / DROP / ALTER SERVICE ACCOUNT) is
    // handled by the protocol-neutral DDL router, which runs before this pgwire
    // delegation.

    // System-level settings (ALTER SYSTEM SET ...).
    if upper.starts_with("ALTER SYSTEM ") {
        return Some(super::super::system_ddl::alter_system(
            state, identity, parts,
        ));
    }

    // Tenant management.
    if upper.starts_with("CREATE TENANT ") {
        return Some(super::super::tenant::create_tenant(state, identity, parts));
    }
    if upper.starts_with("ALTER TENANT ") {
        return Some(super::super::tenant::alter_tenant(state, identity, parts));
    }
    if upper.starts_with("DROP TENANT ") {
        return Some(super::super::tenant::drop_tenant(state, identity, parts));
    }
    if upper.starts_with("PURGE TENANT ") {
        return Some(super::super::tenant::purge_tenant(state, identity, database_id, parts).await);
    }
    if upper.starts_with("SHOW TENANT USAGE") {
        return Some(super::super::tenant::show_tenant_usage(
            state, identity, parts,
        ));
    }
    if upper.starts_with("SHOW TENANT QUOTA") {
        return Some(super::super::tenant::show_tenant_quota(
            state, identity, parts,
        ));
    }

    // GRANT / REVOKE (role-membership and object-permission) are parsed
    // into typed `AuthStmt` variants and dispatched via the AST router —
    // no string-prefix branch is needed here.

    None
}
