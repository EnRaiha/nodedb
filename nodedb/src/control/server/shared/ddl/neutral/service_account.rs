// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral service-account DDL — CREATE / DROP / ALTER SET DATABASES.
//!
//! Ported from the pgwire `ddl::service_account` and
//! `ddl::service_account_alter` handlers. All non-return logic (permission
//! checks, `IF [NOT] EXISTS` token stripping, ROLE / TENANT / FOR DATABASE /
//! IN DATABASE parsing, credential-store `create_service_account` /
//! `drop_user` / `set_service_account_databases`, database-name resolution via
//! the system catalog, and the `audit_record` calls) is preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError`
//! to the protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};
use super::auth_support::{
    parse_role, require_tenant_admin, status, strip_if_exists, strip_if_not_exists,
};

/// Superuser gate, folded in verbatim from the pgwire `require_superuser`
/// helper: on denial it emits `AuditEvent::PermissionDenied` (database-less
/// scope, matching the `None` `db_id` the pgwire handler passed) and returns
/// SQLSTATE 42501, preserving both the side effect and the wire error.
fn require_superuser(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser {
        Ok(())
    } else {
        state.audit_record_with_db(
            AuditEvent::PermissionDenied,
            Some(identity.tenant_id),
            None,
            &identity.username,
            action,
        );
        Err(DdlError {
            sqlstate: "42501".to_string(),
            message: format!("permission denied: {action} requires superuser"),
        })
    }
}

/// CREATE SERVICE ACCOUNT [IF NOT EXISTS] <name> [ROLE <role>] [TENANT <id>]
///                                [FOR DATABASE <db>]
///                                [FOR TENANT <id> IN DATABASE <db>]
///
/// Creates a service account — a non-interactive identity that can only
/// authenticate via API keys. No password, no pgwire login.
pub fn create_service_account(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create service accounts")?;

    let (if_not_exists, parts) = strip_if_not_exists(parts, 3);

    if parts.len() < 4 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: CREATE SERVICE ACCOUNT [IF NOT EXISTS] <name> [ROLE <role>] [FOR DATABASE <db>]".to_string(),
        });
    }

    let name = parts[3];

    // `IF NOT EXISTS`: re-creating an existing service account is a no-op.
    if if_not_exists && state.credentials.get_user(name).is_some() {
        return Ok(status("CREATE SERVICE ACCOUNT"));
    }

    // Parse optional ROLE, TENANT, FOR DATABASE / IN DATABASE.
    let mut role = Role::ReadWrite;
    let mut tenant_id = identity.tenant_id;
    let mut accessible_databases: Vec<nodedb_types::id::DatabaseId> = vec![];
    let mut seen_for_database = false;
    let mut seen_for_tenant = false;
    let mut i = 4;
    while i < parts.len() {
        let up = parts[i].to_uppercase();
        match up.as_str() {
            "ROLE" if i + 1 < parts.len() => {
                role = parse_role(parts[i + 1]);
                i += 2;
            }
            "TENANT" if i + 1 < parts.len() => {
                if !identity.is_superuser {
                    return Err(DdlError {
                        sqlstate: "42501".to_string(),
                        message: "only superuser can assign tenants".to_string(),
                    });
                }
                let tid: u64 = parts[i + 1].parse().map_err(|_| DdlError {
                    sqlstate: "42601".to_string(),
                    message: "TENANT must be a numeric ID".to_string(),
                })?;
                tenant_id = crate::types::TenantId::new(tid);
                seen_for_tenant = true;
                i += 2;
            }
            "FOR" if i + 1 < parts.len() => {
                let next_up = parts[i + 1].to_uppercase();
                match next_up.as_str() {
                    "DATABASE" if i + 2 < parts.len() => {
                        let db_name = parts[i + 2];
                        let db_id = resolve_database(state, db_name)?;
                        accessible_databases = vec![db_id];
                        seen_for_database = true;
                        i += 3;
                    }
                    "TENANT" if i + 2 < parts.len() => {
                        // FOR TENANT <id> IN DATABASE <db> — superuser only.
                        if !identity.is_superuser {
                            return Err(DdlError {
                                sqlstate: "42501".to_string(),
                                message: "only superuser can use FOR TENANT ... IN DATABASE"
                                    .to_string(),
                            });
                        }
                        let tid: u64 = parts[i + 2].parse().map_err(|_| DdlError {
                            sqlstate: "42601".to_string(),
                            message: "TENANT must be a numeric ID".to_string(),
                        })?;
                        tenant_id = crate::types::TenantId::new(tid);
                        seen_for_tenant = true;
                        i += 3;
                        // Expect IN DATABASE <db> immediately after.
                        if i + 2 < parts.len()
                            && parts[i].to_uppercase() == "IN"
                            && parts[i + 1].to_uppercase() == "DATABASE"
                        {
                            let db_name = parts[i + 2];
                            let db_id = resolve_database(state, db_name)?;
                            accessible_databases = vec![db_id];
                            seen_for_database = true;
                            i += 3;
                        } else {
                            return Err(DdlError {
                                sqlstate: "42601".to_string(),
                                message: "FOR TENANT ... must be followed by IN DATABASE <name>"
                                    .to_string(),
                            });
                        }
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            "IN" if i + 2 < parts.len() && parts[i + 1].to_uppercase() == "DATABASE" => {
                let db_name = parts[i + 2];
                let db_id = resolve_database(state, db_name)?;
                accessible_databases = vec![db_id];
                seen_for_database = true;
                i += 3;
            }
            _ => {
                i += 1;
            }
        }
    }

    // FOR TENANT without IN DATABASE is a syntax error.
    if seen_for_tenant && !seen_for_database && !accessible_databases.is_empty() {
        // already set — fine
    } else if seen_for_tenant && !seen_for_database && accessible_databases.is_empty() {
        // If FOR TENANT was used standalone (old form), that's allowed for backwards compat.
        // Only reject when user explicitly wrote FOR TENANT ... (without IN DATABASE in
        // the new sense) after the new parser added the requirement above, which already
        // returns an error inline. So no additional check needed here.
    }
    let _ = seen_for_tenant; // suppress unused warning

    state
        .credentials
        .create_service_account(name, tenant_id, vec![role], accessible_databases)
        .map_err(|e| DdlError {
            sqlstate: "42710".to_string(),
            message: e.to_string(),
        })?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(tenant_id),
        &identity.username,
        &format!("created service account '{name}' in tenant {tenant_id}"),
    );

    Ok(status("CREATE SERVICE ACCOUNT"))
}

/// DROP SERVICE ACCOUNT [IF EXISTS] <name>
pub fn drop_service_account(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop service accounts")?;

    let (if_exists, parts) = strip_if_exists(parts, 3);

    if parts.len() < 4 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: DROP SERVICE ACCOUNT [IF EXISTS] <name>".to_string(),
        });
    }

    let name = parts[3];

    // Verify it's actually a service account.
    let user = match state.credentials.get_user(name) {
        Some(u) => u,
        None => {
            // `IF EXISTS`: dropping a missing account is a no-op success.
            if if_exists {
                return Ok(status("DROP SERVICE ACCOUNT"));
            }
            return Err(DdlError {
                sqlstate: "42704".to_string(),
                message: format!("service account '{name}' not found"),
            });
        }
    };
    if !user.is_service_account {
        return Err(DdlError {
            sqlstate: "42809".to_string(),
            message: format!("'{name}' is a user, not a service account. Use DROP USER instead."),
        });
    }

    let dropped = state.credentials.drop_user(name).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: e.to_string(),
    })?;

    if dropped {
        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped service account '{name}'"),
        );
        Ok(status("DROP SERVICE ACCOUNT"))
    } else {
        Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("service account '{name}' not found"),
        })
    }
}

/// ALTER SERVICE ACCOUNT <name> SET DATABASES (db1, db2, ...)
///
/// Superuser only. Resolves database names to IDs; rejects unknown names with `42704`.
pub fn alter_service_account_set_databases(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "ALTER SERVICE ACCOUNT SET DATABASES")?;

    // parts: ["ALTER", "SERVICE", "ACCOUNT", <name>, "SET", "DATABASES", "(db1,", "db2", ...)"]
    if parts.len() < 7 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: ALTER SERVICE ACCOUNT <name> SET DATABASES (db1, db2, ...)"
                .to_string(),
        });
    }

    if !parts[1].eq_ignore_ascii_case("SERVICE")
        || !parts[2].eq_ignore_ascii_case("ACCOUNT")
        || !parts[4].eq_ignore_ascii_case("SET")
        || !parts[5].eq_ignore_ascii_case("DATABASES")
    {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: ALTER SERVICE ACCOUNT <name> SET DATABASES (db1, db2, ...)"
                .to_string(),
        });
    }

    let name = parts[3];

    // Verify it's actually a service account.
    let user = state.credentials.get_user(name).ok_or_else(|| DdlError {
        sqlstate: "42704".to_string(),
        message: format!("service account '{name}' not found"),
    })?;
    if !user.is_service_account {
        return Err(DdlError {
            sqlstate: "42809".to_string(),
            message: format!("'{name}' is a user, not a service account"),
        });
    }

    // Collect and resolve database names from parts[6..].
    let raw_names: Vec<&str> = parts[6..]
        .iter()
        .map(|s| {
            s.trim_start_matches('(')
                .trim_end_matches(')')
                .trim_end_matches(',')
        })
        .filter(|s| !s.is_empty())
        .collect();

    if raw_names.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "SET DATABASES requires at least one database name".to_string(),
        });
    }

    let catalog = state.credentials.catalog();
    let mut db_ids = Vec::with_capacity(raw_names.len());
    for db_name in raw_names {
        let resolved: Option<nodedb_types::id::DatabaseId> = catalog
            .get_database_id_by_name(db_name)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: e.to_string(),
            })?;
        match resolved {
            Some(id) => db_ids.push(id),
            None => {
                return Err(DdlError {
                    sqlstate: "42704".to_string(),
                    message: format!("database '{db_name}' not found"),
                });
            }
        }
    }

    state
        .credentials
        .set_service_account_databases(name, db_ids)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?;

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("altered service account '{name}': set databases"),
    );

    Ok(status("ALTER SERVICE ACCOUNT"))
}

/// Resolve a database name to its `DatabaseId`, returning a [`DdlError`] if not found.
fn resolve_database(
    state: &SharedState,
    name: &str,
) -> Result<nodedb_types::id::DatabaseId, DdlError> {
    let catalog = state.credentials.catalog();
    let resolved: Option<nodedb_types::id::DatabaseId> = catalog
        .get_database_id_by_name(name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?;
    resolved.ok_or_else(|| DdlError {
        sqlstate: "42704".to_string(),
        message: format!("database '{name}' not found"),
    })
}
