// SPDX-License-Identifier: Apache-2.0

//! Parse top-level `GRANT` / `REVOKE` statements.
//!
//! Disambiguation follows the SQL standard: a `GRANT`/`REVOKE` with **no
//! `ON` clause** is a role-membership grant; one **with an `ON` clause** is
//! an object-permission grant. The `ROLE` keyword (`GRANT ROLE r TO u`) is
//! accepted as an optional alias for the no-`ON` form — it is not required.
//!
//! Both the role list and the permission list may be comma-separated.
//!
//! `SCOPE` / `DELEGATION` / `API KEY` grants belong to the admin router —
//! this family returns `None` for them so they fall through.

use crate::ddl_ast::statement::{AuthStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    _trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("GRANT ") {
        if upper.starts_with("GRANT SCOPE ") {
            return None;
        }
        return Some(parse_grant_revoke(parts, true));
    }
    if upper.starts_with("REVOKE ")
        && !upper.starts_with("REVOKE SCOPE ")
        && !upper.starts_with("REVOKE DELEGATION ")
        && !upper.starts_with("REVOKE API KEY ")
    {
        return Some(parse_grant_revoke(parts, false));
    }
    None
}

/// Classify the object clause of a `GRANT/REVOKE ... ON ...` statement.
///
/// `after` is the token immediately following `ON`; `name_after` is the
/// token after that (consulted only when `after` is an explicit
/// object-type keyword). Returns `(target_type, target_name)`.
///
/// Object-type keywords (`FUNCTION`, `PROCEDURE`, `COLLECTION`, `TABLE`)
/// are matched explicitly so they can never be silently consumed as the
/// object name. `COLLECTION` and `TABLE` are accepted as the explicit
/// spelling of the default (collection) object type; a bare token with
/// no keyword is still treated as a collection name directly.
///
/// `DATABASE` and `TENANT` are not handled here — the caller intercepts
/// them first because they need different statement shapes.
pub(super) fn classify_object_clause(after: &str, name_after: Option<&str>) -> (String, String) {
    // Every name stays a raw token. The handler decodes it once, so a quoted
    // target keeps its case.
    if after.eq_ignore_ascii_case("FUNCTION") {
        (
            "FUNCTION".to_string(),
            name_after.map(|s| s.to_string()).unwrap_or_default(),
        )
    } else if after.eq_ignore_ascii_case("PROCEDURE") {
        (
            "PROCEDURE".to_string(),
            name_after.map(|s| s.to_string()).unwrap_or_default(),
        )
    } else if after.eq_ignore_ascii_case("COLLECTION") || after.eq_ignore_ascii_case("TABLE") {
        (
            "COLLECTION".to_string(),
            name_after.map(|s| s.to_string()).unwrap_or_default(),
        )
    } else {
        ("COLLECTION".to_string(), after.to_string())
    }
}

/// Split a run of whitespace-separated tokens into a comma-separated list,
/// trimming each item. `["READ,", "WRITE"]` → `["READ", "WRITE"]`,
/// `["CREATE", "COLLECTION"]` → `["CREATE COLLECTION"]`.
fn split_list(tokens: &[&str]) -> Vec<String> {
    tokens
        .join(" ")
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn parse_grant_revoke(parts: &[&str], is_grant: bool) -> Result<NodedbStatement, SqlError> {
    let pivot = if is_grant { "TO" } else { "FROM" };
    let kw = if is_grant { "GRANT" } else { "REVOKE" };

    let pivot_pos = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case(pivot))
        .ok_or_else(|| SqlError::Parse {
            detail: format!(
                "syntax: {kw} <role>[, ...] {pivot} <grantee> | \
                 {kw} <perm>[, ...] ON <object> {pivot} <grantee>"
            ),
        })?;

    let grantee = parts
        .get(pivot_pos + 1)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SqlError::Parse {
            detail: format!("{kw}: missing grantee after {pivot}"),
        })?;

    let on_pos = parts.iter().position(|p| p.eq_ignore_ascii_case("ON"));

    match on_pos {
        // Object-permission grant: an `ON` clause sits before the pivot.
        Some(on) if on < pivot_pos => {
            let permissions = split_list(&parts[1..on]);
            if permissions.is_empty() {
                return Err(SqlError::Parse {
                    detail: format!("{kw}: missing permission before ON"),
                });
            }
            let after = parts.get(on + 1).copied().unwrap_or_default();

            if after.eq_ignore_ascii_case("DATABASE") {
                // Database grants carry a single (possibly multi-word) privilege.
                let permission = parts[1..on].join(" ");
                let db_name = parts.get(on + 2).map(|s| s.to_string()).unwrap_or_default();
                return Ok(NodedbStatement::Auth(if is_grant {
                    AuthStmt::GrantDatabasePermission {
                        permission,
                        db_name,
                        grantee,
                    }
                } else {
                    AuthStmt::RevokeDatabasePermission {
                        permission,
                        db_name,
                        grantee,
                    }
                }));
            }

            if after.eq_ignore_ascii_case("TENANT") {
                // Tenant-scoped grant: the permission applies to every
                // collection in the named tenant. The handler resolves the
                // tenant name to its id.
                let tenant_name = parts
                    .get(on + 2)
                    .filter(|_| on + 2 < pivot_pos)
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| SqlError::Parse {
                        detail: format!("{kw}: missing tenant name after ON TENANT"),
                    })?;
                return Ok(NodedbStatement::Auth(if is_grant {
                    AuthStmt::GrantPermission {
                        permissions,
                        target_type: "TENANT".to_string(),
                        target_name: tenant_name,
                        grantee,
                    }
                } else {
                    AuthStmt::RevokePermission {
                        permissions,
                        target_type: "TENANT".to_string(),
                        target_name: tenant_name,
                        grantee,
                    }
                }));
            }

            let (target_type, target_name) =
                classify_object_clause(after, parts.get(on + 2).copied());

            Ok(NodedbStatement::Auth(if is_grant {
                AuthStmt::GrantPermission {
                    permissions,
                    target_type,
                    target_name,
                    grantee,
                }
            } else {
                AuthStmt::RevokePermission {
                    permissions,
                    target_type,
                    target_name,
                    grantee,
                }
            }))
        }

        // Role-membership grant: no `ON` clause.
        _ => {
            // Accept a leading `ROLE` keyword as an optional alias.
            let start = if parts
                .get(1)
                .map(|s| s.eq_ignore_ascii_case("ROLE"))
                .unwrap_or(false)
            {
                2
            } else {
                1
            };
            let roles = split_list(&parts[start.min(pivot_pos)..pivot_pos]);
            if roles.is_empty() {
                return Err(SqlError::Parse {
                    detail: format!("{kw}: missing role name before {pivot}"),
                });
            }
            Ok(NodedbStatement::Auth(if is_grant {
                AuthStmt::GrantRole { roles, grantee }
            } else {
                AuthStmt::RevokeRole { roles, grantee }
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
            .expect("expected Some")
            .expect("expected Ok")
    }

    #[test]
    fn grant_role_without_keyword() {
        match parse("GRANT tenant_admin TO eman") {
            NodedbStatement::Auth(AuthStmt::GrantRole { roles, grantee }) => {
                assert_eq!(roles, vec!["tenant_admin"]);
                assert_eq!(grantee, "eman");
            }
            other => panic!("expected GrantRole, got {other:?}"),
        }
    }

    #[test]
    fn grant_role_with_keyword_alias() {
        match parse("GRANT ROLE readwrite TO grace") {
            NodedbStatement::Auth(AuthStmt::GrantRole { roles, grantee }) => {
                assert_eq!(roles, vec!["readwrite"]);
                assert_eq!(grantee, "grace");
            }
            other => panic!("expected GrantRole, got {other:?}"),
        }
    }

    #[test]
    fn grant_comma_separated_roles() {
        match parse("GRANT readonly, readwrite TO multi") {
            NodedbStatement::Auth(AuthStmt::GrantRole { roles, .. }) => {
                assert_eq!(roles, vec!["readonly", "readwrite"]);
            }
            other => panic!("expected GrantRole, got {other:?}"),
        }
    }

    #[test]
    fn grant_comma_separated_permissions() {
        match parse("GRANT SELECT, INSERT ON orders TO analyst") {
            NodedbStatement::Auth(AuthStmt::GrantPermission {
                permissions,
                target_type,
                target_name,
                grantee,
            }) => {
                assert_eq!(permissions, vec!["SELECT", "INSERT"]);
                assert_eq!(target_type, "COLLECTION");
                assert_eq!(target_name, "orders");
                assert_eq!(grantee, "analyst");
            }
            other => panic!("expected GrantPermission, got {other:?}"),
        }
    }

    #[test]
    fn grant_on_procedure() {
        match parse("GRANT EXECUTE ON PROCEDURE transfer_funds TO data_engineer") {
            NodedbStatement::Auth(AuthStmt::GrantPermission {
                target_type,
                target_name,
                ..
            }) => {
                assert_eq!(target_type, "PROCEDURE");
                assert_eq!(target_name, "transfer_funds");
            }
            other => panic!("expected GrantPermission, got {other:?}"),
        }
    }

    #[test]
    fn grant_on_function() {
        match parse("GRANT EXECUTE ON FUNCTION full_name TO analyst") {
            NodedbStatement::Auth(AuthStmt::GrantPermission { target_type, .. }) => {
                assert_eq!(target_type, "FUNCTION");
            }
            other => panic!("expected GrantPermission, got {other:?}"),
        }
    }

    #[test]
    fn grant_on_database_multiword_privilege() {
        match parse("GRANT CREATE COLLECTION ON DATABASE prod TO alice") {
            NodedbStatement::Auth(AuthStmt::GrantDatabasePermission {
                permission,
                db_name,
                grantee,
            }) => {
                assert_eq!(permission, "CREATE COLLECTION");
                assert_eq!(db_name, "prod");
                assert_eq!(grantee, "alice");
            }
            other => panic!("expected GrantDatabasePermission, got {other:?}"),
        }
    }

    #[test]
    fn revoke_role_without_keyword() {
        match parse("REVOKE tenant_admin FROM demoter") {
            NodedbStatement::Auth(AuthStmt::RevokeRole { roles, grantee }) => {
                assert_eq!(roles, vec!["tenant_admin"]);
                assert_eq!(grantee, "demoter");
            }
            other => panic!("expected RevokeRole, got {other:?}"),
        }
    }

    #[test]
    fn revoke_permission_on_collection() {
        match parse("REVOKE INSERT ON orders FROM analyst") {
            NodedbStatement::Auth(AuthStmt::RevokePermission {
                permissions,
                target_name,
                ..
            }) => {
                assert_eq!(permissions, vec!["INSERT"]);
                assert_eq!(target_name, "orders");
            }
            other => panic!("expected RevokePermission, got {other:?}"),
        }
    }

    #[test]
    fn grant_scope_falls_through() {
        let upper = "GRANT SCOPE 'pro:all' TO ORG 'acme'".to_uppercase();
        let parts: Vec<&str> = "GRANT SCOPE 'pro:all' TO ORG 'acme'"
            .split_whitespace()
            .collect();
        assert!(try_parse(&upper, &parts, "").is_none());
    }

    #[test]
    fn grant_on_collection_keyword() {
        match parse("GRANT SELECT, INSERT ON COLLECTION chunks TO some_role") {
            NodedbStatement::Auth(AuthStmt::GrantPermission {
                permissions,
                target_type,
                target_name,
                grantee,
            }) => {
                assert_eq!(permissions, vec!["SELECT", "INSERT"]);
                assert_eq!(target_type, "COLLECTION");
                // The explicit `COLLECTION` object-type keyword must be
                // recognized, not consumed as the collection name itself.
                assert_ne!(target_name, "COLLECTION");
                assert_eq!(target_name, "chunks");
                assert_eq!(grantee, "some_role");
            }
            other => panic!("expected GrantPermission, got {other:?}"),
        }
    }

    #[test]
    fn grant_on_table_keyword() {
        match parse("GRANT SELECT ON TABLE orders TO analyst") {
            NodedbStatement::Auth(AuthStmt::GrantPermission {
                target_type,
                target_name,
                ..
            }) => {
                assert_eq!(target_type, "COLLECTION");
                assert_ne!(target_name, "TABLE");
                assert_eq!(target_name, "orders");
            }
            other => panic!("expected GrantPermission, got {other:?}"),
        }
    }

    #[test]
    fn revoke_on_collection_keyword() {
        match parse("REVOKE INSERT ON COLLECTION orders FROM analyst") {
            NodedbStatement::Auth(AuthStmt::RevokePermission {
                target_type,
                target_name,
                ..
            }) => {
                assert_eq!(target_type, "COLLECTION");
                assert_ne!(target_name, "COLLECTION");
                assert_eq!(target_name, "orders");
            }
            other => panic!("expected RevokePermission, got {other:?}"),
        }
    }

    #[test]
    fn grant_on_tenant_keyword() {
        match parse("GRANT BACKUP ON TENANT acme TO ops_user") {
            NodedbStatement::Auth(AuthStmt::GrantPermission {
                permissions,
                target_type,
                target_name,
                grantee,
            }) => {
                assert_eq!(permissions, vec!["BACKUP"]);
                assert_eq!(target_type, "TENANT");
                // The tenant name must be captured, not the `TENANT` keyword.
                assert_eq!(target_name, "acme");
                assert_eq!(grantee, "ops_user");
            }
            other => panic!("expected GrantPermission, got {other:?}"),
        }
    }

    #[test]
    fn revoke_on_tenant_keyword() {
        match parse("REVOKE SELECT, INSERT ON TENANT acme FROM ops_user") {
            NodedbStatement::Auth(AuthStmt::RevokePermission {
                permissions,
                target_type,
                target_name,
                ..
            }) => {
                assert_eq!(permissions, vec!["SELECT", "INSERT"]);
                assert_eq!(target_type, "TENANT");
                assert_eq!(target_name, "acme");
            }
            other => panic!("expected RevokePermission, got {other:?}"),
        }
    }

    #[test]
    fn grant_on_tenant_missing_name_is_error() {
        let sql = "GRANT BACKUP ON TENANT TO ops_user";
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // `TO` is the token after `TENANT`; with no name the pivot still
        // parses, so the tenant name resolves empty → explicit error.
        assert!(matches!(
            try_parse(&sql.to_uppercase(), &parts, sql),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn grant_missing_pivot_is_error() {
        let upper = "GRANT readonly".to_uppercase();
        let parts: Vec<&str> = "GRANT readonly".split_whitespace().collect();
        assert!(matches!(
            try_parse(&upper, &parts, ""),
            Some(Err(SqlError::Parse { .. }))
        ));
    }
}
