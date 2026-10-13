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

use super::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{AuthStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("GRANT ") {
        if upper.starts_with("GRANT SCOPE ") {
            return None;
        }
        return Some(parse_grant_revoke(trimmed, true));
    }
    if upper.starts_with("REVOKE ")
        && !upper.starts_with("REVOKE SCOPE ")
        && !upper.starts_with("REVOKE DELEGATION ")
        && !upper.starts_with("REVOKE API KEY ")
    {
        return Some(parse_grant_revoke(trimmed, false));
    }
    None
}

/// Read the object clause after `ON`: `FUNCTION <name>`, `PROCEDURE <name>`,
/// `COLLECTION <name>`, `TABLE <name>`, or a bare collection name. Returns
/// `(target_type, target_name)`.
///
/// Object-type keywords are matched explicitly so they are never consumed as
/// the object name. Every name stays a raw token; the handler decodes it
/// once, so a quoted target keeps its case.
///
/// `DATABASE` and `TENANT` are not handled here — the caller intercepts
/// them first because they need different statement shapes.
pub(super) fn object_clause(c: &mut ClauseCursor<'_, '_>) -> Result<(String, String), SqlError> {
    let target_type = if c.eat_keyword("FUNCTION") {
        "FUNCTION"
    } else if c.eat_keyword("PROCEDURE") {
        "PROCEDURE"
    } else {
        // `COLLECTION` and `TABLE` spell the default object type.
        if !c.eat_keyword("COLLECTION") {
            c.eat_keyword("TABLE");
        }
        "COLLECTION"
    };
    // A dotted name (`_system.audit_log`, `db.coll`) is one object: its
    // tokens are written with no whitespace between them.
    let target_name = c.raw_run("object name")?.to_string();
    Ok((target_type.to_string(), target_name))
}

/// Read a comma-separated list of items. Each item is one or more words,
/// joined by one space (`CREATE COLLECTION`). The list ends before any of
/// `stops`.
fn item_list(c: &mut ClauseCursor<'_, '_>, stops: &[&str]) -> Result<Vec<String>, SqlError> {
    let mut items = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    while !c.at_end() && !stops.iter().any(|s| c.peek_keyword(s)) {
        if c.eat_punct(',') {
            if current.is_empty() {
                return Err(c.error("empty item in list"));
            }
            items.push(current.join(" "));
            current.clear();
            continue;
        }
        current.push(c.raw("list item")?);
    }
    if !current.is_empty() {
        items.push(current.join(" "));
    } else if !items.is_empty() {
        return Err(c.error("trailing ',' in list"));
    }
    Ok(items)
}

fn parse_grant_revoke(sql: &str, is_grant: bool) -> Result<NodedbStatement, SqlError> {
    let (kw, pivot) = if is_grant {
        ("GRANT", "TO")
    } else {
        ("REVOKE", "FROM")
    };
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, if is_grant { "GRANT" } else { "REVOKE" });
    c.expect_keyword(kw)?;
    let role_keyword = c.eat_keyword("ROLE");
    let items = item_list(&mut c, &["ON", pivot])?;

    if !role_keyword && c.eat_keyword("ON") {
        if items.is_empty() {
            return Err(c.error("missing permission before ON"));
        }
        if c.eat_keyword("DATABASE") {
            // Database grants carry a single (possibly multi-word) privilege.
            let permission = items.join(", ");
            let db_name = c.raw("database name")?.to_string();
            let grantee = read_grantee(&mut c, pivot)?;
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
        let (target_type, target_name) = if c.eat_keyword("TENANT") {
            // The permission applies to every collection in the named
            // tenant. The handler resolves the tenant name to its id.
            if c.peek_keyword(pivot) {
                return Err(c.error("missing tenant name after ON TENANT"));
            }
            ("TENANT".to_string(), c.raw("tenant name")?.to_string())
        } else {
            object_clause(&mut c)?
        };
        let grantee = read_grantee(&mut c, pivot)?;
        return Ok(NodedbStatement::Auth(if is_grant {
            AuthStmt::GrantPermission {
                permissions: items,
                target_type,
                target_name,
                grantee,
            }
        } else {
            AuthStmt::RevokePermission {
                permissions: items,
                target_type,
                target_name,
                grantee,
            }
        }));
    }

    // Role-membership grant: no `ON` clause.
    if items.is_empty() {
        return Err(c.error(format!("missing role name before {pivot}")));
    }
    let grantee = read_grantee(&mut c, pivot)?;
    Ok(NodedbStatement::Auth(if is_grant {
        AuthStmt::GrantRole {
            roles: items,
            grantee,
        }
    } else {
        AuthStmt::RevokeRole {
            roles: items,
            grantee,
        }
    }))
}

/// Read `TO|FROM <grantee>` and require the end of the statement. One
/// grantee per statement: a list or trailing words are refused, never
/// silently dropped.
fn read_grantee(c: &mut ClauseCursor<'_, '_>, pivot: &str) -> Result<String, SqlError> {
    if !c.eat_keyword(pivot) {
        return Err(c.error(format!(
            "syntax: <role>[, ...] {pivot} <grantee> | <perm>[, ...] ON <object> {pivot} \
             <grantee> — found {}",
            c.describe_next()
        )));
    }
    let grantee = c.raw("grantee")?.to_string();
    c.finish()?;
    Ok(grantee)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_sql(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
    }

    fn parse(sql: &str) -> NodedbStatement {
        try_sql(sql).expect("expected Some").expect("expected Ok")
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
    fn grant_on_a_dotted_object_reads_the_whole_name() {
        match parse("GRANT SELECT ON _system.audit_log TO auditor") {
            NodedbStatement::Auth(AuthStmt::GrantPermission {
                target_type,
                target_name,
                grantee,
                ..
            }) => {
                assert_eq!(target_type, "COLLECTION");
                assert_eq!(target_name, "_system.audit_log");
                assert_eq!(grantee, "auditor");
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
        assert!(try_sql("GRANT SCOPE 'pro:all' TO ORG 'acme'").is_none());
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
        assert!(matches!(
            try_sql("GRANT BACKUP ON TENANT TO ops_user"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn grant_missing_pivot_is_error() {
        assert!(matches!(
            try_sql("GRANT readonly"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn trailing_grantee_tokens_are_refused() {
        // `bob` is not silently dropped: the statement names one grantee.
        assert!(matches!(
            try_sql("GRANT READ ON orders TO alice bob"),
            Some(Err(SqlError::Parse { .. }))
        ));
        assert!(matches!(
            try_sql("REVOKE WRITE ON orders FROM alice CASCADE"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn extra_object_tokens_are_refused() {
        assert!(matches!(
            try_sql("GRANT READ ON orders extra TO alice"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }
}
