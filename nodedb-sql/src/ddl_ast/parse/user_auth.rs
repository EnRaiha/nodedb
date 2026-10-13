// SPDX-License-Identifier: Apache-2.0

//! Parse users/roles/permissions/grants + audit/tenants/constraints/typeguards.

use super::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{
    AlterRoleOp, AlterUserOp, AuthStmt, DatabaseStmt, MiscStmt, NodedbStatement, TenantSelector,
};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    let family = if upper.starts_with("CREATE USER ") {
        Family::CreateUser
    } else if upper.starts_with("DROP USER ") {
        Family::DropUser
    } else if upper.starts_with("ALTER USER ") {
        Family::AlterUser
    } else if upper.starts_with("SHOW USERS") {
        return Some(Ok(NodedbStatement::Auth(AuthStmt::ShowUsers)));
    } else if upper.starts_with("ALTER ROLE ") {
        Family::AlterRole
    } else if upper.starts_with("SHOW PERMISSIONS") {
        Family::ShowPermissions
    } else if upper.starts_with("SHOW GRANTS") {
        Family::ShowGrants
    } else if upper == "SHOW TENANTS" {
        // Only the bare form lands here. `SHOW TENANTS WITH NAME <name>` and
        // `SHOW TENANT <ident>` are parsed by `tenant.rs` into typed variants.
        return Some(Ok(NodedbStatement::Database(DatabaseStmt::ShowTenants)));
    } else if upper.starts_with("SHOW AUDIT") {
        // The audit router reads the filter clauses from the statement text.
        return Some(Ok(NodedbStatement::Misc(MiscStmt::ShowAuditLog)));
    } else if upper.starts_with("SHOW CONSTRAINTS ") {
        Family::ShowConstraints
    } else if upper.starts_with("SHOW TYPEGUARD") {
        Family::ShowTypeGuards
    } else {
        return None;
    };
    Some(parse(trimmed, family))
}

#[derive(Clone, Copy)]
enum Family {
    CreateUser,
    DropUser,
    AlterUser,
    AlterRole,
    ShowPermissions,
    ShowGrants,
    ShowConstraints,
    ShowTypeGuards,
}

fn parse(sql: &str, family: Family) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let stmt = match family {
        Family::CreateUser => {
            let mut c = ClauseCursor::new(sql, &toks, "CREATE USER");
            parse_create_user(&mut c)?
        }
        Family::DropUser => {
            let mut c = ClauseCursor::new(sql, &toks, "DROP USER");
            c.expect_keywords(&["DROP", "USER"])?;
            let username = c.raw("user name")?.to_string();
            c.finish()?;
            NodedbStatement::Auth(AuthStmt::DropUser { username })
        }
        Family::AlterUser => {
            let mut c = ClauseCursor::new(sql, &toks, "ALTER USER");
            parse_alter_user(&mut c)?
        }
        Family::AlterRole => {
            let mut c = ClauseCursor::new(sql, &toks, "ALTER ROLE");
            parse_alter_role(&mut c)?
        }
        Family::ShowPermissions => {
            // SHOW PERMISSIONS [ON <collection>] [FOR <grantee>]
            let mut c = ClauseCursor::new(sql, &toks, "SHOW PERMISSIONS");
            c.expect_keywords(&["SHOW", "PERMISSIONS"])?;
            let on_collection = if c.eat_keyword("ON") {
                Some(c.raw("collection")?.to_string())
            } else {
                None
            };
            let for_grantee = if c.eat_keyword("FOR") {
                Some(c.raw("grantee")?.to_string())
            } else {
                None
            };
            c.finish()?;
            NodedbStatement::Auth(AuthStmt::ShowPermissions {
                on_collection,
                for_grantee,
            })
        }
        Family::ShowGrants => {
            let mut c = ClauseCursor::new(sql, &toks, "SHOW GRANTS");
            c.expect_keywords(&["SHOW", "GRANTS"])?;
            c.eat_keyword("FOR");
            let username = if c.at_end() {
                None
            } else {
                Some(c.raw("user name")?.to_string())
            };
            c.finish()?;
            NodedbStatement::Auth(AuthStmt::ShowGrants { username })
        }
        Family::ShowConstraints => {
            let mut c = ClauseCursor::new(sql, &toks, "SHOW CONSTRAINTS");
            c.expect_keywords(&["SHOW", "CONSTRAINTS"])?;
            c.eat_keyword("ON");
            let collection = c.raw("collection")?.to_string();
            c.finish()?;
            NodedbStatement::Misc(MiscStmt::ShowConstraints { collection })
        }
        Family::ShowTypeGuards => {
            let mut c = ClauseCursor::new(sql, &toks, "SHOW TYPEGUARDS");
            c.expect_keyword("SHOW")?;
            if !c.eat_keyword("TYPEGUARDS") {
                c.expect_keyword("TYPEGUARD")?;
            }
            c.eat_keyword("ON");
            let collection = c.raw("collection")?.to_string();
            c.finish()?;
            NodedbStatement::Misc(MiscStmt::ShowTypeGuards { collection })
        }
    };
    Ok(stmt)
}

/// Parse `CREATE USER [IF NOT EXISTS] <name> [WITH] PASSWORD '<password>'
/// [ROLE <role>] [TENANT <id>|'<name>']`.
///
/// Extracts fields as primitive types; the handler converts role strings to
/// the `Role` enum and tenant IDs to `TenantId`. A keyword inside the
/// password string is part of the password, never a clause.
fn parse_create_user(c: &mut ClauseCursor<'_, '_>) -> Result<NodedbStatement, SqlError> {
    c.expect_keywords(&["CREATE", "USER"])?;
    let if_not_exists = c.eat_keywords(&["IF", "NOT", "EXISTS"]);
    let username = c.raw("user name")?.to_string();
    let with = c.eat_keyword("WITH");
    let password = if c.eat_keyword("PASSWORD") {
        c.string("PASSWORD")?
    } else if with {
        return Err(c.error(format!("expected PASSWORD — found {}", c.describe_next())));
    } else {
        String::new()
    };
    let mut role = None;
    let mut tenant = None;
    while !c.at_end() {
        if c.eat_keyword("ROLE") {
            c.clause("ROLE")?;
            role = Some(c.raw("ROLE")?.to_lowercase());
        } else if c.eat_keyword("TENANT") {
            c.clause("TENANT")?;
            tenant = Some(parse_tenant_selector(c.raw("TENANT")?));
        } else {
            return Err(c.unknown_clause());
        }
    }
    Ok(NodedbStatement::Auth(AuthStmt::CreateUser {
        username,
        password,
        role,
        tenant,
        if_not_exists,
    }))
}

/// Parse a tenant reference: a bare integer is an id, anything else
/// (optionally single-quoted) is a name.
fn parse_tenant_selector(token: &str) -> TenantSelector {
    match token.parse::<u64>() {
        Ok(id) => TenantSelector::Id(id),
        Err(_) => TenantSelector::Name(token.trim_matches('\'').to_string()),
    }
}

/// The next word upper-cased, consumed; empty at the end of the statement.
fn next_word_upper(c: &mut ClauseCursor<'_, '_>) -> String {
    match c.peek_word_upper() {
        Some(word) => {
            // A peeked word is always readable.
            if c.word("sub-command").is_ok() {
                word
            } else {
                String::new()
            }
        }
        None => String::new(),
    }
}

/// Parse all `ALTER USER <name> ...` forms.
///
/// Supported forms:
/// - `ALTER USER <name> SET PASSWORD '<password>'`
/// - `ALTER USER <name> SET ROLE <role>`
/// - `ALTER USER <name> SET DEFAULT DATABASE <db>`
/// - `ALTER USER <name> ROLE <role>` and `WITH ROLE <role>` (aliases for `SET ROLE`)
/// - `ALTER USER <name> MUST CHANGE PASSWORD`
/// - `ALTER USER <name> PASSWORD NEVER EXPIRES`
/// - `ALTER USER <name> PASSWORD EXPIRES '<iso8601>'`
/// - `ALTER USER <name> PASSWORD EXPIRES IN <N> DAYS`
///
/// Unknown sub-commands and trailing tokens return a parse error naming the
/// offending token — they are never rewritten into a default `AlterUserOp`.
fn parse_alter_user(c: &mut ClauseCursor<'_, '_>) -> Result<NodedbStatement, SqlError> {
    let syntax = |reason: &str| SqlError::Parse {
        detail: alter_user_syntax_msg(reason),
    };
    c.expect_keywords(&["ALTER", "USER"])?;
    if c.at_end() {
        return Err(syntax("missing user name"));
    }
    let username = c.raw("user name")?.to_string();
    if c.at_end() {
        return Err(syntax("ALTER USER requires a sub-command"));
    }
    let sub = next_word_upper(c);

    let op = match sub.as_str() {
        "SET" => match next_word_upper(c).as_str() {
            "PASSWORD" => AlterUserOp::SetPassword {
                password: c.string("SET PASSWORD")?,
            },
            "ROLE" => {
                if c.at_end() {
                    return Err(syntax("ALTER USER ... SET ROLE requires a role name"));
                }
                AlterUserOp::SetRole {
                    role: c.raw("role")?.to_string(),
                }
            }
            "DEFAULT" => {
                c.expect_keyword("DATABASE")?;
                AlterUserOp::SetDefaultDatabase {
                    db_name: c.raw("database name")?.to_string(),
                }
            }
            "" => {
                return Err(syntax(
                    "ALTER USER ... SET requires PASSWORD | ROLE | DEFAULT DATABASE",
                ));
            }
            other => {
                return Err(syntax(&format!(
                    "unknown ALTER USER ... SET action '{other}' \
                     (expected PASSWORD | ROLE | DEFAULT DATABASE)"
                )));
            }
        },
        // PostgreSQL-compatible alias: `ALTER USER <name> ROLE <role>`, the
        // parallel of `CREATE USER ... ROLE ...`.
        "ROLE" => {
            if c.at_end() {
                return Err(syntax("ALTER USER ... ROLE requires a role name"));
            }
            AlterUserOp::SetRole {
                role: c.raw("role")?.to_string(),
            }
        }
        "WITH" => {
            if !c.eat_keyword("ROLE") {
                return Err(syntax(&format!(
                    "unknown ALTER USER ... WITH clause {} (expected WITH ROLE <role>)",
                    c.describe_next()
                )));
            }
            if c.at_end() {
                return Err(syntax("ALTER USER ... WITH ROLE requires a role name"));
            }
            AlterUserOp::SetRole {
                role: c.raw("role")?.to_string(),
            }
        }
        "MUST" => {
            c.expect_keywords(&["CHANGE", "PASSWORD"])?;
            AlterUserOp::MustChangePassword
        }
        "PASSWORD" => match next_word_upper(c).as_str() {
            "NEVER" => {
                c.expect_keyword("EXPIRES")?;
                AlterUserOp::PasswordNeverExpires
            }
            "EXPIRES" => {
                if c.eat_keyword("IN") {
                    let days = c.u64("EXPIRES IN")?;
                    let days = u32::try_from(days)
                        .map_err(|_| syntax(&format!("EXPIRES IN {days} DAYS is out of range")))?;
                    c.expect_keyword("DAYS")?;
                    AlterUserOp::PasswordExpiresInDays { days }
                } else {
                    AlterUserOp::PasswordExpiresAt {
                        iso8601: c.string("PASSWORD EXPIRES")?,
                    }
                }
            }
            "" => {
                return Err(syntax(
                    "ALTER USER ... PASSWORD requires NEVER EXPIRES | EXPIRES ...",
                ));
            }
            other => {
                return Err(syntax(&format!(
                    "unknown ALTER USER ... PASSWORD clause '{other}' \
                     (expected NEVER EXPIRES | EXPIRES '<iso8601>' | EXPIRES IN <N> DAYS)"
                )));
            }
        },
        other => {
            return Err(syntax(&format!(
                "unknown ALTER USER sub-command '{}'",
                if other.is_empty() {
                    c.describe_next()
                } else {
                    other.to_string()
                }
            )));
        }
    };
    if !c.at_end() {
        return Err(syntax(&format!("unexpected {}", c.describe_next())));
    }

    Ok(NodedbStatement::Auth(AuthStmt::AlterUser { username, op }))
}

/// Build the canonical ALTER USER syntax message. The `reason` describes the
/// specific input that failed; the syntax block lists what's accepted.
fn alter_user_syntax_msg(reason: &str) -> String {
    format!(
        "{reason}. ALTER USER syntax: \
         ALTER USER <name> SET PASSWORD '<password>' | \
         ALTER USER <name> SET ROLE <role> | \
         ALTER USER <name> ROLE <role> | \
         ALTER USER <name> MUST CHANGE PASSWORD | \
         ALTER USER <name> PASSWORD NEVER EXPIRES | \
         ALTER USER <name> PASSWORD EXPIRES '<iso8601>' | \
         ALTER USER <name> PASSWORD EXPIRES IN <N> DAYS"
    )
}

/// Parse `ALTER ROLE <name> GRANT/REVOKE/SET ...`.
///
/// Supported forms (object type is `FUNCTION`, `PROCEDURE`, `COLLECTION`,
/// or `TABLE`; a bare name with no keyword is a collection):
/// - `ALTER ROLE <name> GRANT <perm> ON [<object-type>] <target>`
/// - `ALTER ROLE <name> REVOKE <perm> ON [<object-type>] <target>`
/// - `ALTER ROLE <name> SET INHERIT <parent>`
fn parse_alter_role(c: &mut ClauseCursor<'_, '_>) -> Result<NodedbStatement, SqlError> {
    let syntax = |reason: &str| SqlError::Parse {
        detail: alter_role_syntax_msg(reason),
    };
    c.expect_keywords(&["ALTER", "ROLE"])?;
    if c.at_end() {
        return Err(syntax("missing role name"));
    }
    let name = c.raw("role name")?.to_string();
    if c.at_end() {
        return Err(syntax("ALTER ROLE requires a sub-command"));
    }
    let sub_cmd = next_word_upper(c);

    let sub_op = match sub_cmd.as_str() {
        "GRANT" | "REVOKE" => {
            let permission = c.raw("permission")?.to_string();
            c.expect_keyword("ON")?;
            let (target_type, target_name) = super::grant::object_clause(c)?;
            if sub_cmd == "GRANT" {
                AlterRoleOp::Grant {
                    permission,
                    target_type,
                    target_name,
                }
            } else {
                AlterRoleOp::Revoke {
                    permission,
                    target_type,
                    target_name,
                }
            }
        }
        "SET" => {
            if !c.eat_keyword("INHERIT") {
                return Err(syntax(&format!(
                    "unknown ALTER ROLE ... SET clause {} (expected SET INHERIT <parent>)",
                    c.describe_next()
                )));
            }
            if c.at_end() {
                return Err(syntax(
                    "ALTER ROLE ... SET INHERIT requires a parent role name",
                ));
            }
            AlterRoleOp::SetInherit {
                parent: c.raw("parent role")?.to_string(),
            }
        }
        other => {
            return Err(syntax(&format!(
                "unknown ALTER ROLE sub-command '{}'",
                if other.is_empty() {
                    c.describe_next()
                } else {
                    other.to_string()
                }
            )));
        }
    };
    if !c.at_end() {
        return Err(syntax(&format!("unexpected {}", c.describe_next())));
    }

    Ok(NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }))
}

fn alter_role_syntax_msg(reason: &str) -> String {
    format!(
        "{reason}. ALTER ROLE syntax: \
         ALTER ROLE <name> GRANT <perm> ON [<object-type>] <target> | \
         ALTER ROLE <name> REVOKE <perm> ON [<object-type>] <target> | \
         ALTER ROLE <name> SET INHERIT <parent>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_sql(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
    }

    fn parse(sql: &str) -> Option<NodedbStatement> {
        try_sql(sql).map(|r| r.expect("parses"))
    }

    #[test]
    fn create_user_basic() {
        let stmt = parse("CREATE USER alice WITH PASSWORD 'secret' ROLE read_write").unwrap();
        if let NodedbStatement::Auth(AuthStmt::CreateUser {
            username,
            password,
            role,
            tenant,
            if_not_exists,
        }) = stmt
        {
            assert_eq!(username, "alice");
            assert_eq!(password, "secret");
            assert_eq!(role.as_deref(), Some("read_write"));
            assert!(tenant.is_none());
            assert!(!if_not_exists);
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn create_user_if_not_exists() {
        let stmt = parse("CREATE USER IF NOT EXISTS alice WITH PASSWORD 'secret' ROLE read_write")
            .unwrap();
        if let NodedbStatement::Auth(AuthStmt::CreateUser {
            username,
            if_not_exists,
            ..
        }) = stmt
        {
            // The `IF NOT EXISTS` keywords must not be consumed as the
            // username — `alice` is the real name.
            assert_eq!(username, "alice");
            assert!(if_not_exists);
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn create_user_no_role() {
        let stmt = parse("CREATE USER bob WITH PASSWORD 'pw123'").unwrap();
        if let NodedbStatement::Auth(AuthStmt::CreateUser { username, role, .. }) = stmt {
            assert_eq!(username, "bob");
            assert!(role.is_none());
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn create_user_with_tenant() {
        let stmt = parse("CREATE USER carol WITH PASSWORD 'pw' TENANT 42").unwrap();
        if let NodedbStatement::Auth(AuthStmt::CreateUser { tenant, .. }) = stmt {
            assert_eq!(tenant, Some(TenantSelector::Id(42)));
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn create_user_with_tenant_by_name() {
        let stmt = parse("CREATE USER dave WITH PASSWORD 'pw' TENANT 'acme'").unwrap();
        if let NodedbStatement::Auth(AuthStmt::CreateUser { tenant, .. }) = stmt {
            assert_eq!(tenant, Some(TenantSelector::Name("acme".to_string())));
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn clause_words_inside_the_password_are_password_text() {
        let stmt = parse("CREATE USER bob WITH PASSWORD 'x tenant 7 role superuser y'").unwrap();
        if let NodedbStatement::Auth(AuthStmt::CreateUser {
            password,
            role,
            tenant,
            ..
        }) = stmt
        {
            assert_eq!(password, "x tenant 7 role superuser y");
            assert!(role.is_none());
            assert!(tenant.is_none());
        } else {
            panic!("expected CreateUser");
        }
    }

    #[test]
    fn create_user_trailing_tokens_are_refused() {
        assert!(matches!(
            try_sql("CREATE USER bob WITH PASSWORD 'pw' ROLE admin SUPERUSER"),
            Some(Err(_))
        ));
    }

    #[test]
    fn alter_user_set_password() {
        let stmt = parse("ALTER USER alice SET PASSWORD 'newpass'").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::SetPassword {
                    password: "newpass".to_string()
                },
            })
        );
    }

    #[test]
    fn alter_user_set_role() {
        let stmt = parse("ALTER USER alice SET ROLE admin").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::SetRole {
                    role: "admin".to_string()
                },
            })
        );
    }

    #[test]
    fn alter_user_must_change_password() {
        let stmt = parse("ALTER USER alice MUST CHANGE PASSWORD").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::MustChangePassword,
            })
        );
    }

    #[test]
    fn alter_user_password_never_expires() {
        let stmt = parse("ALTER USER alice PASSWORD NEVER EXPIRES").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::PasswordNeverExpires,
            })
        );
    }

    #[test]
    fn alter_user_password_expires_at() {
        let stmt = parse("ALTER USER alice PASSWORD EXPIRES '2026-12-31T00:00:00Z'").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::PasswordExpiresAt {
                    iso8601: "2026-12-31T00:00:00Z".to_string()
                },
            })
        );
    }

    #[test]
    fn alter_user_password_expires_in_days() {
        let stmt = parse("ALTER USER alice PASSWORD EXPIRES IN 90 DAYS").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::AlterUser {
                username: "alice".to_string(),
                op: AlterUserOp::PasswordExpiresInDays { days: 90 },
            })
        );
    }

    #[test]
    fn alter_user_non_numeric_days_is_refused() {
        // A bad day count is never read as 0.
        assert!(matches!(
            try_sql("ALTER USER alice PASSWORD EXPIRES IN ninety DAYS"),
            Some(Err(_))
        ));
    }

    // ── ALTER ROLE tests ─────────────────────────────────────────────

    #[test]
    fn alter_role_set_inherit() {
        let stmt = parse("ALTER ROLE analyst SET INHERIT readonly").unwrap();
        if let NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::SetInherit {
                    parent: "readonly".to_string()
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_grant_collection() {
        let stmt = parse("ALTER ROLE analyst GRANT READ ON my_collection").unwrap();
        if let NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Grant {
                    permission: "READ".to_string(),
                    target_type: "COLLECTION".to_string(),
                    target_name: "my_collection".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_grant_on_collection_keyword() {
        let stmt = parse("ALTER ROLE analyst GRANT READ ON COLLECTION my_collection").unwrap();
        if let NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) = stmt {
            assert_eq!(name, "analyst");
            // The explicit `COLLECTION` object-type keyword must be
            // recognized, not consumed as the collection name itself.
            assert_eq!(
                sub_op,
                AlterRoleOp::Grant {
                    permission: "READ".to_string(),
                    target_type: "COLLECTION".to_string(),
                    target_name: "my_collection".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_revoke_on_collection_keyword() {
        let stmt = parse("ALTER ROLE analyst REVOKE WRITE ON COLLECTION orders").unwrap();
        if let NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Revoke {
                    permission: "WRITE".to_string(),
                    target_type: "COLLECTION".to_string(),
                    target_name: "orders".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_grant_function() {
        let stmt = parse("ALTER ROLE analyst GRANT EXECUTE ON FUNCTION my_func").unwrap();
        if let NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Grant {
                    permission: "EXECUTE".to_string(),
                    target_type: "FUNCTION".to_string(),
                    target_name: "my_func".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_revoke_collection() {
        let stmt = parse("ALTER ROLE analyst REVOKE WRITE ON orders").unwrap();
        if let NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Revoke {
                    permission: "WRITE".to_string(),
                    target_type: "COLLECTION".to_string(),
                    target_name: "orders".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_revoke_function() {
        let stmt = parse("ALTER ROLE analyst REVOKE EXECUTE ON FUNCTION calc").unwrap();
        if let NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) = stmt {
            assert_eq!(name, "analyst");
            assert_eq!(
                sub_op,
                AlterRoleOp::Revoke {
                    permission: "EXECUTE".to_string(),
                    target_type: "FUNCTION".to_string(),
                    target_name: "calc".to_string(),
                }
            );
        } else {
            panic!("expected AlterRole");
        }
    }

    #[test]
    fn alter_role_grant_without_on_is_refused() {
        assert!(matches!(
            try_sql("ALTER ROLE analyst GRANT READ my_collection"),
            Some(Err(_))
        ));
    }

    // ── SHOW PERMISSIONS tests ────────────────────────────────────────

    #[test]
    fn show_permissions_no_filter() {
        let stmt = parse("SHOW PERMISSIONS").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::ShowPermissions {
                on_collection: None,
                for_grantee: None,
            })
        );
    }

    #[test]
    fn show_permissions_on_collection() {
        let stmt = parse("SHOW PERMISSIONS ON orders").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::ShowPermissions {
                on_collection: Some("orders".to_string()),
                for_grantee: None,
            })
        );
    }

    #[test]
    fn show_permissions_for_user() {
        let stmt = parse("SHOW PERMISSIONS FOR alice").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::ShowPermissions {
                on_collection: None,
                for_grantee: Some("alice".to_string()),
            })
        );
    }

    #[test]
    fn show_permissions_on_and_for() {
        let stmt = parse("SHOW PERMISSIONS ON orders FOR alice").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::ShowPermissions {
                on_collection: Some("orders".to_string()),
                for_grantee: Some("alice".to_string()),
            })
        );
    }

    #[test]
    fn show_constraints_on_names_the_collection() {
        let stmt = parse("SHOW CONSTRAINTS ON orders").unwrap();
        assert_eq!(
            stmt,
            NodedbStatement::Misc(MiscStmt::ShowConstraints {
                collection: "orders".to_string()
            })
        );
    }
}
