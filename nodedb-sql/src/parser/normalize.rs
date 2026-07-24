// SPDX-License-Identifier: Apache-2.0

//! SQL identifier normalization.

use crate::error::{Result, SqlError};
use crate::reserved::check_ast_identifier;

/// Human-readable message for schema-qualified name rejections.
/// Defined once so all rejection sites produce consistent output.
pub const SCHEMA_QUALIFIED_MSG: &str = "schema-qualified names are not supported; NodeDB has no schema concept \
     — use 'users' not 'public.users'";

/// Normalize a SQL identifier: lowercase unquoted, preserve quoted.
pub fn normalize_ident(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Normalize a compound object name, rejecting schema-qualified forms.
///
/// Accepts a single-part name (plain identifier) and returns it normalized.
/// Rejects any name with more than one part (e.g. `public.users`,
/// `db.public.users`) with `SqlError::Unsupported`.
pub fn normalize_object_name_checked(name: &sqlparser::ast::ObjectName) -> Result<String> {
    if name.0.len() > 1 {
        // Validate every component before reflecting it in an error. This keeps
        // malformed quoted/control-bearing identifiers out of diagnostics too.
        let qualified = name
            .0
            .iter()
            .map(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => check_ast_identifier(ident),
                _ => Err(SqlError::InvalidIdentifier {
                    name: String::new(),
                    reason: "object name must contain only identifiers",
                }),
            })
            .collect::<Result<Vec<_>>>()?
            .join(".");
        return Err(SqlError::Unsupported {
            detail: format!("'{qualified}': {SCHEMA_QUALIFIED_MSG}"),
        });
    }
    let part = name.0.first().ok_or_else(|| SqlError::InvalidIdentifier {
        name: String::new(),
        reason: "object name must contain an identifier",
    })?;
    match part {
        sqlparser::ast::ObjectNamePart::Identifier(ident) => check_ast_identifier(ident),
        _ => Err(SqlError::InvalidIdentifier {
            name: String::new(),
            reason: "object name must contain an identifier",
        }),
    }
}

/// Normalize a table name, accepting the two system-schema qualifiers that name
/// catalog relations (NodeDB has no user schemas, so any other qualifier is
/// rejected by [`normalize_object_name_checked`]):
///
/// - `pg_catalog.pg_class` → `pg_class` (the `pg_catalog` schema is implicit).
/// - `_system.audit_log` → `_system.audit_log` (the `_system.` prefix is part of
///   the catalog relation's name).
///
/// These are resolved like any other relation through `SqlCatalog::resolve_relation`.
fn normalize_table_name(name: &sqlparser::ast::ObjectName) -> Result<String> {
    if name.0.len() == 2 {
        let parts: Vec<String> = name
            .0
            .iter()
            .map(|part| match part {
                sqlparser::ast::ObjectNamePart::Identifier(ident) => check_ast_identifier(ident),
                _ => Err(SqlError::InvalidIdentifier {
                    name: String::new(),
                    reason: "object name must contain an identifier",
                }),
            })
            .collect::<Result<_>>()?;
        match parts[0].as_str() {
            "pg_catalog" => return Ok(parts[1].clone()),
            "_system" => return Ok(format!("_system.{}", parts[1])),
            _ => {}
        }
    }
    normalize_object_name_checked(name)
}

/// Extract table name and optional alias from a table factor.
///
/// Returns `Err` if the table name is schema-qualified with a non-system schema.
pub fn table_name_from_factor(
    factor: &sqlparser::ast::TableFactor,
) -> Result<Option<(String, Option<String>)>> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            let table = normalize_table_name(name)?;
            let alias_name = alias
                .as_ref()
                .map(|alias| check_ast_identifier(&alias.name))
                .transpose()?;
            Ok(Some((table, alias_name)))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;
    use sqlparser::ast::Statement;

    fn parse_table_factor(sql: &str) -> sqlparser::ast::TableFactor {
        let stmts = parse_sql(sql).expect("parse failed");
        let Statement::Query(q) = &stmts[0] else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
            panic!("expected select body");
        };
        sel.from[0].relation.clone()
    }

    fn parse_object_name(sql: &str) -> sqlparser::ast::ObjectName {
        match parse_table_factor(sql) {
            sqlparser::ast::TableFactor::Table { name, .. } => name,
            other => panic!("expected table factor, got {other:?}"),
        }
    }

    #[test]
    fn plain_name_accepted() {
        let name = parse_object_name("SELECT * FROM users");
        assert_eq!(normalize_object_name_checked(&name).unwrap(), "users");
    }

    #[test]
    fn schema_qualified_two_parts_rejected() {
        let name = parse_object_name("SELECT * FROM public.users");
        let err = normalize_object_name_checked(&name).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("public.users") || msg.contains("schema-qualified"),
            "error should mention the qualified name or schema: {msg}"
        );
    }

    #[test]
    fn schema_qualified_three_parts_rejected() {
        // db.public.users — three-part name.
        // sqlparser may not parse this as a table name with three parts in all dialects,
        // but we can verify via a manually constructed ObjectName.
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
        let name = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("db")),
            ObjectNamePart::Identifier(Ident::new("public")),
            ObjectNamePart::Identifier(Ident::new("users")),
        ]);
        let err = normalize_object_name_checked(&name).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }

    #[test]
    fn object_names_enforce_ast_identifier_rules() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let empty = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(""))]);
        assert!(matches!(
            normalize_object_name_checked(&empty),
            Err(SqlError::InvalidIdentifier { .. })
        ));

        let reserved = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("MATCH"))]);
        assert!(matches!(
            normalize_object_name_checked(&reserved),
            Err(SqlError::ReservedIdentifier { .. })
        ));

        let mut quoted = Ident::new("MiXeD 雪");
        quoted.quote_style = Some('"');
        let quoted_name = ObjectName(vec![ObjectNamePart::Identifier(quoted)]);
        assert_eq!(
            normalize_object_name_checked(&quoted_name).expect("quoted object name"),
            "MiXeD 雪"
        );

        let quote = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("a\"b"))]);
        assert!(matches!(
            normalize_object_name_checked(&quote),
            Err(SqlError::InvalidIdentifier { .. })
        ));
    }

    #[test]
    fn qualified_names_validate_components_before_rejection() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        for invalid_value in ["bad\nname", "a\"b"] {
            let mut invalid = Ident::new(invalid_value);
            invalid.quote_style = Some('"');
            let name = ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("public")),
                ObjectNamePart::Identifier(invalid),
            ]);
            assert!(matches!(
                normalize_object_name_checked(&name),
                Err(SqlError::InvalidIdentifier { .. })
            ));
        }
    }

    #[test]
    fn table_aliases_enforce_ast_identifier_rules() {
        use crate::planner::lateral::plan::lateral_alias_from_factor;

        let quoted = parse_table_factor("SELECT * FROM users AS \"MiXeD 雪\"");
        assert_eq!(
            table_name_from_factor(&quoted).expect("valid alias"),
            Some(("users".to_string(), Some("MiXeD 雪".to_string())))
        );

        for alias in ["\"\"", "\"a\"\"b\""] {
            let ordinary = parse_table_factor(&format!("SELECT * FROM users AS {alias}"));
            assert!(matches!(
                table_name_from_factor(&ordinary),
                Err(SqlError::InvalidIdentifier { .. })
            ));

            let lateral =
                parse_table_factor(&format!("SELECT * FROM LATERAL (SELECT 1) AS {alias}"));
            assert!(matches!(
                lateral_alias_from_factor(&lateral),
                Err(SqlError::InvalidIdentifier { .. })
            ));
        }
    }

    #[test]
    fn system_table_qualifiers_validate_each_identifier() {
        use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

        let system = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("_system")),
            ObjectNamePart::Identifier(Ident::new("audit_log")),
        ]);
        assert_eq!(
            normalize_table_name(&system).expect("system table"),
            "_system.audit_log"
        );

        let invalid = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("_system")),
            ObjectNamePart::Identifier(Ident::new("audit\nlog")),
        ]);
        assert!(matches!(
            normalize_table_name(&invalid),
            Err(SqlError::InvalidIdentifier { .. })
        ));
    }
}
