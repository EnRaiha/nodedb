// SPDX-License-Identifier: Apache-2.0

//! Parse `CREATE TYPE`, `DROP TYPE`, `ALTER TYPE`, and `SHOW TYPES`.
//!
//! Syntax:
//! - `CREATE TYPE <name> AS ENUM ('label1', 'label2', ...)`
//! - `CREATE TYPE <name> AS (<field1> <type1>, <field2> <type2>, ...)`
//! - `DROP TYPE [IF EXISTS] <name>`
//! - `ALTER TYPE <name> ADD VALUE 'label'`
//! - `SHOW TYPES`

use super::clause::{ClauseCursor, Tok, tokenize};
use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt};
use crate::error::SqlError;

/// Try to parse custom type DDL statements.
pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("CREATE TYPE ") {
        return Some(parse_create(trimmed));
    }
    if upper.starts_with("DROP TYPE ") || upper == "DROP TYPE" {
        return Some(parse_drop(trimmed));
    }
    if upper.starts_with("ALTER TYPE ") {
        return Some(parse_alter(trimmed));
    }
    if upper == "SHOW TYPES" {
        return Some(Ok(NodedbStatement::Policy(PolicyStmt::ShowTypes)));
    }
    None
}

/// Parse `CREATE TYPE <name> AS ENUM (...)` or `CREATE TYPE <name> AS (...)`.
fn parse_create(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "CREATE TYPE");
    c.expect_keywords(&["CREATE", "TYPE"])?;
    let name = c.name("type name")?;
    c.expect_keyword("AS")?;

    if c.eat_keyword("ENUM") {
        let labels = c.paren_list("ENUM label list", |c| {
            let label = c.string("ENUM label")?.trim().to_string();
            if label.is_empty() {
                return Err(c.error("an ENUM label must not be empty"));
            }
            Ok(label)
        })?;
        c.finish()?;
        let mut seen = std::collections::HashSet::new();
        for label in &labels {
            if !seen.insert(label.as_str()) {
                return Err(c.error(format!("AS ENUM: duplicate label '{label}'")));
            }
        }
        return Ok(NodedbStatement::Policy(PolicyStmt::CreateEnumType {
            name,
            labels,
        }));
    }

    let inner = c.raw_paren("composite field list")?;
    c.finish()?;
    let fields = parse_composite_fields(inner)?;
    if fields.is_empty() {
        return Err(c.error("AS: composite field list must not be empty"));
    }
    Ok(NodedbStatement::Policy(PolicyStmt::CreateCompositeType {
        name,
        fields,
    }))
}

/// Split `<field> <type>, ...` at top-level commas. A type keeps every word
/// and parenthesized argument (`DOUBLE PRECISION`, `NUMERIC(10, 2)`) and is
/// upper-cased.
fn parse_composite_fields(inner: &str) -> Result<Vec<(String, String)>, SqlError> {
    let toks = tokenize(inner);
    let mut fields = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    for i in 0..=toks.len() {
        let at_comma = match toks.get(i).map(|t| &t.tok) {
            Some(Tok::Punct('(')) => {
                depth += 1;
                false
            }
            Some(Tok::Punct(')')) => {
                depth -= 1;
                false
            }
            Some(Tok::Punct(',')) => depth == 0,
            Some(_) => false,
            None => true,
        };
        if !at_comma {
            continue;
        }
        let group = &toks[start..i];
        start = i + 1;
        if group.is_empty() && i == toks.len() && fields.is_empty() {
            break;
        }
        let (Some(first), Some(second), Some(last)) = (group.first(), group.get(1), group.last())
        else {
            let text = group
                .first()
                .zip(group.last())
                .map_or("", |(f, l)| &inner[f.span.start..l.span.end]);
            return Err(super::clause::parse_err(format!(
                "CREATE TYPE: field definition '{text}' must be '<name> <type>'"
            )));
        };
        let field_name = match &first.tok {
            Tok::Word(w) => w.to_lowercase(),
            Tok::Ident(s) => s.to_string(),
            _ => {
                return Err(super::clause::parse_err(format!(
                    "CREATE TYPE: field name '{}' is not an identifier",
                    &inner[first.span.clone()]
                )));
            }
        };
        let type_name = inner[second.span.start..last.span.end].to_uppercase();
        fields.push((field_name, type_name));
    }
    Ok(fields)
}

/// Parse `DROP TYPE [IF EXISTS] <name>`.
fn parse_drop(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "DROP TYPE");
    c.expect_keywords(&["DROP", "TYPE"])?;
    let if_exists = c.eat_keywords(&["IF", "EXISTS"]);
    let name = c.name("type name")?;
    c.finish()?;
    Ok(NodedbStatement::Policy(PolicyStmt::DropType {
        name,
        if_exists,
    }))
}

/// Parse `ALTER TYPE <name> ADD VALUE 'label'`. `IF NOT EXISTS`, `BEFORE`,
/// and `AFTER` are refused: the statement carries no slot for them, so
/// accepting them will change what the statement does.
fn parse_alter(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "ALTER TYPE");
    c.expect_keywords(&["ALTER", "TYPE"])?;
    let type_name = c.name("type name")?;
    if !c.eat_keywords(&["ADD", "VALUE"]) {
        return Err(c.error(format!(
            "unsupported action {}; expected ADD VALUE 'label'",
            c.describe_next()
        )));
    }
    if c.peek_keyword("IF") {
        return Err(c.error("ADD VALUE IF NOT EXISTS is not supported"));
    }
    let label = c.string("ADD VALUE")?;
    if label.is_empty() {
        return Err(c.error("ADD VALUE: label must not be empty"));
    }
    if c.peek_keyword("BEFORE") || c.peek_keyword("AFTER") {
        return Err(c.error("ADD VALUE ... BEFORE/AFTER is not supported; new labels are appended"));
    }
    c.finish()?;
    Ok(NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue {
        type_name,
        label,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn create_enum_with_unicode_name_preserves_original_offsets() {
        let stmt = ok("CREATE TYPE statusﬀﬀ AS ENUM ('active', 'inactive')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, labels }) => {
                assert_eq!(name, "statusﬀﬀ");
                assert_eq!(labels, vec!["active", "inactive"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_enum_basic() {
        let stmt = ok("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, labels }) => {
                assert_eq!(name, "status");
                assert_eq!(labels, vec!["active", "inactive", "pending"]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_enum_name_lowercased() {
        let stmt = ok("CREATE TYPE Status AS ENUM ('a', 'b')");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, .. }) => {
                assert_eq!(name, "status")
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_enum_empty_rejected() {
        let e = err("CREATE TYPE mood AS ENUM ()");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn create_enum_duplicate_rejected() {
        let e = err("CREATE TYPE mood AS ENUM ('happy', 'happy', 'sad')");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("duplicate"));
    }

    #[test]
    fn create_composite_with_unicode_name_preserves_original_offsets() {
        let stmt = ok("CREATE TYPE addressﬀﬀ AS (street TEXT, city TEXT)");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateCompositeType { name, fields }) => {
                assert_eq!(name, "addressﬀﬀ");
                assert_eq!(fields.len(), 2);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn create_composite_basic() {
        let stmt = ok("CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT)");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateCompositeType { name, fields }) => {
                assert_eq!(name, "address");
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "street");
                assert_eq!(fields[0].1, "TEXT");
                assert_eq!(fields[1].0, "city");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn composite_type_with_parenthesized_arguments_keeps_them() {
        let stmt = ok("CREATE TYPE money AS (amount NUMERIC(10, 2), unit DOUBLE PRECISION)");
        match stmt {
            NodedbStatement::Policy(PolicyStmt::CreateCompositeType { fields, .. }) => {
                assert_eq!(
                    fields,
                    vec![
                        ("amount".to_string(), "NUMERIC(10, 2)".to_string()),
                        ("unit".to_string(), "DOUBLE PRECISION".to_string()),
                    ]
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn drop_basic() {
        let stmt = ok("DROP TYPE status");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropType {
                name: "status".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn drop_if_exists() {
        let stmt = ok("DROP TYPE IF EXISTS status");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::DropType {
                name: "status".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn alter_unicode_type_add_value_preserves_original_offsets() {
        let stmt = ok("ALTER TYPE statusﬀﬀ ADD VALUE 'archived'");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue {
                type_name: "statusﬀﬀ".to_string(),
                label: "archived".to_string(),
            })
        );
    }

    #[test]
    fn alter_add_value() {
        let stmt = ok("ALTER TYPE status ADD VALUE 'archived'");
        assert_eq!(
            stmt,
            NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue {
                type_name: "status".to_string(),
                label: "archived".to_string(),
            })
        );
    }

    #[test]
    fn alter_add_value_before_is_refused_not_folded_into_the_label() {
        let e = err("ALTER TYPE mood ADD VALUE 'sad' BEFORE 'happy'");
        assert!(matches!(e, SqlError::Parse { .. }));
        let e = err("ALTER TYPE mood ADD VALUE IF NOT EXISTS 'sad'");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn alter_unsupported_action() {
        let e = err("ALTER TYPE status RENAME TO new_status");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn show_types() {
        let stmt = ok("SHOW TYPES");
        assert_eq!(stmt, NodedbStatement::Policy(PolicyStmt::ShowTypes));
    }
}
