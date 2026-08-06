// SPDX-License-Identifier: Apache-2.0

//! Strict parsing for column-redaction policy DDL.
//!
//! Grammar (keywords case-insensitive, identifiers bare or double-quoted):
//!
//! ```text
//! CREATE REDACTION POLICY [IF NOT EXISTS] <name> ON <collection>
//!     FOR ROLE <role> ( <field> <MODE> [, <field> <MODE> ...] ) [TENANT <id>]
//! DROP REDACTION POLICY [IF EXISTS] ON <collection> FOR ROLE <role> [TENANT <id>]
//! SHOW REDACTION POLICIES [ON <collection>] [TENANT <id>]
//! ```
//!
//! `<MODE>` is `MASK '<literal>'`, `HASH`, or `NULL`.
//!
//! The optional-clause conventions (`IF NOT EXISTS` / `IF EXISTS`, trailing
//! `TENANT <id>`) mirror the sibling RLS family rather than inventing new ones.

use super::policy_tokens::{
    consume_keyword, parse_error, parse_identifier, parse_optional_tenant, starts_keyword,
    starts_statement, statement_suffix,
};
use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt, RedactionRuleSpec};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if starts_statement(upper, "CREATE REDACTION POLICY") {
        Some(parse_create(trimmed))
    } else if starts_statement(upper, "DROP REDACTION POLICY") {
        Some(parse_drop(trimmed))
    } else if starts_statement(upper, "SHOW REDACTION POLICY")
        || starts_statement(upper, "SHOW REDACTION POLICIES")
    {
        Some(parse_show(trimmed))
    } else {
        None
    }
}

fn parse_create(sql: &str) -> Result<NodedbStatement, SqlError> {
    let mut rest = statement_suffix(sql, "CREATE REDACTION POLICY")?;
    let mut if_not_exists = false;
    if starts_keyword(rest, "IF") {
        rest = consume_keyword(rest, "IF")?;
        rest = consume_keyword(rest, "NOT")?;
        rest = consume_keyword(rest, "EXISTS")?;
        if_not_exists = true;
    }
    let (name, after_name) = parse_identifier(rest)?;
    rest = consume_keyword(after_name, "ON")?;
    let (collection, after_collection) = parse_identifier(rest)?;
    rest = consume_keyword(after_collection, "FOR")?;
    rest = consume_keyword(rest, "ROLE")?;
    let (for_role, after_role) = parse_identifier(rest)?;
    let (rules, after_rules) = parse_rule_list(after_role)?;
    let tenant_id_override = parse_optional_tenant(after_rules)?;

    Ok(NodedbStatement::Policy(PolicyStmt::CreateRedactionPolicy {
        name,
        collection,
        for_role,
        rules,
        if_not_exists,
        tenant_id_override,
    }))
}

fn parse_drop(sql: &str) -> Result<NodedbStatement, SqlError> {
    let mut rest = statement_suffix(sql, "DROP REDACTION POLICY")?;
    let mut if_exists = false;
    if starts_keyword(rest, "IF") {
        rest = consume_keyword(rest, "IF")?;
        rest = consume_keyword(rest, "EXISTS")?;
        if_exists = true;
    }
    rest = consume_keyword(rest, "ON")?;
    let (collection, after_collection) = parse_identifier(rest)?;
    rest = consume_keyword(after_collection, "FOR")?;
    rest = consume_keyword(rest, "ROLE")?;
    let (for_role, after_role) = parse_identifier(rest)?;
    rest = after_role.trim_start();
    if starts_keyword(rest, "IF") {
        if if_exists {
            return Err(parse_error("duplicate IF EXISTS clause"));
        }
        rest = consume_keyword(rest, "IF")?;
        rest = consume_keyword(rest, "EXISTS")?;
        if_exists = true;
    }
    let tenant_id_override = parse_optional_tenant(rest)?;

    Ok(NodedbStatement::Policy(PolicyStmt::DropRedactionPolicy {
        collection,
        for_role,
        if_exists,
        tenant_id_override,
    }))
}

fn parse_show(sql: &str) -> Result<NodedbStatement, SqlError> {
    let upper = sql.to_uppercase();
    let prefix = if starts_statement(&upper, "SHOW REDACTION POLICIES") {
        "SHOW REDACTION POLICIES"
    } else {
        "SHOW REDACTION POLICY"
    };
    let trimmed = sql.trim();
    let suffix = trimmed
        .get(prefix.len()..)
        .ok_or_else(|| parse_error(format!("expected {prefix}")))?;
    if !suffix.is_empty() && !suffix.chars().next().is_some_and(char::is_whitespace) {
        return Err(parse_error(format!("expected {prefix}")));
    }
    let mut rest = suffix.trim_start();

    let mut collection = None;
    if starts_keyword(rest, "ON") {
        rest = consume_keyword(rest, "ON")?;
        let (parsed, after_collection) = parse_identifier(rest)?;
        collection = Some(parsed);
        rest = after_collection;
    }
    let tenant_id_override = parse_optional_tenant(rest)?;

    Ok(NodedbStatement::Policy(PolicyStmt::ShowRedactionPolicies {
        collection,
        tenant_id_override,
    }))
}

/// Parse `( <field> <MODE> [, ...] )`, returning the rules and the remainder.
fn parse_rule_list(input: &str) -> Result<(Vec<RedactionRuleSpec>, &str), SqlError> {
    let mut rest = input.trim_start();
    if !rest.starts_with('(') {
        return Err(parse_error("expected '(' before the redaction rule list"));
    }
    rest = rest[1..].trim_start();

    let mut rules: Vec<RedactionRuleSpec> = Vec::new();
    loop {
        let (field, after_field) = parse_identifier(rest)?;
        let (mode_raw, mask, after_mode) = parse_mode(after_field)?;
        if rules.iter().any(|rule| rule.field == field) {
            return Err(parse_error(format!(
                "duplicate redaction rule for field '{field}'"
            )));
        }
        rules.push(RedactionRuleSpec {
            field,
            mode_raw,
            mask,
        });

        rest = after_mode.trim_start();
        if let Some(next) = rest.strip_prefix(',') {
            rest = next.trim_start();
            continue;
        }
        let Some(next) = rest.strip_prefix(')') else {
            return Err(parse_error(
                "expected ',' or ')' in the redaction rule list",
            ));
        };
        return Ok((rules, next));
    }
}

/// Parse one `<MODE>` token: `MASK '<literal>'`, `HASH`, or `NULL`.
fn parse_mode(input: &str) -> Result<(String, Option<String>, &str), SqlError> {
    let rest = input.trim_start();
    if starts_keyword(rest, "HASH") {
        return Ok(("HASH".to_string(), None, consume_keyword(rest, "HASH")?));
    }
    if starts_keyword(rest, "NULL") {
        return Ok(("NULL".to_string(), None, consume_keyword(rest, "NULL")?));
    }
    if starts_keyword(rest, "MASK") {
        let after_mask = consume_keyword(rest, "MASK")?;
        let (literal, remainder) = parse_string_literal(after_mask)?;
        return Ok(("MASK".to_string(), Some(literal), remainder));
    }
    Err(parse_error(
        "expected redaction mode MASK '<literal>', HASH, or NULL",
    ))
}

/// Parse a single-quoted string literal, honouring `''` as an escaped quote.
fn parse_string_literal(input: &str) -> Result<(String, &str), SqlError> {
    let mut rest = input
        .trim_start()
        .strip_prefix('\'')
        .ok_or_else(|| parse_error("MASK requires a quoted replacement literal"))?;
    let mut value = String::new();
    loop {
        let Some(ch) = rest.chars().next() else {
            return Err(parse_error("unterminated MASK literal"));
        };
        rest = &rest[ch.len_utf8()..];
        if ch == '\'' {
            match rest.strip_prefix('\'') {
                Some(next) => {
                    value.push('\'');
                    rest = next;
                }
                None => return Ok((value, rest)),
            }
        } else {
            value.push(ch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> PolicyStmt {
        let NodedbStatement::Policy(stmt) = parse_create(sql).expect("create parses") else {
            panic!("expected a policy statement")
        };
        stmt
    }

    #[test]
    fn create_parses_every_mode_and_the_tenant_override() {
        let stmt = create(
            "CREATE REDACTION POLICY mask_pii ON \"Sales Data\" FOR ROLE support \
             (email MASK '***@***.com', ssn HASH, notes NULL) TENANT 42",
        );
        let PolicyStmt::CreateRedactionPolicy {
            name,
            collection,
            for_role,
            rules,
            if_not_exists,
            tenant_id_override,
        } = stmt
        else {
            panic!("expected CreateRedactionPolicy")
        };
        assert_eq!(name, "mask_pii");
        assert_eq!(collection, "Sales Data");
        assert_eq!(for_role, "support");
        assert!(!if_not_exists);
        assert_eq!(tenant_id_override, Some(42));
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].field, "email");
        assert_eq!(rules[0].mode_raw, "MASK");
        assert_eq!(rules[0].mask.as_deref(), Some("***@***.com"));
        assert_eq!(rules[1].mode_raw, "HASH");
        assert!(rules[1].mask.is_none());
        assert_eq!(rules[2].mode_raw, "NULL");
    }

    #[test]
    fn create_accepts_if_not_exists_and_escaped_mask_literals() {
        let stmt = create(
            "CREATE REDACTION POLICY IF NOT EXISTS p ON users FOR ROLE support (nick MASK 'it''s')",
        );
        let PolicyStmt::CreateRedactionPolicy {
            rules,
            if_not_exists,
            ..
        } = stmt
        else {
            panic!("expected CreateRedactionPolicy")
        };
        assert!(if_not_exists);
        assert_eq!(rules[0].mask.as_deref(), Some("it's"));
    }

    #[test]
    fn drop_accepts_if_exists_in_either_position() {
        for sql in [
            "DROP REDACTION POLICY IF EXISTS ON users FOR ROLE support TENANT 7",
            "DROP REDACTION POLICY ON users FOR ROLE support IF EXISTS TENANT 7",
        ] {
            let NodedbStatement::Policy(PolicyStmt::DropRedactionPolicy {
                collection,
                for_role,
                if_exists,
                tenant_id_override,
            }) = parse_drop(sql).expect("drop parses")
            else {
                panic!("expected DropRedactionPolicy")
            };
            assert_eq!(collection, "users");
            assert_eq!(for_role, "support");
            assert!(if_exists);
            assert_eq!(tenant_id_override, Some(7));
        }
    }

    #[test]
    fn show_scopes_to_a_collection_and_a_tenant() {
        let NodedbStatement::Policy(PolicyStmt::ShowRedactionPolicies {
            collection,
            tenant_id_override,
        }) = parse_show("SHOW REDACTION POLICIES ON users TENANT 3").expect("show parses")
        else {
            panic!("expected ShowRedactionPolicies")
        };
        assert_eq!(collection.as_deref(), Some("users"));
        assert_eq!(tenant_id_override, Some(3));

        let NodedbStatement::Policy(PolicyStmt::ShowRedactionPolicies { collection, .. }) =
            parse_show("SHOW REDACTION POLICIES").expect("bare show parses")
        else {
            panic!("expected ShowRedactionPolicies")
        };
        assert!(collection.is_none());
    }

    #[test]
    fn parser_rejects_malformed_statements() {
        for sql in [
            "CREATE REDACTION POLICY p ON users FOR ROLE support (email MASK)",
            "CREATE REDACTION POLICY p ON users FOR ROLE support (email SCRAMBLE)",
            "CREATE REDACTION POLICY p ON users FOR ROLE support (email HASH",
            "CREATE REDACTION POLICY p ON users FOR ROLE support (email HASH, email NULL)",
            "CREATE REDACTION POLICY p ON users FOR support (email HASH)",
            "DROP REDACTION POLICY ON users FOR ROLE support TENANT",
            "SHOW REDACTION POLICIES TENANT 1 trailing",
        ] {
            assert!(
                try_parse(&sql.to_uppercase(), &[], sql)
                    .expect("recognized")
                    .is_err(),
                "{sql}"
            );
        }
    }
}
