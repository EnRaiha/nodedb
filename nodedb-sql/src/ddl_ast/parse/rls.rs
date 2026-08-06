// SPDX-License-Identifier: Apache-2.0

//! Strict parsing for RLS policy DDL.

use super::policy_tokens::{
    consume_keyword, is_identifier_char, parse_error, parse_identifier, parse_optional_tenant,
    parse_optional_tenant_prefix, starts_keyword, starts_statement, statement_suffix,
};
use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if starts_statement(upper, "CREATE RLS POLICY") {
        Some(parse_create(trimmed))
    } else if starts_statement(upper, "DROP RLS POLICY") {
        Some(parse_drop(trimmed))
    } else if starts_statement(upper, "SHOW RLS POLICY")
        || starts_statement(upper, "SHOW RLS POLICIES")
    {
        Some(parse_show(trimmed))
    } else {
        None
    }
}

fn parse_create(sql: &str) -> Result<NodedbStatement, SqlError> {
    let mut rest = statement_suffix(sql, "CREATE RLS POLICY")?;
    let (name, after_name) = parse_identifier(rest)?;
    rest = consume_keyword(after_name, "ON")?;
    let (collection, after_collection) = parse_identifier(rest)?;
    rest = consume_keyword(after_collection, "FOR")?;
    let (policy_type, after_type) = parse_keyword_value(rest, &["READ", "WRITE", "ALL"])?;
    rest = consume_keyword(after_type, "USING")?;
    let (predicate_raw, after_predicate) = parse_parenthesized(rest)?;
    let (is_restrictive, tenant_id_override, on_deny_raw) = parse_create_suffix(after_predicate)?;

    Ok(NodedbStatement::Policy(PolicyStmt::CreateRlsPolicy {
        name,
        collection,
        policy_type,
        predicate_raw,
        is_restrictive,
        on_deny_raw,
        tenant_id_override,
    }))
}

fn parse_drop(sql: &str) -> Result<NodedbStatement, SqlError> {
    let mut rest = statement_suffix(sql, "DROP RLS POLICY")?;
    let mut if_exists = false;
    if starts_keyword(rest, "IF") {
        rest = consume_keyword(rest, "IF")?;
        rest = consume_keyword(rest, "EXISTS")?;
        if_exists = true;
    }
    let (name, after_name) = parse_identifier(rest)?;
    rest = consume_keyword(after_name, "ON")?;
    let (collection, after_collection) = parse_identifier(rest)?;
    rest = after_collection.trim_start();
    if starts_keyword(rest, "IF") {
        if if_exists {
            return Err(parse_error("duplicate IF EXISTS clause"));
        }
        rest = consume_keyword(rest, "IF")?;
        rest = consume_keyword(rest, "EXISTS")?;
        if_exists = true;
    }
    let tenant_id_override = parse_optional_tenant(rest)?;

    Ok(NodedbStatement::Policy(PolicyStmt::DropRlsPolicy {
        name,
        collection,
        if_exists,
        tenant_id_override,
    }))
}

fn parse_show(sql: &str) -> Result<NodedbStatement, SqlError> {
    let prefix = if starts_statement(&sql.to_uppercase(), "SHOW RLS POLICIES") {
        "SHOW RLS POLICIES"
    } else {
        "SHOW RLS POLICY"
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

    Ok(NodedbStatement::Policy(PolicyStmt::ShowRlsPolicies {
        collection,
        tenant_id_override,
    }))
}

fn parse_create_suffix(input: &str) -> Result<(bool, Option<u64>, Option<String>), SqlError> {
    let mut rest = input.trim_start();
    let mut restrictive = false;
    if starts_keyword(rest, "RESTRICTIVE") {
        rest = consume_keyword(rest, "RESTRICTIVE")?;
        restrictive = true;
    }
    let (tenant_id_override, after_tenant) = parse_optional_tenant_prefix(rest)?;
    rest = after_tenant;

    let on_deny_raw = if rest.is_empty() || rest == ";" {
        None
    } else {
        rest = consume_keyword(rest, "ON")?;
        rest = consume_keyword(rest, "DENY")?;
        let raw = trim_statement_end(rest)?;
        if raw.is_empty() {
            return Err(parse_error("ON DENY requires a mode"));
        }
        Some(raw.to_string())
    };
    Ok((restrictive, tenant_id_override, on_deny_raw))
}

fn parse_keyword_value<'a>(
    input: &'a str,
    allowed: &[&str],
) -> Result<(String, &'a str), SqlError> {
    let end = input
        .char_indices()
        .find_map(|(index, ch)| (!is_identifier_char(ch)).then_some(index))
        .unwrap_or(input.len());
    let value = &input[..end];
    let normalized = value.to_uppercase();
    if !allowed.contains(&normalized.as_str()) {
        return Err(parse_error("expected READ, WRITE, or ALL"));
    }
    Ok((normalized, &input[end..]))
}

fn parse_parenthesized(input: &str) -> Result<(String, &str), SqlError> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(parse_error("expected '(' after USING"));
    }
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut index = 0usize;
    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .ok_or_else(|| parse_error("invalid predicate"))?;
        let next = index + ch.len_utf8();
        if let Some(delimiter) = quote {
            if ch == delimiter {
                if input[next..].starts_with(delimiter) {
                    index = next + delimiter.len_utf8();
                    continue;
                }
                quote = None;
            }
            index = next;
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| parse_error("USING predicate nesting overflow"))?;
            }
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| parse_error("unexpected ')' in USING predicate"))?;
                if depth == 0 {
                    return Ok((input[1..index].trim().to_string(), &input[next..]));
                }
            }
            _ => {}
        }
        index = next;
    }
    Err(parse_error(if quote.is_some() {
        "unterminated quoted predicate"
    } else {
        "missing ')' after USING predicate"
    }))
}

fn trim_statement_end(input: &str) -> Result<&str, SqlError> {
    let trimmed = input.trim();
    let mut quote: Option<char> = None;
    for (index, ch) in trimmed.char_indices() {
        if let Some(delimiter) = quote {
            if ch == delimiter {
                quote = None;
            }
        } else if matches!(ch, '\'' | '"') {
            quote = Some(ch);
        } else if ch == ';' && index + ch.len_utf8() != trimmed.len() {
            return Err(parse_error("unexpected statement separator"));
        }
    }
    if quote.is_some() {
        return Err(parse_error("unterminated ON DENY quoted text"));
    }
    Ok(trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_preserves_quoted_identifiers_and_balanced_predicate() {
        let statement = parse_create(
            "CREATE RLS POLICY \"Read Own\" ON \"Sales Data\" FOR READ USING ((owner = auth.user_id) AND note = 'x) y') RESTRICTIVE TENANT 42 ON DENY SILENT",
        )
        .expect("RLS CREATE parses");
        let NodedbStatement::Policy(PolicyStmt::CreateRlsPolicy {
            name,
            collection,
            predicate_raw,
            tenant_id_override,
            ..
        }) = statement
        else {
            panic!("expected create")
        };
        assert_eq!(name, "Read Own");
        assert_eq!(collection, "Sales Data");
        assert_eq!(predicate_raw, "(owner = auth.user_id) AND note = 'x) y'");
        assert_eq!(tenant_id_override, Some(42));
    }

    #[test]
    fn drop_and_show_preserve_tenant_override_and_quoted_names() {
        let drop = parse_drop("DROP RLS POLICY IF EXISTS \"Read Own\" ON \"Sales Data\" TENANT 42")
            .expect("drop parses");
        let NodedbStatement::Policy(PolicyStmt::DropRlsPolicy {
            name,
            collection,
            tenant_id_override,
            if_exists,
        }) = drop
        else {
            panic!("expected drop")
        };
        assert_eq!(
            (name, collection, tenant_id_override, if_exists),
            ("Read Own".into(), "Sales Data".into(), Some(42), true)
        );

        let trailing_if_exists =
            parse_drop("DROP RLS POLICY \"Read Own\" ON \"Sales Data\" IF EXISTS TENANT 42")
                .expect("trailing IF EXISTS parses");
        let NodedbStatement::Policy(PolicyStmt::DropRlsPolicy {
            if_exists,
            tenant_id_override,
            ..
        }) = trailing_if_exists
        else {
            panic!("expected drop")
        };
        assert!(if_exists);
        assert_eq!(tenant_id_override, Some(42));

        let show =
            parse_show("SHOW RLS POLICIES ON \"Sales Data\" TENANT 42").expect("show parses");
        let NodedbStatement::Policy(PolicyStmt::ShowRlsPolicies {
            collection,
            tenant_id_override,
        }) = show
        else {
            panic!("expected show")
        };
        assert_eq!(collection.as_deref(), Some("Sales Data"));
        assert_eq!(tenant_id_override, Some(42));
    }

    #[test]
    fn parser_rejects_malformed_or_ambiguous_suffixes() {
        for sql in [
            "CREATE RLS POLICY p ON c FOR READ USING (x = 1) TENANT",
            "CREATE RLS POLICY p ON c FOR READ USING (x = 1) TENANT 1 TENANT 2",
            "DROP RLS POLICY p ON c TENANT nope",
            "SHOW RLS POLICIES TENANT 1 trailing",
            "CREATE RLS POLICY p ON c FOR READ USING (x = 1) ON DENY",
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
