// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/SHOW RETENTION POLICY.

use crate::ddl_ast::statement::{NodedbStatement, PolicyStmt};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE RETENTION POLICY ") {
            return Ok(Some(parse_create_retention_policy(trimmed)?));
        }
        if upper.starts_with("DROP RETENTION POLICY ") {
            return Ok(Some(parse_drop_retention_policy(trimmed)?));
        }
        if upper.starts_with("ALTER RETENTION POLICY ") {
            return Ok(Some(parse_alter_retention_policy(trimmed)?));
        }
        if upper.starts_with("SHOW RETENTION POLIC") {
            return Ok(Some(NodedbStatement::Policy(
                PolicyStmt::ShowRetentionPolicies,
            )));
        }
        let _ = parts;
        Ok(None)
    })()
    .transpose()
}

fn parse_drop_retention_policy(trimmed: &str) -> Result<NodedbStatement, SqlError> {
    const PREFIX: &str = "DROP RETENTION POLICY";
    let mut rest = statement_suffix(trimmed, PREFIX)?;
    let mut if_exists = false;
    if starts_keyword(rest, "IF") {
        rest = consume_keyword(rest, "IF")?;
        rest = consume_keyword(rest, "EXISTS")?;
        if_exists = true;
    }
    let (name, remainder) = parse_identifier_token(rest)?;
    let remainder = remainder.trim();
    let remainder = if remainder.is_empty() || remainder == ";" {
        remainder
    } else {
        let after_on = consume_keyword(remainder, "ON")?;
        let (_, trailing) = parse_identifier_token(after_on)?;
        let trailing = trailing.trim();
        if !trailing.is_empty() && trailing != ";" {
            return Err(parse_error(
                "unexpected trailing tokens after retention policy name",
            ));
        }
        trailing
    };
    let _ = remainder;
    Ok(NodedbStatement::Policy(PolicyStmt::DropRetentionPolicy {
        name,
        if_exists,
    }))
}

fn parse_alter_retention_policy(trimmed: &str) -> Result<NodedbStatement, SqlError> {
    const PREFIX: &str = "ALTER RETENTION POLICY";
    let mut rest = statement_suffix(trimmed, PREFIX)?;
    let (name, remainder) = parse_identifier_token(rest)?;
    rest = remainder.trim_start();
    if starts_keyword(rest, "ON") {
        rest = consume_keyword(rest, "ON")?;
        let (_, remainder) = parse_identifier_token(rest)?;
        rest = remainder.trim_start();
    }

    let (action, set_key, set_value) = if starts_keyword(rest, "ENABLE") {
        let trailing = consume_keyword(rest, "ENABLE")?.trim();
        ensure_statement_end(trailing)?;
        ("ENABLE".to_string(), None, None)
    } else if starts_keyword(rest, "DISABLE") {
        let trailing = consume_keyword(rest, "DISABLE")?.trim();
        ensure_statement_end(trailing)?;
        ("DISABLE".to_string(), None, None)
    } else if starts_keyword(rest, "SET") {
        rest = consume_keyword(rest, "SET")?;
        let (key, remainder) = parse_identifier_token(rest)?;
        let remainder = remainder.trim_start();
        let value_input = remainder
            .strip_prefix('=')
            .ok_or_else(|| parse_error("expected '=' after retention policy SET key"))?
            .trim_start();
        let quoted_value = value_input.starts_with('\'');
        let (value, trailing) = parse_set_value(value_input)?;
        ensure_statement_end(trailing.trim())?;
        let key = key.to_uppercase();
        if key == "AUTO_TIER"
            && (quoted_value
                || (!value.eq_ignore_ascii_case("TRUE") && !value.eq_ignore_ascii_case("FALSE")))
        {
            return Err(parse_error(
                "AUTO_TIER must be an unquoted TRUE or FALSE literal",
            ));
        }
        ("SET".to_string(), Some(key), Some(value))
    } else {
        return Err(parse_error("expected ENABLE, DISABLE, or SET"));
    };

    Ok(NodedbStatement::Policy(PolicyStmt::AlterRetentionPolicy {
        name,
        action,
        set_key,
        set_value,
    }))
}

fn statement_suffix<'a>(input: &'a str, prefix: &str) -> Result<&'a str, SqlError> {
    let input = input.trim();
    let matched = input
        .get(..prefix.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .ok_or_else(|| parse_error(format!("expected {prefix}")))?;
    let rest = &input[matched.len()..];
    if !rest.chars().next().is_some_and(char::is_whitespace) {
        return Err(parse_error("expected retention policy name"));
    }
    Ok(rest.trim_start())
}

fn starts_keyword(input: &str, keyword: &str) -> bool {
    input
        .get(..keyword.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
        && !input[keyword.len()..]
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch == '$' || ch.is_alphanumeric())
}

fn ensure_statement_end(input: &str) -> Result<(), SqlError> {
    if input.is_empty() || input == ";" {
        Ok(())
    } else {
        Err(parse_error(
            "unexpected trailing tokens in retention policy statement",
        ))
    }
}

fn parse_set_value(input: &str) -> Result<(String, &str), SqlError> {
    if input.starts_with('\'') {
        return parse_single_quoted_string(input);
    }
    let end = input
        .char_indices()
        .find_map(|(index, ch)| (ch.is_whitespace() || ch == ';').then_some(index))
        .unwrap_or(input.len());
    let value = &input[..end];
    if value.is_empty() {
        return Err(parse_error("missing retention policy SET value"));
    }
    Ok((value.to_string(), &input[end..]))
}

/// Structural extraction for `CREATE RETENTION POLICY`.
///
/// Extracts name, collection, raw body (between the body parentheses), and an
/// optional `EVAL_INTERVAL`.  This is deliberately a complete parser for the
/// outer statement shape: forwarding a partially decoded statement to the
/// retention handler would make malformed suffixes indistinguishable from an
/// omitted suffix.
fn parse_create_retention_policy(trimmed: &str) -> Result<NodedbStatement, SqlError> {
    const PREFIX: &str = "CREATE RETENTION POLICY";
    let input = trimmed.trim();
    let after_prefix = input
        .get(..PREFIX.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(PREFIX))
        .ok_or_else(|| parse_error("expected CREATE RETENTION POLICY"))?;
    let mut rest = &input[after_prefix.len()..];
    if !rest.chars().next().is_some_and(char::is_whitespace) {
        return Err(parse_error("expected policy name"));
    }
    rest = rest.trim_start();

    let (name, after_name) = parse_identifier_token(rest)?;
    let after_on = consume_keyword(after_name.trim_start(), "ON")?;
    let (collection, after_collection) = parse_identifier_token(after_on)?;
    let after_collection = after_collection.trim_start();
    if !after_collection.starts_with('(') {
        return Err(parse_error("expected '(' after collection name"));
    }

    let (body_raw, after_body) = parse_balanced_body(after_collection)?;
    let eval_interval_raw = parse_optional_eval_interval(after_body)?;

    Ok(NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
        name,
        collection,
        body_raw,
        eval_interval_raw,
    }))
}

fn parse_error(detail: impl Into<String>) -> SqlError {
    SqlError::Parse {
        detail: detail.into(),
    }
}

fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Result<&'a str, SqlError> {
    let candidate = input
        .get(..keyword.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(keyword))
        .ok_or_else(|| parse_error(format!("expected {keyword}")))?;
    let rest = &input[candidate.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|ch| ch == '_' || ch == '$' || ch.is_alphanumeric())
    {
        return Err(parse_error(format!("expected {keyword}")));
    }
    Ok(rest.trim_start())
}

fn parse_identifier_token(input: &str) -> Result<(String, &str), SqlError> {
    if input.is_empty() {
        return Err(parse_error("missing identifier"));
    }
    if let Some(mut rest) = input.strip_prefix('"') {
        let mut value = String::new();
        loop {
            let Some(ch) = rest.chars().next() else {
                return Err(parse_error("unterminated quoted identifier"));
            };
            rest = &rest[ch.len_utf8()..];
            if ch == '"' {
                if let Some(next) = rest.strip_prefix('"') {
                    value.push('"');
                    rest = next;
                    continue;
                }
                if value.is_empty() || value.chars().any(char::is_control) {
                    return Err(parse_error("invalid quoted identifier"));
                }
                return Ok((value, rest));
            }
            value.push(ch);
        }
    }

    let end = input
        .char_indices()
        .find_map(|(index, ch)| (!is_bare_identifier_char(ch)).then_some(index))
        .unwrap_or(input.len());
    let name = &input[..end];
    if name.is_empty()
        || !name
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_alphabetic())
    {
        return Err(parse_error("invalid identifier"));
    }
    Ok((name.to_lowercase(), &input[end..]))
}

fn is_bare_identifier_char(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_alphanumeric()
}

/// Return the body content and the suffix after its matching delimiter.
/// Parentheses inside SQL single- and double-quoted text are literal.
fn parse_balanced_body(input: &str) -> Result<(String, &str), SqlError> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut index = 0usize;
    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .ok_or_else(|| parse_error("invalid body encoding"))?;
        let next_index = index + ch.len_utf8();
        if let Some(delimiter) = quote {
            if ch == delimiter {
                if input[next_index..].starts_with(delimiter) {
                    index = next_index + delimiter.len_utf8();
                    continue;
                }
                quote = None;
            }
            index = next_index;
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| parse_error("body nesting overflow"))?
            }
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| parse_error("unexpected ')'"))?;
                if depth == 0 {
                    return Ok((input[1..index].trim().to_string(), &input[next_index..]));
                }
            }
            _ => {}
        }
        index = next_index;
    }
    Err(parse_error(if quote.is_some() {
        "unterminated quoted text in retention policy body"
    } else {
        "missing closing ')' for retention policy body"
    }))
}

fn parse_optional_eval_interval(input: &str) -> Result<Option<String>, SqlError> {
    let input = input.trim();
    if input.is_empty() || input == ";" {
        return Ok(None);
    }
    let after_with = consume_keyword(input, "WITH")?;
    let after_open = after_with
        .strip_prefix('(')
        .ok_or_else(|| parse_error("expected '(' after WITH"))?
        .trim_start();
    let after_key = consume_keyword(after_open, "EVAL_INTERVAL")?;
    let after_equals = after_key
        .strip_prefix('=')
        .ok_or_else(|| parse_error("expected '=' after EVAL_INTERVAL"))?
        .trim_start();
    let (interval, after_interval) = parse_single_quoted_string(after_equals)?;
    let trailing = after_interval.trim_start();
    let trailing = trailing
        .strip_prefix(')')
        .ok_or_else(|| parse_error("expected ')' after EVAL_INTERVAL"))?
        .trim();
    if !trailing.is_empty() && trailing != ";" {
        return Err(parse_error("unexpected trailing tokens after WITH clause"));
    }
    Ok(Some(interval))
}

fn parse_single_quoted_string(input: &str) -> Result<(String, &str), SqlError> {
    let Some(mut rest) = input.strip_prefix('\'') else {
        return Err(parse_error("expected quoted EVAL_INTERVAL"));
    };
    let mut value = String::new();
    loop {
        let Some(ch) = rest.chars().next() else {
            return Err(parse_error("unterminated EVAL_INTERVAL"));
        };
        rest = &rest[ch.len_utf8()..];
        if ch == '\'' {
            if let Some(next) = rest.strip_prefix('\'') {
                value.push('\'');
                rest = next;
                continue;
            }
            return Ok((value, rest));
        }
        value.push(ch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_retention_name_preserves_original_offsets() {
        let sql = "CREATE RETENTION POLICY rpﬀﬀ ON metrics (RAW RETAIN '7d')";
        if let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            ..
        }) = parse_create_retention_policy(sql).expect("retention policy parses")
        {
            assert_eq!(name, "rpﬀﬀ");
            assert_eq!(collection, "metrics");
            assert_eq!(body_raw, "RAW RETAIN '7d'");
        } else {
            panic!("expected CreateRetentionPolicy");
        }
    }

    #[test]
    fn parse_basic_retention_policy() {
        let sql = "CREATE RETENTION POLICY rp1 ON metrics (RAW RETAIN '7d')";
        if let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            eval_interval_raw,
        }) = parse_create_retention_policy(sql).expect("retention policy parses")
        {
            assert_eq!(name, "rp1");
            assert_eq!(collection, "metrics");
            assert!(body_raw.contains("RAW RETAIN"));
            assert!(eval_interval_raw.is_none());
        } else {
            panic!("expected CreateRetentionPolicy");
        }
    }

    #[test]
    fn parse_with_eval_interval() {
        let sql =
            "CREATE RETENTION POLICY rp2 ON ts (RAW RETAIN '30d') WITH (EVAL_INTERVAL = '1h')";
        if let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            eval_interval_raw,
            ..
        }) = parse_create_retention_policy(sql).expect("retention policy parses")
        {
            assert_eq!(eval_interval_raw.as_deref(), Some("1h"));
        } else {
            panic!("expected CreateRetentionPolicy");
        }
    }

    #[test]
    fn create_preserves_quoted_identifiers_and_quoted_body_parentheses() {
        let sql = "CREATE RETENTION POLICY \"Policy Name\" ON \"Metric (Raw)\" (RAW RETAIN '7d (literal)')";
        let statement = parse_create_retention_policy(sql).expect("retention policy parses");
        let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            eval_interval_raw,
        }) = statement
        else {
            panic!("expected CreateRetentionPolicy");
        };
        assert_eq!(name, "Policy Name");
        assert_eq!(collection, "Metric (Raw)");
        assert_eq!(body_raw, "RAW RETAIN '7d (literal)'");
        assert_eq!(eval_interval_raw, None);
    }

    #[test]
    fn create_decodes_doubled_quotes_in_identifiers_and_eval_interval() {
        let sql = "CREATE RETENTION POLICY \"a\"\"b\" ON \"c\"\"d\" (RAW RETAIN '7d') WITH (EVAL_INTERVAL = 'it''s')";
        let statement = parse_create_retention_policy(sql).expect("retention policy parses");
        let NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            eval_interval_raw,
            ..
        }) = statement
        else {
            panic!("expected CreateRetentionPolicy");
        };
        assert_eq!(name, "a\"b");
        assert_eq!(collection, "c\"d");
        assert_eq!(eval_interval_raw.as_deref(), Some("it's"));
    }

    #[test]
    fn create_rejects_malformed_unknown_duplicate_and_trailing_with_clauses() {
        for sql in [
            "CREATE RETENTION POLICY rp ON metrics (RAW RETAIN '7d') WITH",
            "CREATE RETENTION POLICY rp ON metrics (RAW RETAIN '7d') WITH (UNKNOWN = '1h')",
            "CREATE RETENTION POLICY rp ON metrics (RAW RETAIN '7d') WITH (EVAL_INTERVAL = '1h', EVAL_INTERVAL = '2h')",
            "CREATE RETENTION POLICY rp ON metrics (RAW RETAIN '7d') WITH (EVAL_INTERVAL = '1h') extra",
        ] {
            assert!(
                parse_create_retention_policy(sql).is_err(),
                "must reject: {sql}"
            );
        }
    }

    #[test]
    fn drop_preserves_quoted_name_and_if_exists() {
        let statement = parse_drop_retention_policy(
            "DROP RETENTION POLICY IF EXISTS \"Policy \"\"Name\" ON \"Metric Collection\";",
        )
        .expect("drop policy parses");
        assert!(matches!(
            statement,
            NodedbStatement::Policy(PolicyStmt::DropRetentionPolicy { name, if_exists })
                if name == "Policy \"Name" && if_exists
        ));
    }

    #[test]
    fn alter_preserves_quoted_name_and_decodes_set_value() {
        let statement = parse_alter_retention_policy(
            "ALTER RETENTION POLICY \"Policy \"\"Name\" ON \"Metric Collection\" SET interval = 'it''s';",
        )
        .expect("alter policy parses");
        assert!(matches!(
            statement,
            NodedbStatement::Policy(PolicyStmt::AlterRetentionPolicy {
                name,
                action,
                set_key: Some(key),
                set_value: Some(value),
            }) if name == "Policy \"Name" && action == "SET" && key == "INTERVAL" && value == "it's"
        ));
    }

    #[test]
    fn try_parse_uses_trimmed_sql_for_quoted_drop_and_alter_names() {
        for sql in [
            "DROP RETENTION POLICY \"Policy Name\"",
            "ALTER RETENTION POLICY \"Policy Name\" ENABLE",
        ] {
            let upper = sql.to_uppercase();
            let parts: Vec<&str> = sql.split_whitespace().collect();
            assert!(
                try_parse(&upper, &parts, sql).is_some_and(|result| result.is_ok()),
                "must parse quoted name from original SQL: {sql}"
            );
        }
    }

    #[test]
    fn drop_and_alter_reject_trailing_or_malformed_tokens() {
        for sql in [
            "DROP RETENTION POLICY \"unterminated",
            "DROP RETENTION POLICY policy unexpected",
            "ALTER RETENTION POLICY policy ENABLE extra",
            "ALTER RETENTION POLICY policy SET interval = '1h' extra",
        ] {
            let result = if sql.starts_with("DROP") {
                parse_drop_retention_policy(sql)
            } else {
                parse_alter_retention_policy(sql)
            };
            assert!(result.is_err(), "must reject: {sql}");
        }
    }

    #[test]
    fn alter_auto_tier_requires_unquoted_boolean_literal() {
        for sql in [
            "ALTER RETENTION POLICY policy SET AUTO_TIER = 'TRUE'",
            "ALTER RETENTION POLICY policy SET AUTO_TIER = enabled",
            "ALTER RETENTION POLICY policy SET AUTO_TIER = 1",
        ] {
            let upper = sql.to_uppercase();
            let parts: Vec<&str> = sql.split_whitespace().collect();
            assert!(
                try_parse(&upper, &parts, sql).is_some_and(|result| result.is_err()),
                "must reject: {sql}"
            );
        }

        for sql in [
            "ALTER RETENTION POLICY policy SET AUTO_TIER = TRUE",
            "ALTER RETENTION POLICY policy SET AUTO_TIER = false",
        ] {
            let upper = sql.to_uppercase();
            let parts: Vec<&str> = sql.split_whitespace().collect();
            let statement = try_parse(&upper, &parts, sql)
                .expect("retention ALTER recognized")
                .expect("unquoted boolean accepted");
            assert!(matches!(
                statement,
                NodedbStatement::Policy(PolicyStmt::AlterRetentionPolicy {
                    set_key: Some(key),
                    set_value: Some(value),
                    ..
                }) if key == "AUTO_TIER"
                    && (value.eq_ignore_ascii_case("TRUE") || value.eq_ignore_ascii_case("FALSE"))
            ));
        }
    }
}
