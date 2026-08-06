// SPDX-License-Identifier: Apache-2.0

//! Token helpers shared by the strict policy-DDL parsers (`rls`, `redaction`).
//!
//! These families are hand-parsed rather than routed through sqlparser because
//! their grammars are NodeDB extensions. Both need the same primitives —
//! statement-prefix matching, keyword consumption, identifier parsing, and the
//! trailing `TENANT <id>` clause — so they live here instead of being copied
//! per family.

use crate::error::SqlError;

/// True when `upper` begins with `prefix` at a word boundary.
pub(super) fn starts_statement(upper: &str, prefix: &str) -> bool {
    upper
        .strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.chars().next().is_some_and(char::is_whitespace))
}

/// Strip `prefix` from `sql`, requiring at least one token after it.
pub(super) fn statement_suffix<'a>(sql: &'a str, prefix: &str) -> Result<&'a str, SqlError> {
    let sql = sql.trim();
    let matched = sql
        .get(..prefix.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .ok_or_else(|| parse_error(format!("expected {prefix}")))?;
    let rest = &sql[matched.len()..];
    if !rest.chars().next().is_some_and(char::is_whitespace) {
        return Err(parse_error(format!("expected tokens after {prefix}")));
    }
    Ok(rest.trim_start())
}

/// True when `input` begins with `keyword` at a word boundary.
pub(super) fn starts_keyword(input: &str, keyword: &str) -> bool {
    input
        .get(..keyword.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
        && !input[keyword.len()..]
            .chars()
            .next()
            .is_some_and(is_identifier_char)
}

/// Consume `keyword` from the front of `input`, erroring when absent.
pub(super) fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Result<&'a str, SqlError> {
    let input = input.trim_start();
    if !starts_keyword(input, keyword) {
        return Err(parse_error(format!("expected {keyword}")));
    }
    Ok(input[keyword.len()..].trim_start())
}

/// Parse a bare or double-quoted identifier. Bare identifiers are lowercased;
/// quoted ones keep their case and may contain `""` escapes.
pub(super) fn parse_identifier(input: &str) -> Result<(String, &str), SqlError> {
    let input = input.trim_start();
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
                } else {
                    if value.is_empty() || value.chars().any(char::is_control) {
                        return Err(parse_error("invalid quoted identifier"));
                    }
                    return Ok((value, rest));
                }
            } else {
                value.push(ch);
            }
        }
    }

    let end = input
        .char_indices()
        .find_map(|(index, ch)| (!is_identifier_char(ch)).then_some(index))
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

/// Parse a trailing `TENANT <id>` clause and require the statement to end.
pub(super) fn parse_optional_tenant(input: &str) -> Result<Option<u64>, SqlError> {
    let (tenant, rest) = parse_optional_tenant_prefix(input)?;
    ensure_end(rest)?;
    Ok(tenant)
}

/// Parse a leading `TENANT <id>` clause, returning the remaining input.
pub(super) fn parse_optional_tenant_prefix(input: &str) -> Result<(Option<u64>, &str), SqlError> {
    let rest = input.trim_start();
    if !starts_keyword(rest, "TENANT") {
        return Ok((None, rest));
    }
    let after_tenant = consume_keyword(rest, "TENANT")?;
    let end = after_tenant
        .char_indices()
        .find_map(|(index, ch)| (ch.is_whitespace() || ch == ';').then_some(index))
        .unwrap_or(after_tenant.len());
    let raw = &after_tenant[..end];
    if raw.is_empty() {
        return Err(parse_error("TENANT requires an unsigned integer ID"));
    }
    let tenant = raw
        .parse::<u64>()
        .map_err(|_| parse_error("TENANT requires an unsigned integer ID"))?;
    Ok((Some(tenant), &after_tenant[end..]))
}

/// Require that only an optional statement terminator remains.
pub(super) fn ensure_end(input: &str) -> Result<(), SqlError> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed == ";" {
        Ok(())
    } else {
        Err(parse_error(
            "unexpected trailing tokens in policy statement",
        ))
    }
}

/// Characters that may appear inside a bare identifier.
pub(super) fn is_identifier_char(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_alphanumeric()
}

/// Build a `SqlError::Parse` with `detail`.
pub(super) fn parse_error(detail: impl Into<String>) -> SqlError {
    SqlError::Parse {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_identifier_preserves_case_and_escapes() {
        let (name, rest) = parse_identifier("\"Sales \"\"Data\"\" \" FOR").expect("quoted");
        assert_eq!(name, "Sales \"Data\" ");
        assert_eq!(rest.trim_start(), "FOR");
    }

    #[test]
    fn bare_identifier_is_lowercased() {
        let (name, _) = parse_identifier("Users FOR").expect("bare");
        assert_eq!(name, "users");
    }

    #[test]
    fn tenant_clause_requires_unsigned_integer() {
        assert_eq!(
            parse_optional_tenant(" TENANT 42").expect("tenant"),
            Some(42)
        );
        assert_eq!(parse_optional_tenant("  ").expect("absent"), None);
        assert!(parse_optional_tenant(" TENANT nope").is_err());
        assert!(parse_optional_tenant(" TENANT 1 extra").is_err());
    }

    #[test]
    fn keyword_matching_respects_word_boundaries() {
        assert!(starts_keyword("TENANT 1", "TENANT"));
        assert!(!starts_keyword("TENANTS 1", "TENANT"));
        assert!(starts_statement(
            "SHOW REDACTION POLICIES",
            "SHOW REDACTION POLICIES"
        ));
        assert!(!starts_statement(
            "SHOW REDACTION POLICIESX",
            "SHOW REDACTION POLICIES"
        ));
    }
}
