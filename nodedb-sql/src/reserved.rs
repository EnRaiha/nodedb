// SPDX-License-Identifier: Apache-2.0

//! NodeDB reserved identifier list and canonical identifier validation.
//!
//! The validator is the single boundary for identifier content that survives
//! into planning. Bare identifiers are canonicalized to lowercase; quoted
//! identifiers preserve case and may opt into NodeDB-reserved words.

use crate::error::SqlError;

/// Words that NodeDB claims as dispatch or rewrite keywords.
pub const RESERVED_KEYWORDS: &[&str] = &[
    "GRAPH", "MATCH", "OPTIONAL", "UPSERT", "UNDROP", "PURGE", "CASCADE", "SEARCH", "CRDT",
];

fn reason_for(upper: &str) -> &'static str {
    match upper {
        "GRAPH" | "MATCH" | "OPTIONAL" => "graph dispatch keyword",
        "UPSERT" => "preprocess rewrite keyword",
        "UNDROP" => "DDL dispatch keyword",
        "PURGE" | "CASCADE" => "DROP modifier keyword",
        "SEARCH" => "DSL dispatch keyword",
        "CRDT" => "DSL dispatch keyword",
        _ => "reserved by NodeDB",
    }
}

/// Return `true` if `name` matches a NodeDB reserved keyword
/// (case-insensitive).
pub fn is_reserved(name: &str) -> bool {
    let upper = name.to_uppercase();
    RESERVED_KEYWORDS.contains(&upper.as_str())
}

fn invalid(name: &str, reason: &'static str) -> SqlError {
    SqlError::InvalidIdentifier {
        name: name.to_string(),
        reason,
    }
}

fn validate_quoted_content(value: &str) -> Result<(), SqlError> {
    if value.is_empty() {
        return Err(invalid(value, "quoted identifier must not be empty"));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(
            value,
            "identifier must not contain control characters",
        ));
    }
    if value.contains('"') {
        return Err(invalid(value, "identifier must not contain double quotes"));
    }
    Ok(())
}

fn validate_bare_content(value: &str) -> Result<(), SqlError> {
    if value.is_empty() {
        return Err(invalid(value, "identifier must not be empty"));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(
            value,
            "identifier must not contain control characters",
        ));
    }
    let mut chars = value.chars();
    let first = chars
        .next()
        .ok_or_else(|| invalid(value, "identifier must not be empty"))?;
    if first != '_' && !first.is_alphabetic() {
        return Err(invalid(
            value,
            "bare identifier must start with a letter or underscore",
        ));
    }
    if !chars.all(|ch| ch == '_' || ch == '$' || ch.is_alphanumeric()) {
        return Err(invalid(
            value,
            "bare identifier may contain only letters, digits, underscores, or dollar signs",
        ));
    }
    Ok(())
}

fn normalize_bare(value: &str) -> Result<String, SqlError> {
    validate_bare_content(value)?;
    let normalized = value.to_lowercase();
    let upper = normalized.to_uppercase();
    if RESERVED_KEYWORDS.contains(&upper.as_str()) {
        return Err(SqlError::ReservedIdentifier {
            name: value.to_string(),
            reason: reason_for(&upper),
        });
    }
    Ok(normalized)
}

/// Validate a raw identifier token extracted from SQL.
///
/// Quoted tokens must use SQL doubled-quote escaping. Their decoded content
/// preserves case and bypasses the reserved-word restriction, but all control
/// characters and embedded quotes are rejected. Bare tokens must use NodeDB's
/// canonical ASCII identifier grammar.
pub fn check_identifier(raw_name: &str) -> Result<String, SqlError> {
    if raw_name.starts_with('"') {
        return decode_raw_quoted_identifier(raw_name);
    }
    normalize_bare(raw_name)
}

fn decode_raw_quoted_identifier(raw_name: &str) -> Result<String, SqlError> {
    let mut rest = raw_name
        .strip_prefix('"')
        .ok_or_else(|| invalid(raw_name, "quoted identifier must start with a double quote"))?;
    let mut decoded = String::new();
    loop {
        let Some(ch) = rest.chars().next() else {
            return Err(invalid(raw_name, "unterminated quoted identifier"));
        };
        rest = &rest[ch.len_utf8()..];
        if ch == '"' {
            if let Some(after_escape) = rest.strip_prefix('"') {
                decoded.push('"');
                rest = after_escape;
                continue;
            }
            if !rest.is_empty() {
                return Err(invalid(
                    raw_name,
                    "unexpected content after quoted identifier",
                ));
            }
            validate_quoted_content(&decoded)?;
            return Ok(decoded);
        }
        decoded.push(ch);
    }
}

/// Validate an identifier already decoded by `sqlparser`.
///
/// `sqlparser::ast::Ident::value` has SQL quote escaping decoded, so no raw
/// token reconstruction is needed. Quoted identifiers preserve case; bare
/// identifiers are canonicalized and checked against reserved words.
pub fn check_ast_identifier(ident: &sqlparser::ast::Ident) -> Result<String, SqlError> {
    if ident.quote_style.is_some() {
        validate_quoted_content(&ident.value)?;
        Ok(ident.value.clone())
    } else {
        normalize_bare(&ident.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Ident;

    #[test]
    fn bare_identifiers_normalize_and_reserved_words_reject() {
        assert_eq!(
            check_identifier("MiXeD_1$Name").expect("bare"),
            "mixed_1$name"
        );
        assert!(matches!(
            check_identifier("match"),
            Err(SqlError::ReservedIdentifier { .. })
        ));
        assert_eq!(
            check_identifier("\"MATCH\"").expect("quoted reserved"),
            "MATCH"
        );
    }

    #[test]
    fn quoted_identifiers_preserve_case_but_reject_unsafe_content() {
        assert_eq!(
            check_identifier("\"MiXeD Unicode 雪\"").expect("quoted"),
            "MiXeD Unicode 雪"
        );
        for raw in [
            "\"\"",
            "\"a\"\"b\"",
            "\"a\n\"",
            "\"unterminated",
            "\"a\"tail",
        ] {
            assert!(
                matches!(
                    check_identifier(raw),
                    Err(SqlError::InvalidIdentifier { .. })
                ),
                "{raw}"
            );
        }
    }

    #[test]
    fn bare_identifiers_reject_noncanonical_or_control_content() {
        for raw in ["", "1name", "na-me", "name;drop", "name\n", "name\""] {
            assert!(
                matches!(
                    check_identifier(raw),
                    Err(SqlError::InvalidIdentifier { .. })
                ),
                "{raw}"
            );
        }
        assert_eq!(check_identifier("雪表").expect("Unicode bare"), "雪表");
    }

    #[test]
    fn ast_identifiers_apply_decoded_content_rules() {
        assert_eq!(
            check_ast_identifier(&Ident::new("MiXeD")).expect("bare AST"),
            "mixed"
        );
        let mut quoted = Ident::new("MATCH");
        quoted.quote_style = Some('"');
        assert_eq!(
            check_ast_identifier(&quoted).expect("quoted reserved AST"),
            "MATCH"
        );

        for value in ["", "a\"b", "a\n"] {
            let mut ident = Ident::new(value);
            ident.quote_style = Some('"');
            assert!(matches!(
                check_ast_identifier(&ident),
                Err(SqlError::InvalidIdentifier { .. })
            ));
        }
    }
}
