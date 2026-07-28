// SPDX-License-Identifier: Apache-2.0

//! Parsing of a raw column `type_str` token — the bare type plus the inline
//! modifiers (`PRIMARY KEY`, `NOT NULL`, `DEFAULT <expr>`) the column-list
//! parser leaves attached to it.

use crate::parser::preprocess::lex::find_ascii_case_insensitive;

/// Parse a raw type_str token that may contain SQL modifiers and a DEFAULT clause.
///
/// Returns `(bare_type, is_primary_key, is_not_null, default_expr)`.
/// The `default_expr` is the raw expression text following the `DEFAULT` keyword,
/// trimmed of surrounding whitespace.
pub fn parse_column_type_str_full(type_str: &str) -> (String, bool, bool, Option<String>) {
    let is_pk = find_ascii_case_insensitive(type_str, "PRIMARY KEY").is_some();
    let is_not_null = find_ascii_case_insensitive(type_str, "NOT NULL").is_some();

    // Extract the DEFAULT clause from the type_str.
    // type_str may look like: "TEXT DEFAULT upper('x')" or "INT NOT NULL DEFAULT 1 + 2".
    let default_expr = if let Some(def_pos) = find_ascii_case_insensitive(type_str, "DEFAULT") {
        let after = type_str
            .get(def_pos + "DEFAULT".len()..)
            .unwrap_or("")
            .trim();
        let expression = &after[..default_expression_end(after)];
        if expression.trim().is_empty() {
            None
        } else {
            Some(expression.trim().to_string())
        }
    } else {
        None
    };

    // Strip modifiers to get bare type token.
    let bare = type_str
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(',');

    (bare.to_string(), is_pk, is_not_null, default_expr)
}

/// Return the byte offset where a trailing column constraint begins. Constraint
/// keywords inside quoted strings or parenthesized expressions belong to the
/// default expression and are intentionally ignored.
fn default_expression_end(input: &str) -> usize {
    const CONSTRAINTS: &[&str] = &[
        "NOT NULL",
        "PRIMARY KEY",
        "UNIQUE",
        "CHECK",
        "REFERENCES",
        "COLLATE",
        "GENERATED",
        "CONSTRAINT",
    ];

    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut quote = None;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(quoted_by) = quote {
            if byte == quoted_by {
                if index + 1 < bytes.len() && bytes[index + 1] == quoted_by {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'\'' | b'"' => quote = Some(byte),
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && (index == 0 || bytes[index - 1].is_ascii_whitespace()) => {
                let tail = &input[index..];
                if CONSTRAINTS.iter().any(|keyword| {
                    tail.get(..keyword.len())
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
                        && tail
                            .as_bytes()
                            .get(keyword.len())
                            .is_none_or(|next| next.is_ascii_whitespace() || *next == b'(')
                }) {
                    return index;
                }
            }
            _ => {}
        }
        index += 1;
    }
    input.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_after_unicode_type_text_preserves_original_offsets() {
        let (bare_type, is_pk, is_not_null, default_expr) =
            parse_column_type_str_full("CUSTOMßﬀİ DEFAULT 42");
        assert_eq!(bare_type, "CUSTOMßﬀİ");
        assert!(!is_pk);
        assert!(!is_not_null);
        assert_eq!(default_expr.as_deref(), Some("42"));
    }

    #[test]
    fn default_expression_excludes_trailing_constraints() {
        let (_, _, is_not_null, default_expr) = parse_column_type_str_full(
            "TEXT DEFAULT concat('NOT NULL', upper('(CHECK)')) NOT NULL UNIQUE",
        );
        assert!(is_not_null);
        assert_eq!(
            default_expr.as_deref(),
            Some("concat('NOT NULL', upper('(CHECK)'))")
        );
    }
}
