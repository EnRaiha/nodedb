// SPDX-License-Identifier: Apache-2.0

//! SQL-standard quoting for identifiers and string literals.
//!
//! These helpers preserve input bytes other than the delimiter being escaped.
//! They are intended for SQL text reconstruction only; parameter binding remains
//! preferable when the protocol supports it.

/// Quote a SQL identifier by doubling embedded double quotes and wrapping the
/// result in double quotes.
#[must_use]
pub fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Quote a SQL string literal by doubling embedded single quotes and wrapping
/// the result in single quotes.
#[must_use]
pub fn quote_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_quote_empty_and_embedded_delimiters() {
        assert_eq!(quote_ident(""), "\"\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn literals_quote_empty_and_embedded_delimiters() {
        assert_eq!(quote_literal(""), "''");
        assert_eq!(quote_literal("O'Reilly"), "'O''Reilly'");
    }

    #[test]
    fn quoting_preserves_control_shaped_unicode_and_sql_punctuation() {
        let value = "snow 雪;\n\t\u{0001}'\"--";
        assert_eq!(quote_ident(value), "\"snow 雪;\n\t\u{0001}'\"\"--\"");
        assert_eq!(quote_literal(value), "'snow 雪;\n\t\u{0001}''\"--'");
    }
}
