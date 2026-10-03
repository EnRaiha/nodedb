// SPDX-License-Identifier: Apache-2.0

//! Free-standing modifier keywords of `CREATE COLLECTION` / `CREATE TABLE`.
//!
//! A flag is a whole keyword token outside string literals, quoted
//! identifiers, and the column list. So a column named `is_append_only`, a
//! column named `hash_chain`, and a literal `'BITEMPORAL'` set nothing.
//! `FLAG = <value>` sets the flag only for a true value (`true`, `1`, `on`,
//! `yes`, quoted or bare).

/// Every recognised flag, in the order the parsed list reports them.
const FLAGS: [&str; 4] = ["APPEND_ONLY", "HASH_CHAIN", "BITEMPORAL", "SIGNED_DELTAS"];

/// The flags `body` sets. `column_list` is the byte span of the column-list
/// parentheses, both ends inclusive, when the body has one.
pub(super) fn extract_flags(body: &str, column_list: Option<(usize, usize)>) -> Vec<String> {
    let bytes = body.as_bytes();
    let mut set = [false; FLAGS.len()];
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some((start, end)) = column_list
            && i == start
        {
            i = end + 1;
            continue;
        }
        match bytes[i] {
            quote @ (b'\'' | b'"' | b'`') => i = skip_quoted(bytes, i, quote),
            b if is_ident_byte(b) => {
                let start = i;
                while i < bytes.len() && is_ident_byte(bytes[i]) {
                    i += 1;
                }
                let word = &body[start..i];
                if let Some(pos) = FLAGS.iter().position(|f| word.eq_ignore_ascii_case(f)) {
                    let (enabled, next) = flag_value(body, i);
                    set[pos] |= enabled;
                    i = next;
                }
            }
            _ => i += 1,
        }
    }
    FLAGS
        .iter()
        .zip(set)
        .filter(|(_, enabled)| *enabled)
        .map(|(flag, _)| (*flag).to_string())
        .collect()
}

/// Identifier bytes. Non-ASCII bytes count, so a Unicode identifier that
/// contains a flag's letters stays one token.
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// The index after the quoted run opening at `open`. A doubled quote is an
/// escaped quote. An unterminated run ends the body.
fn skip_quoted(bytes: &[u8], open: usize, quote: u8) -> usize {
    let mut i = open + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// Whether the flag keyword ending at `after` is set, and where scanning
/// resumes. A bare keyword is set. `= value` is set only for a true value.
fn flag_value(body: &str, after: usize) -> (bool, usize) {
    let rest = &body[after..];
    let Some(assigned) = rest.trim_start().strip_prefix('=') else {
        return (true, after);
    };
    let value = assigned.trim_start();
    let value_start = after + (rest.len() - value.len());
    let token = match value.as_bytes().first() {
        Some(&quote @ (b'\'' | b'"')) => {
            let end = skip_quoted(value.as_bytes(), 0, quote);
            let inner_end = end.saturating_sub(1).max(1);
            (value.get(1..inner_end).unwrap_or(""), end)
        }
        _ => {
            let end = value
                .bytes()
                .position(|b| !is_ident_byte(b))
                .unwrap_or(value.len());
            (&value[..end], end)
        }
    };
    let (word, consumed) = token;
    let enabled = ["true", "1", "on", "yes"]
        .iter()
        .any(|truthy| word.eq_ignore_ascii_case(truthy));
    (enabled, value_start + consumed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flags(body: &str) -> Vec<String> {
        extract_flags(body, None)
    }

    #[test]
    fn trailing_flags_after_a_with_clause_are_set() {
        assert_eq!(
            flags("(id STRING) WITH (engine='document_schemaless') APPEND_ONLY HASH_CHAIN"),
            vec!["APPEND_ONLY", "HASH_CHAIN"]
        );
    }

    #[test]
    fn a_flag_inside_an_identifier_sets_nothing() {
        assert!(flags("WITH (engine='kv') not_append_only_view").is_empty());
        assert!(flags("WITH (engine='kv') hash_chains").is_empty());
    }

    #[test]
    fn a_flag_inside_a_literal_or_quoted_identifier_sets_nothing() {
        assert!(flags("WITH (note='HASH_CHAIN') \"BITEMPORAL\"").is_empty());
        assert!(flags("WITH (note='it''s APPEND_ONLY')").is_empty());
    }

    #[test]
    fn a_column_named_like_a_flag_sets_nothing() {
        let body = "(append_only BOOL, hash_chain TEXT) WITH (engine='document_strict')";
        let close = body.find(')').expect("column list");
        assert!(extract_flags(body, Some((0, close))).is_empty());
    }

    #[test]
    fn an_assigned_flag_follows_its_value() {
        assert_eq!(flags("WITH (bitemporal=true)"), vec!["BITEMPORAL"]);
        assert_eq!(flags("WITH (bitemporal = 'TRUE')"), vec!["BITEMPORAL"]);
        assert!(flags("WITH (bitemporal=false)").is_empty());
        assert!(flags("WITH (append_only = 'no')").is_empty());
    }
}
