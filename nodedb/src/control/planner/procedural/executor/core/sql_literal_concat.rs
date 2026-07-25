// SPDX-License-Identifier: BUSL-1.1

//! Lexical folding for adjacent SQL string-literal concatenations.

use super::super::sql_bytes::skip_ascii_whitespace;

pub(super) fn fold_literal_string_concat(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if let Some(end) = opaque_sql_end(sql, i) {
            result.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        if bytes[i] != b'\'' {
            let Some(ch) = sql[i..].chars().next() else {
                break;
            };
            result.push(ch);
            i += ch.len_utf8();
            continue;
        }

        let start = i;
        let Some((mut cursor, mut combined)) = parse_single_quoted_literal(sql, i) else {
            // An unterminated literal is left unchanged for the SQL parser to
            // reject; copy one complete UTF-8 character rather than one byte.
            let Some(ch) = sql[i..].chars().next() else {
                break;
            };
            result.push(ch);
            i += ch.len_utf8();
            continue;
        };

        let mut folded = false;
        while let Some(op_end) = consume_string_concat_operator(bytes, cursor) {
            let next_lit = skip_ascii_whitespace(bytes, op_end);
            let Some((next_cursor, next_literal)) = parse_single_quoted_literal(sql, next_lit)
            else {
                break;
            };

            combined.push_str(&next_literal);
            cursor = next_cursor;
            folded = true;
        }

        if folded {
            result.push('\'');
            for ch in combined.chars() {
                if ch == '\'' {
                    result.push_str("''");
                } else {
                    result.push(ch);
                }
            }
            result.push('\'');
        } else {
            result.push_str(&sql[start..cursor]);
        }
        i = cursor;
    }

    result
}

fn opaque_sql_end(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    match bytes[start] {
        b'"' => Some(consume_quoted_region(sql, start, b'"')),
        b'-' if bytes.get(start + 1) == Some(&b'-') => Some(
            sql[start..]
                .find('\n')
                .map_or(sql.len(), |offset| start + offset),
        ),
        b'/' if bytes.get(start + 1) == Some(&b'*') => {
            Some(consume_block_comment_region(sql, start))
        }
        b'$' => consume_dollar_quote_region(sql, start),
        _ => None,
    }
}

fn consume_quoted_region(sql: &str, start: usize, quote: u8) -> usize {
    let bytes = sql.as_bytes();
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if bytes[cursor] == quote {
            cursor += 1;
            if bytes.get(cursor) != Some(&quote) {
                return cursor;
            }
            cursor += 1;
        } else {
            cursor += sql[cursor..].chars().next().map_or(1, char::len_utf8);
        }
    }
    sql.len()
}

fn consume_block_comment_region(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut cursor = start + 2;
    let mut depth = 1usize;
    while cursor + 1 < bytes.len() {
        match (bytes[cursor], bytes[cursor + 1]) {
            (b'/', b'*') => {
                depth += 1;
                cursor += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                cursor += 2;
                if depth == 0 {
                    return cursor;
                }
            }
            _ => cursor += sql[cursor..].chars().next().map_or(1, char::len_utf8),
        }
    }
    sql.len()
}

fn consume_dollar_quote_region(sql: &str, start: usize) -> Option<usize> {
    let rest = &sql[start..];
    let close = rest[1..].find('$')? + 1;
    let tag = &rest[..=close];
    let tag_body = &tag[1..tag.len() - 1];
    if !tag_body.is_empty()
        && (!tag_body
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_alphabetic())
            || !tag_body.chars().all(|ch| ch == '_' || ch.is_alphanumeric()))
    {
        return None;
    }
    let body_start = start + tag.len();
    Some(
        sql[body_start..]
            .find(tag)
            .map_or(sql.len(), |offset| body_start + offset + tag.len()),
    )
}

fn parse_single_quoted_literal(sql: &str, start: usize) -> Option<(usize, String)> {
    let bytes = sql.as_bytes();
    if bytes.get(start) != Some(&b'\'') {
        return None;
    }

    let mut i = start + 1;
    let mut literal = String::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if bytes.get(i + 1) == Some(&b'\'') {
                    literal.push('\'');
                    i += 2;
                } else {
                    return Some((i + 1, literal));
                }
            }
            _ => {
                let ch = sql[i..].chars().next()?;
                literal.push(ch);
                i += ch.len_utf8();
            }
        }
    }

    None
}

fn consume_string_concat_operator(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = skip_ascii_whitespace(bytes, start);
    if bytes.get(i) != Some(&b'|') {
        return None;
    }
    i += 1;
    i = skip_ascii_whitespace(bytes, i);
    if bytes.get(i) != Some(&b'|') {
        return None;
    }
    Some(i + 1)
}

#[cfg(test)]
mod tests {
    use super::{fold_literal_string_concat, parse_single_quoted_literal};

    #[test]
    fn folds_adjacent_string_concat_literals() {
        let sql = "INSERT INTO log VALUES ('as1' || '_log', 'ok')";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, "INSERT INTO log VALUES ('as1_log', 'ok')");
    }

    #[test]
    fn folds_multiple_concat_segments() {
        let sql = "VALUES ('a' || 'b' || 'c')";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, "VALUES ('abc')");
    }

    #[test]
    fn folds_spaced_concat_operator_segments() {
        let sql = "VALUES ('a' | | 'b')";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, "VALUES ('ab')");
    }

    #[test]
    fn leaves_non_concat_literals_unchanged() {
        let sql = "VALUES ('a', col)";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, sql);
    }

    #[test]
    fn leaves_opaque_regions_unfolded() {
        let sql = "\"'a' || 'b'\" -- 'c' || 'd'\n/* 'e' || 'f' */ $$'g' || 'h'$$, 'i' || 'j'";
        assert_eq!(
            fold_literal_string_concat(sql),
            "\"'a' || 'b'\" -- 'c' || 'd'\n/* 'e' || 'f' */ $$'g' || 'h'$$, 'ij'"
        );
    }

    #[test]
    fn folds_unicode_literals_and_preserves_doubled_quotes() {
        let sql = "VALUES ('雪''猫' || '犬')";
        assert_eq!(fold_literal_string_concat(sql), "VALUES ('雪''猫犬')");
        assert_eq!(
            parse_single_quoted_literal("'雪''猫'", 0),
            Some(("'雪''猫'".len(), "雪'猫".to_string()))
        );
    }
}
