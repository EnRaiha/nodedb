// SPDX-License-Identifier: BUSL-1.1

//! Literal-aware scanning helpers for the MATCH parser.
//!
//! Every search runs over a top-level mask: a byte counts only when it is
//! outside string literals, quoted names and comments, and at bracket depth
//! zero. So a keyword, comma, or operator inside `'...'`, `"..."`, `(...)`,
//! `[...]` or `{...}` is never matched.

use nodedb_sql::parser::preprocess::lex::{self, SqlSegment};

/// A `BadRequest` error with `detail`.
pub(super) fn bad(detail: impl Into<String>) -> crate::Error {
    crate::Error::BadRequest {
        detail: detail.into(),
    }
}

/// One flag per byte of `text`: true when the byte is outside every literal,
/// quoted name and comment, and at bracket depth zero. An opening bracket
/// at depth zero and its matching closing bracket are themselves top-level.
pub(super) fn top_level_mask(text: &str) -> Vec<bool> {
    let mut mask = vec![false; text.len()];
    let mut depth = 0i32;
    let mut base = 0usize;
    for segment in lex::segments(text) {
        let raw = match &segment {
            SqlSegment::Text(t)
            | SqlSegment::SingleQuotedString(t)
            | SqlSegment::QuotedIdent(t)
            | SqlSegment::LineComment(t)
            | SqlSegment::BlockComment(t) => *t,
        };
        if let SqlSegment::Text(t) = &segment {
            for (i, b) in t.bytes().enumerate() {
                match b {
                    b'(' | b'[' | b'{' => {
                        mask[base + i] = depth == 0;
                        depth += 1;
                    }
                    b')' | b']' | b'}' => {
                        depth -= 1;
                        mask[base + i] = depth == 0;
                    }
                    _ => mask[base + i] = depth == 0,
                }
            }
        }
        base += raw.len();
    }
    mask
}

/// Bytes that continue a name. Non-ASCII bytes count, so a Unicode name that
/// contains a keyword's letters stays one name.
pub(super) fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// The end of the keyword phrase `words` when it starts at `start`. Words
/// are matched case-insensitively, separated by any whitespace, and the
/// phrase must not run into a following name.
fn match_phrase(bytes: &[u8], mask: &[bool], start: usize, words: &[&str]) -> Option<usize> {
    let mut pos = start;
    for (n, word) in words.iter().enumerate() {
        if n > 0 {
            let gap_start = pos;
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            if pos == gap_start {
                return None;
            }
        }
        let end = pos + word.len();
        if end > bytes.len() || !mask[pos] || !bytes[pos..end].eq_ignore_ascii_case(word.as_bytes())
        {
            return None;
        }
        pos = end;
    }
    let runs_on = bytes
        .get(pos)
        .is_some_and(|&b| is_word_byte(b) || b == b'.');
    (!runs_on).then_some(pos)
}

/// Every top-level occurrence of the keyword phrase `phrase` (words
/// separated by single spaces), as `(start, end)` byte ranges. A match must
/// not be part of a longer name or follow a `.`, so `a.limit` and
/// `is_where` never match.
pub(super) fn find_keyword_all(text: &str, mask: &[bool], phrase: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let words: Vec<&str> = phrase.split(' ').collect();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let starts_word = i == 0 || !(is_word_byte(bytes[i - 1]) || bytes[i - 1] == b'.');
        if mask[i]
            && starts_word
            && let Some(end) = match_phrase(bytes, mask, i, &words)
        {
            out.push((i, end));
            i = end;
            continue;
        }
        i += 1;
    }
    out
}

/// Split `text` at its top-level commas.
pub(super) fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mask = top_level_mask(text);
    let mut parts = Vec::new();
    let mut start = 0usize;
    for (i, b) in text.bytes().enumerate() {
        if b == b',' && mask[i] {
            parts.push(&text[start..i]);
            start = i + 1;
        }
    }
    parts.push(&text[start..]);
    parts
}

/// Split `text` at each top-level occurrence of the keyword `keyword`.
pub(super) fn split_top_level_keyword<'a>(text: &'a str, keyword: &str) -> Vec<&'a str> {
    let mask = top_level_mask(text);
    let mut parts = Vec::new();
    let mut start = 0usize;
    for (s, e) in find_keyword_all(text, &mask, keyword) {
        parts.push(&text[start..s]);
        start = e;
    }
    parts.push(&text[start..]);
    parts
}

/// The first top-level comparison operator in `text`, as `(position, op)`.
/// At one position the two-character operator wins over its prefix.
pub(super) fn find_top_level_operator(text: &str) -> Option<(usize, &'static str)> {
    const OPS: [&str; 7] = ["<=", ">=", "<>", "!=", "<", ">", "="];
    let mask = top_level_mask(text);
    let bytes = text.as_bytes();
    (0..bytes.len()).filter(|&i| mask[i]).find_map(|i| {
        OPS.iter()
            .find(|op| bytes[i..].starts_with(op.as_bytes()))
            .map(|op| (i, *op))
    })
}

/// The decoded text of `text` when it is exactly one single-quoted string
/// (`''` is an escaped quote). `None` for anything else, including an
/// unterminated string or trailing text.
pub(super) fn single_quoted(text: &str) -> Option<String> {
    let inner = text.strip_prefix('\'')?;
    let mut out = String::new();
    let mut chars = inner.char_indices();
    while let Some((i, ch)) = chars.next() {
        if ch != '\'' {
            out.push(ch);
            continue;
        }
        if inner[i + 1..].starts_with('\'') {
            out.push('\'');
            chars.next();
            continue;
        }
        return (i + 1 == inner.len()).then_some(out);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyword_inside_a_literal_or_group_is_not_found() {
        let text = "a = 'x RETURN y' AND f(RETURN) RETURN b";
        let mask = top_level_mask(text);
        let hits = find_keyword_all(text, &mask, "RETURN");
        assert_eq!(hits.len(), 1);
        assert_eq!(&text[hits[0].0..], "RETURN b");
    }

    #[test]
    fn field_named_like_a_keyword_is_not_found() {
        let text = "RETURN a.limit, is_where";
        let mask = top_level_mask(text);
        assert!(find_keyword_all(text, &mask, "LIMIT").is_empty());
        assert!(find_keyword_all(text, &mask, "WHERE").is_empty());
    }

    #[test]
    fn phrase_matches_across_any_whitespace() {
        let text = "RETURN a ORDER\n  BY a";
        let mask = top_level_mask(text);
        assert_eq!(find_keyword_all(text, &mask, "ORDER BY").len(), 1);
    }

    #[test]
    fn operator_inside_a_literal_is_skipped() {
        assert_eq!(find_top_level_operator("a.n = 'x<y'"), Some((4, "=")));
        assert_eq!(find_top_level_operator("a.n <= 3"), Some((4, "<=")));
    }

    #[test]
    fn single_quoted_requires_one_whole_string() {
        assert_eq!(single_quoted("'it''s'").as_deref(), Some("it's"));
        assert_eq!(single_quoted("'open"), None);
        assert_eq!(single_quoted("'a' b"), None);
        assert_eq!(single_quoted("'ab''"), None);
    }
}
