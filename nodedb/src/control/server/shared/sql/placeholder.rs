// SPDX-License-Identifier: BUSL-1.1

//! SQL placeholder scanning and rewriting utilities.
//!
//! Provides context-aware `$N` placeholder detection that correctly skips
//! string literals, quoted identifiers, comments, and dollar-quoted blocks.

/// The highest `$N` index this scanner will treat as a real placeholder.
///
/// Matches the pgwire wire format's own ceiling: `Parse` and `Bind` both
/// carry the parameter count as an `Int16`, so no conforming client ever
/// names a placeholder above this. A hostile SQL string can still spell
/// `$99999999999999` — parsed as a `usize` with nothing to stop it — and
/// every consumer of [`placeholder_ranges`] (placeholder counting,
/// substitution, rewriting) would otherwise size a `Vec` off that index.
/// Rejecting it here, at the shared scan, degrades the position to the same
/// "not a placeholder we track" outcome as a malformed `$` body — every
/// caller inherits the guard without having to duplicate it.
const MAX_PLACEHOLDER_INDEX: usize = u16::MAX as usize;

/// Locate all `$N` placeholders in SQL, returning `(start, end, index)` triples.
///
/// Skips placeholders inside single-quoted strings, double-quoted identifiers,
/// line comments (`--`), block comments (`/* */`), and dollar-quoted strings.
/// An index beyond [`MAX_PLACEHOLDER_INDEX`] (or one that overflows `usize`)
/// is left out of the result, same as a malformed `$` body.
pub(crate) fn placeholder_ranges(sql: &str) -> Vec<(usize, usize, usize)> {
    let bytes = sql.as_bytes();
    let mut ranges = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_single_quoted(bytes, i),
            b'"' => i = skip_double_quoted(bytes, i),
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                i = skip_line_comment(bytes, i);
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i = skip_block_comment(bytes, i);
            }
            b'$' => {
                if let Some(next) = skip_dollar_quoted(bytes, i) {
                    i = next;
                    continue;
                }
                let start_digits = i + 1;
                let mut j = start_digits;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start_digits
                    && let Ok(idx) = sql[start_digits..j].parse::<usize>()
                    && idx <= MAX_PLACEHOLDER_INDEX
                {
                    ranges.push((i, j, idx));
                    i = j;
                    continue;
                }
                i += 1;
            }
            _ => i += 1,
        }
    }

    ranges
}

/// Rewrite `$N` placeholders with the corresponding replacement strings.
///
/// Placeholders are 1-indexed: `$1` maps to `replacements[0]`, etc.
/// Placeholders without a matching replacement are left as-is.
pub(crate) fn rewrite_sql_placeholders(sql: &str, replacements: &[String]) -> String {
    let ranges = placeholder_ranges(sql);
    if ranges.is_empty() {
        return sql.to_owned();
    }

    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;
    for (start, end, idx) in ranges {
        out.push_str(&sql[cursor..start]);
        if let Some(replacement) = idx.checked_sub(1).and_then(|i| replacements.get(i)) {
            out.push_str(replacement);
        } else {
            out.push_str(&sql[start..end]);
        }
        cursor = end;
    }
    out.push_str(&sql[cursor..]);
    out
}

fn skip_single_quoted(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn skip_double_quoted(bytes: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], mut i: usize) -> usize {
    i += 2;
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

fn skip_block_comment(bytes: &[u8], mut i: usize) -> usize {
    i += 2;
    while i + 1 < bytes.len() {
        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            return i + 2;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_dollar_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if j >= bytes.len() || bytes[j] != b'$' {
        return None;
    }
    let tag = &bytes[i..=j];
    let mut k = j + 1;
    while k + tag.len() <= bytes.len() {
        if &bytes[k..k + tag.len()] == tag {
            return Some(k + tag.len());
        }
        k += 1;
    }
    Some(bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_placeholders_are_unaffected() {
        assert_eq!(
            placeholder_ranges("SELECT $1, $2"),
            vec![(7, 9, 1), (11, 13, 2)]
        );
    }

    #[test]
    fn malformed_bodies_are_not_placeholders() {
        assert_eq!(placeholder_ranges("SELECT $"), Vec::new());
        assert_eq!(placeholder_ranges("SELECT $abc"), Vec::new());
    }

    #[test]
    fn zero_index_is_a_recognised_range_here() {
        // This scanner only filters on the upper ceiling; index-0 is a
        // syntactically valid digit run and is left for callers such as
        // `parse_placeholder_body` to reject as not a 1-based position.
        assert_eq!(placeholder_ranges("SELECT $0"), vec![(7, 9, 0)]);
    }

    #[test]
    fn boundary_index_is_still_a_placeholder() {
        let sql = "SELECT $65535";
        assert_eq!(placeholder_ranges(sql), vec![(7, 13, 65535)]);
    }

    #[test]
    fn just_above_boundary_is_not_a_placeholder() {
        let sql = "SELECT $65536";
        assert_eq!(placeholder_ranges(sql), Vec::new());
    }

    #[test]
    fn absurdly_large_index_does_not_allocate_or_panic() {
        let sql = "SELECT $99999999999999";
        // Must not panic, and must not be tracked as a real placeholder —
        // any consumer sizing a Vec off the reported index would otherwise
        // attempt a multi-terabyte allocation.
        assert_eq!(placeholder_ranges(sql), Vec::new());
    }
}
