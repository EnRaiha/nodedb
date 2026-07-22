// SPDX-License-Identifier: BUSL-1.1

//! Stateful ILP token scanning and zero-copy escape decoding.

use std::borrow::Cow;
use std::ops::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScanError {
    DanglingEscape(usize),
    InvalidQuote(usize),
}

/// Split an identifier or tag token at unescaped delimiters.
pub(super) fn split_escaped(
    source: &str,
    delimiter: char,
) -> Result<Vec<(&str, Range<usize>)>, ScanError> {
    scan_delimited(source, delimiter, false)
}

/// Split a field set at unescaped delimiters while preserving delimiters in
/// quoted string field values.
pub(super) fn split_field_delimited(
    source: &str,
    delimiter: char,
) -> Result<Vec<(&str, Range<usize>)>, ScanError> {
    scan_delimited(source, delimiter, true)
}

/// Split an ILP line into whitespace-separated tokens. Quotes have syntactic
/// meaning only when they begin a field value, never in measurement, tag, or
/// field-key identifiers where they are literal protocol characters.
pub(super) fn split_line_tokens(source: &str) -> Result<Vec<(&str, Range<usize>)>, ScanError> {
    scan_delimited(source, ' ', false)
}

fn scan_delimited(
    source: &str,
    delimiter: char,
    mut in_field_set: bool,
) -> Result<Vec<(&str, Range<usize>)>, ScanError> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut escaped = false;
    let mut field_value_starts = false;
    let mut quoted_string = false;
    let mut quote_open = false;

    for (offset, ch) in source.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if quoted_string && ch == '"' {
            quote_open = !quote_open;
            continue;
        }
        if !quote_open && field_value_starts && ch == '"' {
            quoted_string = true;
            quote_open = true;
            field_value_starts = false;
            continue;
        }
        if !quote_open && field_value_starts {
            field_value_starts = false;
        }
        if !quote_open && in_field_set && ch == '=' {
            field_value_starts = true;
            continue;
        }
        if !quote_open && ch == delimiter {
            parts.push((&source[start..offset], start..offset));
            start = offset + ch.len_utf8();
            if delimiter == ' ' {
                in_field_set = true;
            }
            if delimiter == ',' && in_field_set {
                field_value_starts = false;
                quoted_string = false;
            }
            continue;
        }
        // A comma starts another field while scanning the complete line for
        // whitespace tokens. It is already handled by the delimiter branch
        // when this scanner is splitting a field set directly.
        if !quote_open && in_field_set && delimiter != ',' && ch == ',' {
            field_value_starts = false;
            quoted_string = false;
        }
    }
    if escaped {
        return Err(ScanError::DanglingEscape(source.len().saturating_sub(1)));
    }
    if quote_open {
        return Err(ScanError::InvalidQuote(source.len()));
    }
    parts.push((&source[start..], start..source.len()));
    Ok(parts)
}

/// Find an unescaped delimiter in an identifier or tag token.
pub(super) fn find_unescaped_delimiter(
    source: &str,
    delimiter: char,
) -> Result<Option<usize>, ScanError> {
    Ok(split_escaped(source, delimiter)?
        .first()
        .and_then(|(_, span)| {
            if span.end < source.len() {
                Some(span.end)
            } else {
                None
            }
        }))
}

/// Decode an ILP identifier or tag value. Only the protocol-defined escapes
/// are accepted, and no allocation occurs when the token has no escape.
pub(super) fn decode_name<'a>(
    source: &'a str,
    permit_equals: bool,
) -> Result<Cow<'a, str>, ScanError> {
    decode_escaped(source, |ch| {
        ch == ',' || ch == ' ' || ch == '\\' || (permit_equals && ch == '=')
    })
}

/// Decode a quoted field string. The source must contain only its interior.
pub(super) fn decode_string<'a>(source: &'a str) -> Result<Cow<'a, str>, ScanError> {
    let Some(first_special) = source.find(['\\', '"']) else {
        return Ok(Cow::Borrowed(source));
    };
    let mut output = String::with_capacity(source.len());
    output.push_str(&source[..first_special]);
    let mut chars = source[first_special..].char_indices();
    while let Some((offset, ch)) = chars.next() {
        if ch == '"' {
            return Err(ScanError::InvalidQuote(first_special + offset));
        }
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let Some((_, escaped)) = chars.next() else {
            return Err(ScanError::DanglingEscape(first_special + offset));
        };
        if escaped != '"' && escaped != '\\' {
            return Err(ScanError::DanglingEscape(first_special + offset));
        }
        output.push(escaped);
    }
    Ok(Cow::Owned(output))
}

fn decode_escaped<'a>(
    source: &'a str,
    allowed: impl Fn(char) -> bool,
) -> Result<Cow<'a, str>, ScanError> {
    let Some(first_escape) = source.find('\\') else {
        return Ok(Cow::Borrowed(source));
    };
    let mut output = String::with_capacity(source.len());
    output.push_str(&source[..first_escape]);
    let mut chars = source[first_escape..].char_indices();
    while let Some((offset, ch)) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let Some((_, escaped)) = chars.next() else {
            return Err(ScanError::DanglingEscape(first_escape + offset));
        };
        if !allowed(escaped) {
            return Err(ScanError::DanglingEscape(first_escape + offset));
        }
        output.push(escaped);
    }
    Ok(Cow::Owned(output))
}
