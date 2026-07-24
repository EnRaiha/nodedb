// SPDX-License-Identifier: BUSL-1.1

//! Quote-aware lexical helpers for retention policy parsing.

use super::DdlError;

fn err(message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: "42601".to_string(),
        message: message.into(),
    }
}

pub(super) fn split_top_level_commas(input: &str) -> Result<Vec<&str>, DdlError> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut index = 0usize;
    let mut quote: Option<char> = None;

    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .ok_or_else(|| err("invalid tier encoding"))?;
        let next = index + ch.len_utf8();
        if let Some(delimiter) = quote {
            if ch == delimiter {
                if input[next..].starts_with(delimiter) {
                    index = next + delimiter.len_utf8();
                    continue;
                }
                quote = None;
            }
            index = next;
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| err("tier nesting overflow"))?;
            }
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| err("unexpected ')' in tier definition"))?;
            }
            ',' if depth == 0 => {
                parts.push(&input[start..index]);
                start = next;
            }
            _ => {}
        }
        index = next;
    }

    if quote.is_some() {
        return Err(err("unterminated quoted text in tier definition"));
    }
    if depth != 0 {
        return Err(err("missing ')' in tier definition"));
    }
    if start < input.len() {
        parts.push(&input[start..]);
    }
    Ok(parts)
}

pub(super) fn find_matching_paren(input: &str, open: usize) -> Result<Option<usize>, DdlError> {
    if !input.get(open..).is_some_and(|tail| tail.starts_with('(')) {
        return Ok(None);
    }
    let mut depth = 0usize;
    let mut index = open;
    let mut quote: Option<char> = None;
    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .ok_or_else(|| err("invalid parenthesized expression"))?;
        let next = index + ch.len_utf8();
        if let Some(delimiter) = quote {
            if ch == delimiter {
                if input[next..].starts_with(delimiter) {
                    index = next + delimiter.len_utf8();
                    continue;
                }
                quote = None;
            }
            index = next;
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| err("parenthesis nesting overflow"))?;
            }
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| err("unexpected ')' in parenthesized expression"))?;
                if depth == 0 {
                    return Ok(Some(index));
                }
            }
            _ => {}
        }
        index = next;
    }
    if quote.is_some() {
        return Err(err("unterminated quoted text in parenthesized expression"));
    }
    Ok(None)
}

pub(super) fn extract_quoted_string(input: &str) -> Result<(String, &str), DdlError> {
    let Some(mut rest) = input.strip_prefix('\'') else {
        return Err(err("expected quoted string"));
    };
    let mut value = String::new();
    loop {
        let Some(ch) = rest.chars().next() else {
            return Err(err("unterminated quoted string"));
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

pub(super) fn parse_single_quoted_string(input: &str) -> Result<(String, &str), DdlError> {
    extract_quoted_string(input)
}

pub(super) fn consume_keyword<'a>(input: &'a str, keyword: &str) -> Result<&'a str, DdlError> {
    let candidate = input
        .get(..keyword.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(keyword))
        .ok_or_else(|| err(format!("expected {keyword}")))?;
    let rest = &input[candidate.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|ch| ch == '_' || ch == '$' || ch.is_alphanumeric())
    {
        return Err(err(format!("expected {keyword}")));
    }
    Ok(rest.trim_start())
}

pub(super) fn parse_identifier_token(input: &str) -> Result<(String, &str), DdlError> {
    if input.is_empty() {
        return Err(err("missing identifier"));
    }
    if let Some(mut rest) = input.strip_prefix('"') {
        let mut value = String::new();
        loop {
            let Some(ch) = rest.chars().next() else {
                return Err(err("unterminated quoted identifier"));
            };
            rest = &rest[ch.len_utf8()..];
            if ch == '"' {
                if let Some(next) = rest.strip_prefix('"') {
                    value.push('"');
                    rest = next;
                    continue;
                }
                if value.is_empty() || value.chars().any(char::is_control) {
                    return Err(err("invalid quoted identifier"));
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
        return Err(err("invalid identifier"));
    }
    Ok((name.to_lowercase(), &input[end..]))
}

fn is_bare_identifier_char(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_alphanumeric()
}
