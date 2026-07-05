// SPDX-License-Identifier: Apache-2.0

//! Parse a trailing MySQL-style `ENGINE [=] <name>` clause that appears
//! after the column-list closing paren in `CREATE COLLECTION` / `CREATE TABLE`.
//!
//! Grammar accepted: `ENGINE = name`, `ENGINE=name`, `ENGINE name` (bare
//! identifier or a `'quoted'`/`"quoted"` name). Only matches the `ENGINE`
//! keyword as a standalone token at paren-depth 0 in the scanned tail, so a
//! `WITH (engine='kv')` clause occurring in the same tail is never
//! mismatched as the suffix form.

use crate::error::SqlError;

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Scan `tail` (the text after the column-list closing paren) for a
/// top-level `ENGINE [=] <name>` token and return the captured engine name
/// (as written; case-normalisation is the caller's job), or `None` if absent.
pub(super) fn extract_engine_suffix(tail: &str) -> Result<Option<String>, SqlError> {
    let bytes = tail.as_bytes();
    let mut depth = 0i32;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                depth -= 1;
                i += 1;
                continue;
            }
            _ => {}
        }

        if depth == 0 && matches_engine_token(bytes, i) {
            let before_ok = i == 0 || !is_ident_byte(bytes[i - 1]);
            if before_ok {
                let after_pos = i + "ENGINE".len();
                let rest = tail[after_pos..].trim_start();
                let rest = rest.strip_prefix('=').unwrap_or(rest).trim_start();
                let name = read_engine_name(rest)?;
                return Ok(Some(name));
            }
        }
        i += 1;
    }

    Ok(None)
}

/// Does `bytes[i..]` start with the case-insensitive token `ENGINE`,
/// followed by a non-identifier byte (or end of input)?
fn matches_engine_token(bytes: &[u8], i: usize) -> bool {
    const KW: &[u8] = b"ENGINE";
    if i + KW.len() > bytes.len() {
        return false;
    }
    if !bytes[i..i + KW.len()].eq_ignore_ascii_case(KW) {
        return false;
    }
    match bytes.get(i + KW.len()) {
        Some(&b) => !is_ident_byte(b),
        None => true,
    }
}

/// Read the engine-name token at the start of `rest`: either a quoted
/// string (`'name'` / `"name"`) or a bare identifier (alnum/underscore run).
/// Returns a typed parse error on an empty/malformed name.
fn read_engine_name(rest: &str) -> Result<String, SqlError> {
    let rest = rest.trim_start();
    if let Some(quote) = rest.chars().next().filter(|c| *c == '\'' || *c == '"') {
        let body = &rest[quote.len_utf8()..];
        let end = body.find(quote).ok_or_else(|| SqlError::Parse {
            detail: "unterminated quoted engine name in ENGINE clause".to_string(),
        })?;
        let name = body[..end].trim();
        if name.is_empty() {
            return Err(SqlError::Parse {
                detail: "empty engine name in ENGINE clause".to_string(),
            });
        }
        return Ok(name.to_string());
    }

    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    let name = &rest[..end];
    if name.is_empty() {
        return Err(SqlError::Parse {
            detail: "missing engine name after ENGINE clause".to_string(),
        });
    }
    Ok(name.to_string())
}
