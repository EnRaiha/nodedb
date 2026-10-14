// SPDX-License-Identifier: Apache-2.0

//! Read a MySQL-style `ENGINE [=] <name>` clause of `CREATE COLLECTION` /
//! `CREATE TABLE`.
//!
//! Grammar accepted after the `ENGINE` keyword: `= name`, `name`, or either
//! with a `'quoted'` / `"quoted"` name. The body reader calls this only for
//! an `ENGINE` word at a clause position, so `WITH (engine='kv')` and a
//! literal containing `ENGINE` never reach it.

use crate::ddl_ast::parse::clause::{ClauseCursor, Tok};
use crate::error::SqlError;

/// Read the engine name after `ENGINE`, as written. Case normalisation is
/// the caller's job.
pub(super) fn read_engine_suffix(c: &mut ClauseCursor<'_, '_>) -> Result<String, SqlError> {
    c.eat_punct('=');
    let name = match c.remaining_tokens().first().map(|t| &t.tok) {
        Some(Tok::Str(_)) => c.string("ENGINE")?,
        Some(Tok::Ident(s)) => {
            let s = s.to_string();
            c.skip(1);
            s
        }
        Some(Tok::Word(_)) => c.word("ENGINE")?.to_string(),
        Some(Tok::Punct('\'' | '"')) => {
            return Err(c.error("unterminated quoted engine name in ENGINE clause"));
        }
        _ => return Err(c.error("missing engine name after ENGINE clause")),
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(c.error("empty engine name in ENGINE clause"));
    }
    Ok(name.to_string())
}
