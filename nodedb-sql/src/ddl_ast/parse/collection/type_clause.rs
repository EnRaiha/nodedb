// SPDX-License-Identifier: Apache-2.0

//! Read the `TYPE <kind>` clause of `CREATE COLLECTION` / `CREATE TABLE`, and
//! the `STRICT (...)` column-list form.
//!
//! Each spelling names an engine, and the clause selects that engine as
//! `WITH (engine='...')` does:
//!
//! | Spelling                                          | Engine                |
//! | ------------------------------------------------- | --------------------- |
//! | `TYPE DOCUMENT`, `TYPE DOCUMENT SCHEMALESS`, `TYPE SCHEMALESS` | `document_schemaless` |
//! | `TYPE STRICT`, `TYPE DOCUMENT STRICT`, `STRICT (...)` | `document_strict`  |
//! | `TYPE KV`, `TYPE KEY_VALUE`                        | `kv`                  |
//! | `TYPE COLUMNAR`                                    | `columnar`            |
//! | `TYPE TIMESERIES`                                  | `timeseries`          |
//! | `TYPE SPATIAL`                                     | `spatial`             |
//! | `TYPE VECTOR`                                      | `vector`              |
//!
//! Any other kind is refused with SQLSTATE 42601 and the supported spelling.
//! A `TYPE` word is never skipped: skipping it created a schemaless
//! collection for `TYPE STRICT (...)`.

use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::error::SqlError;

/// The engines a `TYPE` clause may name, as `WITH (engine='...')` spells them.
const ENGINES: &str =
    "document_schemaless, document_strict, kv, columnar, timeseries, spatial, vector";

/// Read the kind after a consumed `TYPE` keyword and return the canonical
/// engine name it selects.
pub(super) fn read_type_kind(c: &mut ClauseCursor<'_, '_>) -> Result<&'static str, SqlError> {
    let Some(kind) = c.peek_word_upper() else {
        return Err(c.error(format!(
            "TYPE needs a collection kind; use TYPE <kind> or WITH (engine='<name>') with one \
             of: {ENGINES}"
        )));
    };
    c.skip(1);
    let engine = match kind.as_str() {
        "DOCUMENT" => {
            if c.eat_keyword("STRICT") {
                "document_strict"
            } else {
                c.eat_keyword("SCHEMALESS");
                "document_schemaless"
            }
        }
        "SCHEMALESS" => "document_schemaless",
        "STRICT" => "document_strict",
        "KV" | "KEY_VALUE" => "kv",
        "COLUMNAR" => "columnar",
        "TIMESERIES" => "timeseries",
        "SPATIAL" => "spatial",
        "VECTOR" => "vector",
        other => {
            return Err(c.error(format!(
                "unknown collection TYPE '{}'; use WITH (engine='<name>') with one of: \
                 {ENGINES}",
                other.to_lowercase()
            )));
        }
    };
    Ok(engine)
}

/// The engine a leading kind word selects, read from the cursor right after
/// the collection name: `TYPE <kind>`, or `STRICT` before the column list.
/// `None`, with the cursor unmoved, when neither is there.
pub(super) fn read_leading_kind(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<Option<&'static str>, SqlError> {
    if c.eat_keyword("TYPE") {
        c.clause("TYPE")?;
        return read_type_kind(c).map(Some);
    }
    if c.peek_keyword("STRICT") {
        c.skip(1);
        c.clause("TYPE")?;
        return Ok(Some("document_strict"));
    }
    Ok(None)
}
