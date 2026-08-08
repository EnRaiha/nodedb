// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, keyword_position_outside_literals,
};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::server::shared::ddl::sql_parse::{parse_sql_value, split_values};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::types::{ParsedInsert, ddl_err};

const RETURNING_KEYWORD: &str = "RETURNING";

/// Parse an INSERT/UPSERT SQL statement into structured fields.
///
/// `keyword` is the SQL prefix to match (e.g., "INSERT INTO " or "UPSERT INTO ").
/// Returns `None` if the collection has a typed schema (let the SQL path handle it).
///
/// A trailing `RETURNING` list is split off the text up front and carried on
/// [`ParsedInsert::returning_clause`] for the caller to re-attach. Every form
/// below REBUILDS the statement from the parsed fields, so a clause left in the
/// text is discarded by that rebuild — and the object-literal rewriter refuses
/// input it cannot account for, so leaving it there would refuse a clause the
/// pipeline can in fact carry.
pub(in crate::control::server::shared::ddl::neutral::collection) fn parse_write_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    keyword: &str,
) -> Option<Result<ParsedInsert, DdlError>> {
    let (sql, returning_clause) = split_returning(sql);
    let kw_pos = find_ascii_case_insensitive(sql, keyword)?;
    let after_into = sql[kw_pos + keyword.len()..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name = coll_name_str.to_lowercase();

    // Check if collection is schemaless. Let the SQL path handle typed INSERT
    // with VALUES syntax, but always handle here for pre-write concerns:
    // - UPSERT (triggers + nodedb-sql handles the routing)
    // - { } object literal syntax (triggers + nodedb-sql handles the routing)
    let tenant_id = identity.tenant_id;
    let is_upsert = keyword.starts_with("UPSERT");
    let after_coll_trimmed = after_into[coll_name_str.len()..].trim_start();
    let is_object_literal =
        after_coll_trimmed.starts_with('{') || after_coll_trimmed.starts_with('[');
    let mut coll_type: Option<nodedb_types::CollectionType> = None;
    let catalog = state.credentials.catalog();
    if let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), &coll_name) {
        // Skip non-schemaless collections for standard VALUES INSERT (let SQL path handle).
        // But always handle here for: UPSERT, { } object literal (any collection type).
        if !is_upsert && !is_object_literal && !coll.collection_type.is_schemaless() {
            return None;
        }
        coll_type = Some(coll.collection_type.clone());
    }

    // Determine which form this statement uses: { } object literal or (cols) VALUES (vals).
    // If { }, rewrite to VALUES SQL via nodedb-sql's preprocess, then parse that.
    let after_coll_name = after_into[coll_name_str.len()..].trim_start();
    if after_coll_name.starts_with('{') || after_coll_name.starts_with('[') {
        // The rewriter's own diagnostic is carried through verbatim. It is the
        // only place that knows WHY the literal was rejected — a malformed
        // field, or a trailing clause the brace form cannot carry — and
        // replacing it with a generic message would tell the author that
        // something failed without telling them what to change.
        match nodedb_sql::parser::preprocess::preprocess(sql) {
            // The preprocessed SQL is always INSERT INTO regardless of original keyword.
            Ok(Some(preprocessed)) => {
                return with_returning(
                    parse_values_form(&preprocessed.sql, "INSERT INTO ", &coll_name, coll_type),
                    returning_clause,
                );
            }
            Ok(None) => {
                return Some(Err(ddl_err(
                    "42601",
                    "failed to parse object literal in INSERT/UPSERT statement",
                )));
            }
            Err(error) => return Some(Err(ddl_err("42601", error.to_string()))),
        }
    }

    with_returning(
        parse_values_form(sql, keyword, &coll_name, coll_type),
        returning_clause,
    )
}

/// Split a trailing `RETURNING <columns>` off the statement text.
///
/// Returns the statement without the clause plus the projected column list.
/// The keyword is located outside string literals so a value containing the
/// word is data, not a clause.
fn split_returning(sql: &str) -> (&str, Option<String>) {
    match keyword_position_outside_literals(sql, RETURNING_KEYWORD) {
        Some(pos) => (
            &sql[..pos],
            Some(sql[pos + RETURNING_KEYWORD.len()..].trim().to_string()),
        ),
        None => (sql, None),
    }
}

/// Stamp the split-off projection onto a successfully parsed statement.
fn with_returning(
    parsed: Option<Result<ParsedInsert, DdlError>>,
    returning_clause: Option<String>,
) -> Option<Result<ParsedInsert, DdlError>> {
    parsed.map(|result| {
        result.map(|mut parsed| {
            parsed.returning_clause = returning_clause;
            parsed
        })
    })
}

/// Parse the `(cols) VALUES (vals)` form.
///
/// Any trailing `RETURNING` is already gone — [`parse_write_statement`] splits
/// it off before either form is examined. That matters here: the value list is
/// located by a REVERSE search for `)`, which a `RETURNING upper(x)` would
/// otherwise capture, swallowing the real values.
fn parse_values_form(
    sql: &str,
    keyword: &str,
    coll_name: &str,
    coll_type: Option<nodedb_types::CollectionType>,
) -> Option<Result<ParsedInsert, DdlError>> {
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(ddl_err(
                "42601",
                format!("missing column list in {}", keyword.trim()),
            )));
        }
    };
    let values_kw = match find_ascii_case_insensitive(sql, "VALUES") {
        Some(p) => p,
        None => return Some(Err(ddl_err("42601", "missing VALUES clause"))),
    };
    let first_close = match sql[first_open..values_kw].rfind(')') {
        Some(p) => first_open + p,
        None => {
            return Some(Err(ddl_err("42601", "missing closing ) for column list")));
        }
    };
    let cols_str = &sql[first_open + 1..first_close];
    let columns: Vec<&str> = cols_str.split(',').map(|c| c.trim()).collect();

    let after_values = sql[values_kw + 6..].trim_start();
    let vals_open = match after_values.find('(') {
        Some(p) => p,
        None => return Some(Err(ddl_err("42601", "missing VALUES (...)"))),
    };
    let vals_close = match after_values.rfind(')') {
        Some(p) => p,
        None => return Some(Err(ddl_err("42601", "missing closing ) for VALUES"))),
    };
    let vals_str = &after_values[vals_open + 1..vals_close];
    let values: Vec<&str> = split_values(vals_str);

    if columns.len() != values.len() {
        return Some(Err(ddl_err(
            "42601",
            format!(
                "column count ({}) doesn't match value count ({})",
                columns.len(),
                values.len()
            ),
        )));
    }

    let mut doc_id = String::new();
    let mut fields = HashMap::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id")
            || col.eq_ignore_ascii_case("document_id")
            || col.eq_ignore_ascii_case("key")
        {
            doc_id = val.trim_matches('\'').to_string();
        }
        fields.insert(col.to_string(), parse_sql_value(val));
    }

    if doc_id.is_empty() {
        doc_id = nodedb_types::id_gen::uuid_v7();
    }

    Some(Ok(ParsedInsert {
        coll_name: coll_name.to_string(),
        doc_id,
        fields,
        // Stamped by `parse_write_statement`, which owns the split.
        returning_clause: None,
        collection_type: coll_type,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_keyword_after_unicode_identifier_preserves_original_offsets() {
        let sql = "INSERT INTO tﬀﬀ (a) VALUES (42)";
        let parsed = parse_values_form(sql, "INSERT INTO ", "tﬀﬀ", None)
            .expect("statement should be recognized")
            .expect("statement should parse");
        assert_eq!(
            parsed.fields.get("a"),
            Some(&nodedb_types::Value::Integer(42))
        );
    }
}
