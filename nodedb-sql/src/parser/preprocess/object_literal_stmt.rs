// SPDX-License-Identifier: Apache-2.0

//! Rewrite `INSERT/UPSERT INTO coll { ... }` (and `[{ ... }, ...]`) into
//! standard `INSERT INTO coll (cols) VALUES (row), ...`.

use super::literal::value_to_sql_literal;
use crate::error::SqlError;
use crate::parser::object_literal::{
    parse_object_literal_array_complete, parse_object_literal_complete,
};

/// Try to rewrite `INSERT INTO coll { ... }` or `INSERT INTO coll [{ ... }, { ... }]`
/// into standard `INSERT INTO coll (cols) VALUES (row1), (row2)`.
///
/// `Ok(None)` means the statement does not use object-literal syntax and the
/// caller should carry on with the original text. `Err` means it does and is
/// malformed — including the case where the author wrote a clause after the
/// literal.
///
/// The literal form carries no trailing clause, and that is a property of this
/// rewrite rather than an oversight: [`fields_to_values_sql`] reconstructs the
/// statement from the parsed fields alone, so anything the author wrote after
/// the literal has nowhere to go. Carrying it is not a matter of appending the
/// text either — the INSERT handler downstream rebuilds its own SQL from the
/// parsed fields a second time, dropping the clause again, and the hand-rolled
/// `(cols) VALUES (…)` scanner locates the value list with a reverse search for
/// `)`, which an `ON CONFLICT (id)` would capture. Making the form carry clauses
/// is a change to that whole pipeline; until then the honest answer is to refuse
/// the statement and point at the `(cols) VALUES (…)` form, which does carry
/// them.
///
/// `RETURNING` is the one clause this never sees: every caller splits it off the
/// text before rewriting and re-attaches it to the rebuilt statement, so it
/// survives the reconstruction and is not leftover input to reject.
pub(super) fn try_rewrite_object_literal(sql: &str) -> Result<Option<String>, SqlError> {
    let after_into = sql["INSERT INTO ".len()..].trim_start();
    let Some(coll_end) = after_into.find(|c: char| c.is_whitespace()) else {
        return Ok(None);
    };
    let coll_name = &after_into[..coll_end];
    let rest = after_into[coll_end..].trim_start();

    // Strip trailing semicolon before parsing.
    let obj_str = rest.trim_end_matches(';').trim_end();

    if obj_str.starts_with('[') {
        return rewrite_array_form(coll_name, obj_str);
    }

    if !obj_str.starts_with('{') {
        return Ok(None);
    }

    let Some(parsed) = parse_object_literal_complete(obj_str) else {
        return Ok(None);
    };
    let fields = parsed?;
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(fields_to_values_sql(coll_name, &[fields])))
}

/// Rewrite `[{ ... }, { ... }]` → multi-row VALUES.
fn rewrite_array_form(coll_name: &str, obj_str: &str) -> Result<Option<String>, SqlError> {
    let Some(parsed) = parse_object_literal_array_complete(obj_str) else {
        return Ok(None);
    };
    let objects = parsed?;
    if objects.is_empty() {
        return Ok(None);
    }
    Ok(Some(fields_to_values_sql(coll_name, &objects)))
}

/// Build `INSERT INTO coll (col_union) VALUES (row1), (row2), ...`
///
/// Collects the union of all keys across all rows. Missing keys get NULL.
fn fields_to_values_sql(
    coll_name: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> String {
    let mut all_keys: Vec<String> = rows
        .iter()
        .flat_map(|r| r.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    all_keys.sort();

    let col_list = all_keys.join(", ");

    let row_strs: Vec<String> = rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = all_keys
                .iter()
                .map(|k| match row.get(k) {
                    Some(v) => value_to_sql_literal(v),
                    None => "NULL".to_string(),
                })
                .collect();
            format!("({})", vals.join(", "))
        })
        .collect();

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        coll_name,
        col_list,
        row_strs.join(", ")
    )
}
