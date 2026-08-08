// SPDX-License-Identifier: BUSL-1.1

//! RETURNING clause pre-processing for DML statements.
//!
//! DataFusion does not support RETURNING on DML (INSERT/UPDATE/DELETE).
//! This module detects and strips the RETURNING clause from raw SQL before
//! DataFusion planning, parsing the projected column list so the response
//! handler can format the Data Plane's returned documents as a pgwire
//! QueryResponse with one column per projected field.

// Re-export bridge types so callers only import from this module.
pub(super) use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem, ReturningSpec};

use crate::Error;
use nodedb_sql::parser::preprocess::lex::{find_ascii_keyword, keyword_position_outside_literals};
use nodedb_types::starts_with_ascii_case_insensitive;

const RETURNING_KEYWORD: &str = "RETURNING";

/// Check if a DML statement contains a RETURNING clause and strip it.
///
/// Returns `(cleaned_sql, returning_spec)`. The cleaned SQL has the
/// `RETURNING ...` suffix removed so DataFusion can parse it.
///
/// RETURNING is honored on UPDATE, DELETE and MERGE. On INSERT it is
/// **refused** — see [`refuse_unsupported_insert_returning`].
///
/// Arithmetic expressions (e.g. `RETURNING stock * 2`) are rejected with
/// a typed error — only bare column names and `*` are supported.
pub(super) fn strip_returning(sql: &str) -> Result<(String, Option<ReturningSpec>), Error> {
    let trimmed = sql.trim_start();

    refuse_unsupported_insert_returning(trimmed, sql)?;

    if !starts_with_ascii_case_insensitive(trimmed, "UPDATE")
        && !starts_with_ascii_case_insensitive(trimmed, "DELETE")
        && !starts_with_ascii_case_insensitive(trimmed, "MERGE")
    {
        return Ok((sql.to_string(), None));
    }

    if let Some(pos) = keyword_position_outside_literals(sql, RETURNING_KEYWORD) {
        let cleaned = sql[..pos].trim_end().to_string();
        let columns_str = sql[pos + RETURNING_KEYWORD.len()..].trim();
        let spec = parse_returning_columns(columns_str)?;
        Ok((cleaned, Some(spec)))
    } else {
        Ok((sql.to_string(), None))
    }
}

/// Refuse `INSERT ... RETURNING`, which nothing in the system can honor.
///
/// No insert operation carries a `returning` slot on any engine —
/// `DocumentOp::PointInsert` / `PointPut` / `BatchInsert`, and the key-value,
/// columnar, and vector inserts alike — while `PointUpdate`, `PointDelete`,
/// `Merge`, and `UpdateFrom` all do. So the clause has nowhere to be planned
/// into: it was parsed, discarded, and the caller received a command tag for a
/// statement that asked for rows, with nothing anywhere saying the request had
/// been dropped.
///
/// Refusing is the honest answer until an insert can actually return its row.
/// Returning the caller's own submitted values instead would be worse than
/// silence: every other RETURNING in the product returns the STORED row and is
/// gated by the read policy (see the `rls_filters` slot beside each `returning`
/// field), and an echo of the request would match neither.
///
/// Scoped to statements that begin with INSERT rather than to "everything that
/// is not UPDATE/DELETE/MERGE", so no unrelated statement that happens to
/// contain the word is caught by it. `UPSERT` is deliberately not included:
/// the protocol-neutral DDL router claims every UPSERT before this function is
/// reached, and it answers the clause from its own parse rather than from the
/// planner.
fn refuse_unsupported_insert_returning(trimmed: &str, sql: &str) -> Result<(), Error> {
    if !starts_with_ascii_case_insensitive(trimmed, "INSERT") {
        return Ok(());
    }
    if keyword_position_outside_literals(sql, RETURNING_KEYWORD).is_none() {
        return Ok(());
    }
    Err(Error::BadRequest {
        detail: "RETURNING is not supported on INSERT; it is supported on UPDATE, DELETE, and \
                 MERGE. Follow the insert with a SELECT on the inserted key to read the stored \
                 row."
            .to_string(),
    })
}

/// Parse the column list that appears after the RETURNING keyword.
///
/// Supports:
/// - `*`
/// - `col1, col2`
/// - `col1 AS alias1, col2`
///
/// Rejects arithmetic expressions (e.g. `stock * 2`) with a typed error.
fn parse_returning_columns(columns_str: &str) -> Result<ReturningSpec, Error> {
    let columns_str = columns_str.trim();
    if columns_str == "*" {
        return Ok(ReturningSpec {
            columns: ReturningColumns::Star,
        });
    }

    let mut items = Vec::new();
    for raw_item in columns_str.split(',') {
        let item = raw_item.trim();
        if item.is_empty() {
            continue;
        }

        // Reject arithmetic: contains operators that are not part of a name.
        if contains_arithmetic(item) {
            return Err(Error::BadRequest {
                detail: format!(
                    "RETURNING expression '{item}' is not supported; \
                     only bare column names and RETURNING * are allowed"
                ),
            });
        }

        // Parse `name [AS alias]` — case-insensitive AS.
        if let Some(as_pos) = find_ascii_keyword(item, "AS") {
            let name = item[..as_pos].trim().to_string();
            let alias = item[as_pos + 2..].trim().to_string();
            if name.is_empty() || alias.is_empty() {
                return Err(Error::BadRequest {
                    detail: format!("invalid RETURNING column expression: '{item}'"),
                });
            }
            items.push(ReturningItem {
                name,
                alias: Some(alias),
            });
        } else {
            let name = item.to_string();
            if !is_valid_column_name(&name) {
                return Err(Error::BadRequest {
                    detail: format!(
                        "RETURNING expression '{name}' is not supported; \
                         only bare column names and RETURNING * are allowed"
                    ),
                });
            }
            items.push(ReturningItem { name, alias: None });
        }
    }

    if items.is_empty() {
        return Err(Error::BadRequest {
            detail: "empty RETURNING column list".into(),
        });
    }

    Ok(ReturningSpec {
        columns: ReturningColumns::Named(items),
    })
}

/// Return true if the expression token contains arithmetic operators
/// (*, /, +, -) outside of quoted identifiers.
fn contains_arithmetic(expr: &str) -> bool {
    let mut in_quote = false;
    let mut prev = '\0';
    for ch in expr.chars() {
        if ch == '"' {
            in_quote = !in_quote;
            prev = ch;
            continue;
        }
        if in_quote {
            prev = ch;
            continue;
        }
        if matches!(ch, '+' | '/' | '%') {
            return true;
        }
        // `-` is arithmetic only when not a leading sign or part of an identifier.
        if ch == '-' && (prev.is_ascii_alphanumeric() || prev == '_') {
            return true;
        }
        // `*` is arithmetic when preceded by an identifier character.
        if ch == '*' && (prev.is_ascii_alphanumeric() || prev == '_') {
            return true;
        }
        prev = ch;
    }
    false
}

/// Return true if the given name is a valid bare identifier (letters, digits, underscores).
fn is_valid_column_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No insert op carries a `returning` slot, so the clause used to be parsed
    /// away and the caller got a command tag for a statement that asked for
    /// rows. It is refused now, and the refusal says which statements do
    /// support it.
    #[test]
    fn insert_returning_is_refused_rather_than_dropped() {
        for sql in [
            "INSERT INTO items (id, name) VALUES ('a', 'alpha') RETURNING *",
            "insert into items (id) values ('a') returning id",
            "  INSERT INTO items (id) VALUES ('a') RETURNING id AS k",
        ] {
            let error = strip_returning(sql).expect_err("INSERT RETURNING must be refused");
            let detail = error.to_string();
            assert!(
                detail.contains("RETURNING") && detail.contains("UPDATE"),
                "the refusal must name the clause and where it IS supported; got {detail}"
            );
        }
    }

    /// An INSERT with no such clause is untouched — the refusal must not turn
    /// ordinary inserts into errors.
    #[test]
    fn a_plain_insert_is_untouched() {
        let sql = "INSERT INTO items (id, name) VALUES ('a', 'alpha')";
        let (out, spec) = strip_returning(sql).expect("a plain insert must plan");
        assert_eq!(out, sql);
        assert!(spec.is_none());
    }

    /// The word inside a string literal is data, not a clause.
    #[test]
    fn returning_inside_a_string_literal_is_not_a_clause() {
        let sql = "INSERT INTO items (id, note) VALUES ('a', 'RETURNING soon')";
        let (out, spec) = strip_returning(sql).expect("a quoted keyword is not a clause");
        assert_eq!(out, sql);
        assert!(spec.is_none());
    }

    /// The refusal is scoped to INSERT: a statement that merely contains the
    /// word elsewhere is not caught by it.
    #[test]
    fn a_non_insert_statement_is_not_caught_by_the_refusal() {
        let sql = "SELECT returning_count FROM items";
        let (out, spec) = strip_returning(sql).expect("a select must pass through");
        assert_eq!(out, sql);
        assert!(spec.is_none());
    }

    #[test]
    fn strips_star_returning_from_update() {
        let (sql, spec) =
            strip_returning("UPDATE products SET stock = 1 WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "UPDATE products SET stock = 1 WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(spec.columns, ReturningColumns::Star);
    }

    #[test]
    fn strips_named_columns_returning_from_update() {
        let (sql, spec) = strip_returning(
            "UPDATE products SET stock = stock - 1 WHERE id = 'p1' RETURNING id, stock",
        )
        .unwrap();
        assert_eq!(sql, "UPDATE products SET stock = stock - 1 WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![
                ReturningItem {
                    name: "id".into(),
                    alias: None
                },
                ReturningItem {
                    name: "stock".into(),
                    alias: None
                },
            ])
        );
    }

    #[test]
    fn strips_star_returning_from_delete() {
        let (sql, spec) =
            strip_returning("DELETE FROM products WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM products WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(spec.columns, ReturningColumns::Star);
    }

    #[test]
    fn strips_named_returning_from_delete() {
        let (sql, spec) =
            strip_returning("DELETE FROM products WHERE id = 'p1' RETURNING id").unwrap();
        assert_eq!(sql, "DELETE FROM products WHERE id = 'p1'");
        let spec = spec.unwrap();
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "id".into(),
                alias: None
            }])
        );
    }

    #[test]
    fn strips_star_returning_from_merge() {
        let (sql, spec) = strip_returning(
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET stock = s.stock RETURNING *",
        )
        .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET stock = s.stock"
        );
        assert_eq!(spec.unwrap().columns, ReturningColumns::Star);
    }

    #[test]
    fn strips_named_returning_from_merge() {
        let (sql, spec) = strip_returning(
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id) RETURNING id, stock",
        )
        .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO products t USING staging s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
        );
        assert_eq!(
            spec.unwrap().columns,
            ReturningColumns::Named(vec![
                ReturningItem {
                    name: "id".into(),
                    alias: None
                },
                ReturningItem {
                    name: "stock".into(),
                    alias: None
                },
            ])
        );
    }

    #[test]
    fn merge_without_returning_is_unchanged() {
        let original = "MERGE INTO products t USING staging s ON t.id = s.id \
                        WHEN MATCHED THEN DELETE";
        let (sql, spec) = strip_returning(original).unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, original);
    }

    #[test]
    fn no_returning() {
        let (sql, spec) = strip_returning("UPDATE products SET stock = 0 WHERE id = 'p1'").unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, "UPDATE products SET stock = 0 WHERE id = 'p1'");
    }

    #[test]
    fn returning_inside_identifier_not_treated_as_keyword() {
        // A collection/table whose name embeds "returning" (with `_` as an
        // identifier boundary) must NOT match the RETURNING keyword inside the
        // name — the real keyword is the trailing one after WHERE.
        let (sql, spec) =
            strip_returning("DELETE FROM orders_returning WHERE id = 'p1' RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM orders_returning WHERE id = 'p1'");
        assert_eq!(spec.unwrap().columns, ReturningColumns::Star);

        // Same identifier with no trailing RETURNING clause → no spec, unchanged.
        let (sql, spec) = strip_returning("DELETE FROM orders_returning WHERE id = 'p1'").unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, "DELETE FROM orders_returning WHERE id = 'p1'");
    }

    #[test]
    fn returning_in_string_literal_ignored() {
        let (sql, spec) =
            strip_returning("UPDATE products SET note = 'RETURNING soon' WHERE id = 'p1'").unwrap();
        assert!(spec.is_none());
        assert_eq!(
            sql,
            "UPDATE products SET note = 'RETURNING soon' WHERE id = 'p1'"
        );
    }

    #[test]
    fn select_not_affected() {
        let (sql, spec) = strip_returning("SELECT * FROM products").unwrap();
        assert!(spec.is_none());
        assert_eq!(sql, "SELECT * FROM products");
    }

    #[test]
    fn case_insensitive() {
        let (sql, spec) =
            strip_returning("update products set stock = 0 where id = 'p1' returning id").unwrap();
        let spec = spec.unwrap();
        assert_eq!(sql, "update products set stock = 0 where id = 'p1'");
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "id".into(),
                alias: None
            }])
        );
    }

    #[test]
    fn unicode_identifier_before_returning_preserves_original_offsets() {
        let (sql, spec) = strip_returning("DELETE FROM tﬀﬀ RETURNING *").unwrap();
        assert_eq!(sql, "DELETE FROM tﬀﬀ");
        assert_eq!(spec.unwrap().columns, ReturningColumns::Star);
    }

    #[test]
    fn unicode_returning_column_before_alias_preserves_original_offsets() {
        let (_, spec) = strip_returning("UPDATE t SET x = 1 RETURNING ﬀﬀ AS alias").unwrap();
        assert_eq!(
            spec.unwrap().columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "ﬀﬀ".into(),
                alias: Some("alias".into()),
            }])
        );
    }

    #[test]
    fn arithmetic_in_returning_is_error() {
        let result = strip_returning("UPDATE t SET x=1 RETURNING x*2");
        assert!(result.is_err());
        let e = result.unwrap_err().to_string();
        assert!(
            e.contains("not supported") || e.contains("expression"),
            "unexpected error: {e}"
        );
    }

    #[test]
    fn returning_with_alias() {
        let (sql, spec) =
            strip_returning("UPDATE t SET x=2 WHERE id='a' RETURNING x AS new_x").unwrap();
        assert_eq!(sql, "UPDATE t SET x=2 WHERE id='a'");
        let spec = spec.unwrap();
        assert_eq!(
            spec.columns,
            ReturningColumns::Named(vec![ReturningItem {
                name: "x".into(),
                alias: Some("new_x".into()),
            }])
        );
    }

    #[test]
    fn output_names_star_returns_none() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
        };
        assert!(spec.output_names().is_none());
    }

    #[test]
    fn output_names_named_uses_aliases() {
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem {
                    name: "id".into(),
                    alias: None,
                },
                ReturningItem {
                    name: "x".into(),
                    alias: Some("val".into()),
                },
            ]),
        };
        assert_eq!(
            spec.output_names(),
            Some(vec!["id".to_string(), "val".to_string()])
        );
    }
}
