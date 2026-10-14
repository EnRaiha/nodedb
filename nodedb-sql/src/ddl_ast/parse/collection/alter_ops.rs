// SPDX-License-Identifier: Apache-2.0

//! Parse ALTER COLLECTION sub-operations.
//!
//! The sub-operation is chosen by the tokens right after the collection
//! name. A keyword elsewhere in the statement — inside a DEFAULT literal, a
//! column name, or a value — never selects or changes the operation.

use super::super::clause::{ClauseCursor, Tok};
use crate::ddl_ast::statement::AlterCollectionOp;
use crate::error::SqlError;

/// Grammar quoted back to the client whenever a `MATERIALIZED_SUM` clause is
/// present but malformed.
const MATERIALIZED_SUM_SYNTAX: &str = "syntax: ALTER COLLECTION <target> ADD COLUMN <col> <type> \
     MATERIALIZED_SUM SOURCE <source> ON <source>.<col> = <target>.<col> VALUE <source>.<col>";

/// Column-definition clauses that end a column type.
const COLUMN_CLAUSES: &[&[&str]] = &[&["NOT", "NULL"], &["NULL"], &["DEFAULT"]];

/// Parse the sub-operation of an `ALTER COLLECTION` / `ALTER TABLE` statement.
/// `c` is positioned right after the collection name.
///
/// `None` means "not a typed sub-operation" — the statement belongs to another
/// route and the caller falls through. `Some(Err(..))` means the sub-operation
/// was recognised but is malformed, and the error is surfaced to the client as
/// written; falling through instead would let a generic SQL parser reject the
/// statement at the `COLLECTION` keyword with an error naming none of the
/// grammar the user actually got wrong.
pub(super) fn parse_alter_operation(
    c: &mut ClauseCursor<'_, '_>,
    collection_name: &str,
) -> Option<Result<AlterCollectionOp, SqlError>> {
    // Operations owned by the raw-SQL dispatcher: return None so the router
    // falls through to it.
    let raw_sql_owned: &[&[&str]] = &[
        &["ADD", "CONSTRAINT"],
        &["ADD", "PERIOD", "LOCK"],
        &["DROP", "PERIOD", "LOCK"],
        &["SET", "PERMISSION_TREE"],
        &["DROP", "PERMISSION_TREE"],
        &["ADD", "TRANSITION", "CHECK"],
        &["SET", "VECTOR", "METADATA"],
    ];
    if raw_sql_owned.iter().any(|seq| c.peek_keywords(seq)) {
        return None;
    }

    if c.peek_keyword("ADD") {
        if has_top_level_word(c, "MATERIALIZED_SUM") {
            return Some(parse_materialized_sum(c, collection_name));
        }
        return Some(parse_add_column(c));
    }
    if c.eat_keywords(&["DROP", "COLUMN"]) {
        return Some(finish_with(c, |c| {
            Ok(AlterCollectionOp::DropColumn {
                column_name: c.name("column")?,
            })
        }));
    }
    if c.eat_keywords(&["RENAME", "COLUMN"]) {
        return Some(finish_with(c, |c| {
            let old_name = c.name("column")?;
            c.expect_keyword("TO")?;
            let new_name = c.name("new column name")?;
            Ok(AlterCollectionOp::RenameColumn { old_name, new_name })
        }));
    }
    if c.eat_keywords(&["ALTER", "COLUMN"]) {
        return Some(finish_with(c, |c| {
            let column_name = c.name("column")?;
            c.eat_keywords(&["SET", "DATA"]);
            c.expect_keyword("TYPE")?;
            let new_type = c.raw_until("TYPE", &[])?.to_string();
            Ok(AlterCollectionOp::AlterColumnType {
                column_name,
                new_type,
            })
        }));
    }
    if c.eat_keywords(&["OWNER", "TO"]) {
        return Some(finish_with(c, |c| {
            Ok(AlterCollectionOp::OwnerTo {
                new_owner: c.raw("new owner")?.to_string(),
            })
        }));
    }
    if c.eat_keywords(&["SET", "RETENTION"]) {
        return Some(finish_with(c, |c| {
            c.expect_punct('=')?;
            // The duration is reported upper-cased, the form every stored
            // retention value already has.
            let value = set_value(c, "RETENTION")?.to_uppercase();
            Ok(AlterCollectionOp::SetRetention { value })
        }));
    }
    if c.eat_keywords(&["SET", "APPEND_ONLY"]) {
        return Some(finish_with(c, |_| Ok(AlterCollectionOp::SetAppendOnly)));
    }
    if c.eat_keywords(&["SET", "LAST_VALUE_CACHE"]) {
        return Some(finish_with(c, |c| {
            c.expect_punct('=')?;
            let enabled = c.bool_literal("LAST_VALUE_CACHE")?;
            Ok(AlterCollectionOp::SetLastValueCache { enabled })
        }));
    }
    if c.eat_keywords(&["SET", "LEGAL_HOLD"]) {
        return Some(finish_with(c, |c| {
            c.expect_punct('=')?;
            let enabled = c.bool_literal("LEGAL_HOLD")?;
            c.expect_keyword("TAG")?;
            // The tag is reported upper-cased, the form every stored hold
            // tag already has, so a later release matches it.
            let tag = set_value(c, "TAG")?.to_uppercase();
            Ok(AlterCollectionOp::SetLegalHold { enabled, tag })
        }));
    }
    None
}

/// Run `body`, then require the end of the statement.
fn finish_with(
    c: &mut ClauseCursor<'_, '_>,
    body: impl FnOnce(&mut ClauseCursor<'_, '_>) -> Result<AlterCollectionOp, SqlError>,
) -> Result<AlterCollectionOp, SqlError> {
    let op = body(c)?;
    c.finish()?;
    Ok(op)
}

/// A quoted string or a bare word, as the value of `SET <key> = ...`.
fn set_value(c: &mut ClauseCursor<'_, '_>, key: &str) -> Result<String, SqlError> {
    if let Ok(value) = c.string(key) {
        if value.is_empty() {
            return Err(c.error(format!("{key} must not be empty")));
        }
        return Ok(value);
    }
    Ok(c.word(key)?.to_string())
}

/// Whether `word` appears as a bare word token anywhere after the cursor.
fn has_top_level_word(c: &ClauseCursor<'_, '_>, word: &str) -> bool {
    c.remaining_tokens()
        .iter()
        .any(|t| matches!(&t.tok, Tok::Word(w) if w.eq_ignore_ascii_case(word)))
}

/// `ADD [COLUMN] <name> <type> [NOT NULL | NULL] [DEFAULT <expr>]`.
fn parse_add_column(c: &mut ClauseCursor<'_, '_>) -> Result<AlterCollectionOp, SqlError> {
    c.expect_keyword("ADD")?;
    c.eat_keyword("COLUMN");
    let column_name = c.name("column")?;
    let column_type = c.raw_until("column type", COLUMN_CLAUSES)?.to_string();
    let mut not_null = false;
    let mut default_expr = None;
    while !c.at_end() {
        if c.eat_keywords(&["NOT", "NULL"]) {
            c.clause("NULL")?;
            not_null = true;
        } else if c.eat_keyword("NULL") {
            c.clause("NULL")?;
        } else if c.eat_keyword("DEFAULT") {
            c.clause("DEFAULT")?;
            default_expr = Some(c.raw_until("DEFAULT", COLUMN_CLAUSES)?.to_string());
        } else {
            return Err(c.unknown_clause());
        }
    }
    Ok(AlterCollectionOp::AddColumn {
        column_name,
        column_type,
        not_null,
        default_expr,
    })
}

/// Build a `MATERIALIZED_SUM` parse error naming the clause that failed.
fn materialized_sum_error(what: &str) -> SqlError {
    SqlError::Parse {
        detail: format!("MATERIALIZED_SUM: {what}. {MATERIALIZED_SUM_SYNTAX}"),
    }
}

/// Parse `ADD [COLUMN] <col> <type> [AS] MATERIALIZED_SUM SOURCE <src>
/// ON <src>.<col> = <target>.<col> VALUE <src>.<col>` into a typed
/// [`AlterCollectionOp::AddMaterializedSum`].
///
/// Returns a typed parse error if any required keyword is absent, rather than
/// declining the statement: no other route handles `MATERIALIZED_SUM`, so
/// declining would surface an error about the `COLLECTION` keyword instead of
/// about the clause the user actually got wrong.
fn parse_materialized_sum(
    c: &mut ClauseCursor<'_, '_>,
    collection_name: &str,
) -> Result<AlterCollectionOp, SqlError> {
    let fail = |what: &str| materialized_sum_error(what);
    c.expect_keyword("ADD")
        .map_err(|_| fail("missing ADD [COLUMN] clause"))?;
    c.eat_keyword("COLUMN");
    let target_column = c
        .name("column")
        .map_err(|_| fail("missing target column name"))?;

    // The column is real — it is read back with a plain SELECT and written by
    // every maintenance write — so a type is required.
    if c.peek_keyword("MATERIALIZED_SUM") || c.peek_keyword("AS") || c.peek_keyword("DEFAULT") {
        return Err(fail("missing target column type"));
    }
    let target_column_type = c
        .raw_until(
            "column type",
            &[&["AS", "MATERIALIZED_SUM"], &["MATERIALIZED_SUM"]],
        )
        .map_err(|_| fail("missing target column type"))?
        .to_string();
    c.eat_keyword("AS");
    c.expect_keyword("MATERIALIZED_SUM")
        .map_err(|_| fail("MATERIALIZED_SUM must follow the column type"))?;
    if !c.eat_keyword("SOURCE") {
        return Err(fail("missing SOURCE keyword"));
    }
    let source_collection = c
        .name("source collection")
        .map_err(|_| fail("missing source collection name"))?;
    if !c.eat_keyword("ON") {
        return Err(fail("missing ON clause"));
    }
    let on_clause = c
        .raw_until("ON", &[&["VALUE"]])
        .map_err(|_| fail("missing ON clause"))?;
    let join_column = extract_join_column(on_clause, &source_collection).ok_or_else(|| {
        fail("the ON clause must be an equality between a source column and a target column")
    })?;
    if !c.eat_keyword("VALUE") {
        return Err(fail("missing VALUE keyword"));
    }
    let value_raw = c.rest();
    let value_expr = extract_value_expr(value_raw, &source_collection).ok_or_else(|| {
        fail("VALUE must be a single column reference; use a pre-computed column for expressions")
    })?;

    Ok(AlterCollectionOp::AddMaterializedSum {
        target_collection: collection_name.to_lowercase(),
        target_column,
        target_column_type,
        source_collection,
        join_column,
        value_expr,
    })
}

/// Extract the join column from `source.col = target.id` — returns the
/// source-side column. `None` unless the clause is one equality between two
/// single column references.
fn extract_join_column(join_clause: &str, source_coll: &str) -> Option<String> {
    let (left, right) = join_clause.split_once('=')?;
    let left = left.trim().to_lowercase();
    let right = right.trim().to_lowercase();
    let is_column_ref = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.')
    };
    if !is_column_ref(&left) || !is_column_ref(&right) {
        return None;
    }
    let prefix = format!("{source_coll}.");
    let col = if let Some(col) = left.strip_prefix(&prefix) {
        col.to_string()
    } else if let Some(col) = right.strip_prefix(&prefix) {
        col.to_string()
    } else {
        left.rsplit('.').next().unwrap_or(&left).to_string()
    };
    (!col.is_empty()).then_some(col)
}

/// Extract the value column — a simple column reference or qualified
/// `source.column`. Returns `None` for an expression, which requires a
/// pre-computed column.
fn extract_value_expr(expr_str: &str, source_coll: &str) -> Option<String> {
    let lower = expr_str.trim().to_lowercase();
    let prefix = format!("{source_coll}.");
    let col_name = lower.strip_prefix(&prefix).unwrap_or(&lower).to_string();
    (!col_name.is_empty() && col_name.chars().all(|c| c.is_alphanumeric() || c == '_'))
        .then_some(col_name)
}

#[cfg(test)]
mod tests {
    use super::super::super::clause::tokenize;
    use super::*;

    /// Parse the sub-operation of `sql`, which starts with
    /// `ALTER COLLECTION <name>`.
    fn op(sql: &str) -> Option<Result<AlterCollectionOp, SqlError>> {
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "ALTER COLLECTION");
        c.expect_keywords(&["ALTER", "COLLECTION"]).expect("prefix");
        let name = c.name("collection").expect("name");
        parse_alter_operation(&mut c, &name)
    }

    #[test]
    fn materialized_sum_keywords_after_unicode_name_preserve_original_offsets() {
        let sql = "ALTER COLLECTION totalsﬀﬀ ADD COLUMN total INT MATERIALIZED_SUM SOURCE orders ON orders.customer_id = totalsﬀﬀ.id VALUE orders.amount";
        let parsed = op(sql)
            .expect("claimed")
            .expect("materialized sum should parse");
        assert!(matches!(
            parsed,
            AlterCollectionOp::AddMaterializedSum {
                target_collection,
                source_collection,
                join_column,
                value_expr,
                ..
            } if target_collection == "totalsﬀﬀ"
                && source_collection == "orders"
                && join_column == "customer_id"
                && value_expr == "amount"
        ));
    }

    /// A malformed ON clause must produce a `MATERIALIZED_SUM` parse error, not
    /// a decline. Declining hands the statement to the generic SQL parser,
    /// which rejects it at the `COLLECTION` keyword and reports a list of ALTER
    /// targets that says nothing about the clause that is actually wrong.
    #[test]
    fn on_clause_without_an_equality_reports_the_materialized_sum_grammar() {
        let sql = "ALTER COLLECTION accounts ADD COLUMN balance TEXT \
                   MATERIALIZED_SUM SOURCE entries ON account_id VALUE amount";
        let error = op(sql)
            .expect("claimed")
            .expect_err("an ON clause with no equality must not parse");
        let SqlError::Parse { detail } = error else {
            panic!("expected a parse error");
        };
        assert!(detail.contains("MATERIALIZED_SUM"), "{detail}");
        assert!(
            detail.contains("ON <source>.<col> = <target>.<col>"),
            "{detail}"
        );
    }

    #[test]
    fn a_missing_source_keyword_reports_the_materialized_sum_grammar() {
        let sql = "ALTER COLLECTION accounts ADD COLUMN balance TEXT \
                   MATERIALIZED_SUM ON entries.account_id = accounts.id VALUE entries.amount";
        let error = op(sql)
            .expect("claimed")
            .expect_err("a missing SOURCE keyword must not parse");
        let SqlError::Parse { detail } = error else {
            panic!("expected a parse error");
        };
        assert!(detail.contains("missing SOURCE keyword"), "{detail}");
    }

    #[test]
    fn add_column_reads_type_default_and_not_null() {
        let parsed =
            op("ALTER COLLECTION t ADD COLUMN lowered TEXT NOT NULL DEFAULT LOWER(status)")
                .expect("claimed")
                .expect("parses");
        assert_eq!(
            parsed,
            AlterCollectionOp::AddColumn {
                column_name: "lowered".into(),
                column_type: "TEXT".into(),
                not_null: true,
                default_expr: Some("LOWER(status)".into()),
            }
        );
        let parsed = op("ALTER COLLECTION t ADD COLUMN note TEXT DEFAULT 'a b'")
            .expect("claimed")
            .expect("parses");
        assert!(matches!(
            parsed,
            AlterCollectionOp::AddColumn { default_expr: Some(d), .. } if d == "'a b'"
        ));
    }

    #[test]
    fn keyword_inside_a_default_literal_does_not_change_the_operation() {
        let parsed = op("ALTER COLLECTION t ADD COLUMN m TEXT DEFAULT 'ADD CONSTRAINT'")
            .expect("claimed")
            .expect("parses");
        assert!(matches!(parsed, AlterCollectionOp::AddColumn { .. }));
    }

    #[test]
    fn legal_hold_requires_a_boolean_literal() {
        assert!(
            op("ALTER COLLECTION audit SET LEGAL_HOLD = TRU TAG 'x'")
                .expect("claimed")
                .is_err()
        );
        let parsed = op("ALTER COLLECTION audit SET LEGAL_HOLD = TRUE TAG 'litigation'")
            .expect("claimed")
            .expect("parses");
        assert_eq!(
            parsed,
            AlterCollectionOp::SetLegalHold {
                enabled: true,
                tag: "LITIGATION".into()
            }
        );
    }

    #[test]
    fn last_value_cache_requires_a_boolean_literal() {
        assert!(
            op("ALTER COLLECTION m SET LAST_VALUE_CACHE = yes")
                .expect("claimed")
                .is_err()
        );
    }

    #[test]
    fn trailing_tokens_are_refused() {
        assert!(
            op("ALTER COLLECTION t DROP COLUMN a b")
                .expect("claimed")
                .is_err()
        );
        assert!(
            op("ALTER COLLECTION t SET APPEND_ONLY NOW")
                .expect("claimed")
                .is_err()
        );
    }

    #[test]
    fn raw_sql_owned_operations_fall_through() {
        assert!(op("ALTER COLLECTION t ADD CONSTRAINT c CHECK (x > 0)").is_none());
        assert!(op("ALTER COLLECTION t SET PERMISSION_TREE ON x").is_none());
    }
}
