// SPDX-License-Identifier: Apache-2.0

//! Best-effort type inference for `$N` prepared-statement placeholders.
//!
//! The pgwire extended-query protocol lets a client send `Parse` without
//! declaring any parameter OIDs. The server then has to answer `Describe`
//! with a `ParameterDescription`, and answering "unknown" (OID 0) for every
//! position forces well-behaved clients into a failure: `tokio-postgres`
//! refuses to serialize an `i64` against an unknown OID. This module walks
//! the *unsubstituted* statement and reports the positions whose type the
//! SQL itself pins down.
//!
//! # Why a separate walk rather than planner output
//!
//! The planner cannot plan an AST that still contains placeholders — the
//! resolver has no `Value::Placeholder` arm — and the Control Plane's
//! schema-inference pass therefore rewrites `$N` to `NULL` in the SQL text
//! before planning. That rewrite destroys the position → type link, so
//! inference has to happen here, on the parsed-but-unbound AST.
//!
//! # Directionality: under-infer, never over-infer
//!
//! A PostgreSQL client that receives an unknown parameter type sends the
//! value in text format, which the bind layer already handles — so leaving
//! a position unresolved only costs a text round-trip. Reporting a concrete
//! OID, by contrast, makes the client commit to that type's *binary*
//! encoding. Any position whose type is not pinned down by the SQL stays
//! `None`.

use core::ops::ControlFlow;

use sqlparser::ast::{Expr, LimitClause, Query, Statement, Value, ValueWithSpan, Visit, Visitor};

use crate::parser::array_stmt::try_parse_array_statement;
use crate::parser::preprocess;
use crate::parser::statement::parse_sql;
use crate::types_expr::SqlDataType;

/// Best-effort, catalog-aware inference of `$N` placeholder types.
///
/// Returns one slot per placeholder, indexed by `N - 1`. `None` means the
/// position is not one this pass can resolve unambiguously.
///
/// Under-inference is always safe: a PostgreSQL client that receives an
/// unknown parameter type sends the value in text format, which this server
/// already handles. Over-inference is NOT safe — reporting a concrete OID
/// makes the client commit to a binary encoding for that type, so any
/// position whose type is ambiguous MUST stay `None` rather than be guessed.
///
/// The pass never errors and never panics: unparseable SQL simply yields an
/// empty result.
///
/// # Resolved forms
///
/// * `LIMIT $N` / `OFFSET $N` (including the `UPDATE`/`DELETE` limit forms)
///   → [`SqlDataType::Int64`].
/// * `$N::<type>` and `CAST($N AS <type>)` → the named type.
///
/// Column-backed forms (`WHERE col = $N`, `INSERT INTO t (col) VALUES ($N)`)
/// need catalog lookup and are deliberately left `None` here; resolving them
/// is a matter of threading a catalog into [`InferenceContext`] and
/// extending the visitor, not of restructuring this pass.
pub fn infer_placeholder_types(sql: &str) -> Vec<Option<SqlDataType>> {
    let Some(statements) = parse_best_effort(sql) else {
        return Vec::new();
    };
    let mut ctx = InferenceContext::default();
    for stmt in &statements {
        // `Visit` only breaks when the visitor asks it to; this one never does.
        let _ = stmt.visit(&mut ctx);
    }
    ctx.finish()
}

/// Parse `sql` the same way `plan_sql` does, but swallowing every failure.
///
/// Mirrors `plan_sql`'s front end so a statement that plans successfully is
/// also one this pass sees: NodeDB's `ARRAY` DDL/DML family bypasses
/// sqlparser entirely (and carries no placeholders), and everything else goes
/// through the preprocessor before `parse_sql`. If the preprocessor rejects
/// the input we still try the raw text, since a preprocessor-only failure
/// (e.g. an unsupported NodeDB extension) does not imply the placeholders are
/// unreadable.
fn parse_best_effort(sql: &str) -> Option<Vec<Statement>> {
    // Array statements accept no bound parameters.
    if let Ok(Some(_)) = try_parse_array_statement(sql) {
        return None;
    }
    let preprocessed = preprocess::preprocess(sql).ok().flatten();
    let effective_sql = preprocessed.as_ref().map_or(sql, |p| p.sql.as_str());
    parse_sql(effective_sql).ok()
}

/// One placeholder position's inference state.
#[derive(Clone, PartialEq)]
enum Slot {
    /// Seen, but in no position this pass can type.
    Unresolved,
    /// Typed by exactly one form (or by several that agree).
    Resolved(SqlDataType),
    /// Typed by two forms that disagree — e.g. `LIMIT $1` and `$1::TEXT`
    /// in the same statement. Reported as unknown, never as a guess.
    Conflicted,
}

/// Accumulated inference state for one SQL string.
///
/// Column-backed inference will add a catalog reference here; every visitor
/// method already routes its conclusions through [`Self::record`], so that
/// extension is a parameter thread rather than a rewrite.
#[derive(Default)]
struct InferenceContext {
    slots: Vec<Slot>,
}

impl InferenceContext {
    /// Grow the slot table so a 1-based placeholder index is addressable.
    fn slot_mut(&mut self, index_1based: usize) -> Option<&mut Slot> {
        let zero_based = index_1based.checked_sub(1)?;
        if self.slots.len() <= zero_based {
            self.slots.resize(zero_based + 1, Slot::Unresolved);
        }
        self.slots.get_mut(zero_based)
    }

    /// Note that `$N` exists without typing it.
    fn observe(&mut self, index_1based: usize) {
        let _ = self.slot_mut(index_1based);
    }

    /// Record a resolved type for `$N`, demoting to `Conflicted` when a
    /// different type was already recorded for the same position.
    fn record(&mut self, index_1based: usize, ty: SqlDataType) {
        let Some(slot) = self.slot_mut(index_1based) else {
            return;
        };
        let next = match slot {
            Slot::Unresolved => Slot::Resolved(ty),
            Slot::Resolved(existing) if *existing == ty => Slot::Resolved(ty),
            Slot::Resolved(_) | Slot::Conflicted => Slot::Conflicted,
        };
        *slot = next;
    }

    /// Record `$N` as an integer when `expr` is a bare placeholder.
    ///
    /// Used for every row-count position (`LIMIT`, `OFFSET`), all of which
    /// PostgreSQL types as `bigint`.
    fn record_row_count(&mut self, expr: &Expr) {
        if let Some(index) = placeholder_index(expr) {
            self.record(index, SqlDataType::Int64);
        }
    }

    fn record_limit_clause(&mut self, clause: &LimitClause) {
        match clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by: _,
            } => {
                // `LIMIT BY <expr>,...` is a ClickHouse grouping key, not a
                // row count — nothing to infer from it.
                if let Some(limit) = limit {
                    self.record_row_count(limit);
                }
                if let Some(offset) = offset {
                    self.record_row_count(&offset.value);
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                self.record_row_count(offset);
                self.record_row_count(limit);
            }
        }
    }

    fn finish(self) -> Vec<Option<SqlDataType>> {
        self.slots
            .into_iter()
            .map(|slot| match slot {
                Slot::Resolved(ty) => Some(ty),
                Slot::Unresolved | Slot::Conflicted => None,
            })
            .collect()
    }
}

impl Visitor for InferenceContext {
    type Break = ();

    fn pre_visit_value(&mut self, value: &Value) -> ControlFlow<Self::Break> {
        // Every placeholder position sqlparser defines reaches this hook —
        // the same coverage `params::ParamBinder` relies on for binding — so
        // the result is sized by the highest index that actually exists.
        if let Value::Placeholder(body) = value
            && let Some(index) = parse_placeholder_body(body)
        {
            self.observe(index);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        // `$1::INT` and `CAST($1 AS INT)` differ only in `CastKind`; both
        // name the parameter's type outright.
        if let Expr::Cast {
            expr: inner,
            data_type,
            ..
        } = expr
            && let Some(index) = placeholder_index(inner)
            && let Some(ty) = type_name_to_sql_data_type(&data_type.to_string())
        {
            self.record(index, ty);
        }
        // Any other expression shape: not a form this pass infers. The walk
        // continues into it regardless — this hook only adds conclusions.
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(clause) = &query.limit_clause {
            self.record_limit_clause(clause);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        // `UPDATE ... LIMIT $N` / `DELETE ... LIMIT $N` carry their own limit
        // expression outside any `Query`, so `pre_visit_query` never sees it.
        match statement {
            Statement::Update(update) => {
                if let Some(limit) = &update.limit {
                    self.record_row_count(limit);
                }
            }
            Statement::Delete(delete) => {
                if let Some(limit) = &delete.limit {
                    self.record_row_count(limit);
                }
            }
            // Any other statement: no statement-level typed position. Nested
            // queries and expressions are still visited by the other hooks.
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// The 1-based index of `expr` when it is (a parenthesised) `$N`.
fn placeholder_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Placeholder(body),
            ..
        }) => parse_placeholder_body(body),
        Expr::Nested(inner) => placeholder_index(inner),
        // Not a bare placeholder — nothing to attribute a type to.
        _ => None,
    }
}

/// Parse the `1` out of a `$1` placeholder body.
///
/// Returns `None` for any other placeholder spelling (`?`, `:name`, `$`,
/// `$0`, `$abc`) rather than assuming a position.
fn parse_placeholder_body(body: &str) -> Option<usize> {
    let digits = body.strip_prefix('$')?;
    let index: usize = digits.parse().ok()?;
    (index > 0).then_some(index)
}

/// Map a SQL type name to the planner's resolved type.
///
/// Takes the rendered type name (sqlparser's `DataType` `Display`, which is
/// how `resolver::expr::convert` and `planner::const_fold` already carry cast
/// targets) and normalises it the same way `const_fold::fold_cast` does:
/// upper-cased, with any `(precision, scale)` suffix stripped.
///
/// `None` for an unrecognised name — including types that have no faithful
/// wire representation on the caller's side yet. Adding a name here widens
/// what `Describe` promises, so only add one whose value a client can
/// actually round-trip.
fn type_name_to_sql_data_type(type_name: &str) -> Option<SqlDataType> {
    let upper = type_name.to_uppercase();
    let base = upper
        .split('(')
        .next()
        .map(str::trim)
        .unwrap_or(upper.as_str());

    match base {
        "INT" | "INTEGER" | "INT2" | "INT4" | "INT8" | "INT64" | "SMALLINT" | "BIGINT" => {
            Some(SqlDataType::Int64)
        }
        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" => {
            Some(SqlDataType::Float64)
        }
        "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" | "BPCHAR" => {
            Some(SqlDataType::String)
        }
        "BOOL" | "BOOLEAN" => Some(SqlDataType::Bool),
        "BYTEA" | "BYTES" | "BLOB" => Some(SqlDataType::Bytes),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => Some(SqlDataType::Timestamp),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Some(SqlDataType::Timestamptz),
        // Unrecognised type name — the position stays unknown, which costs a
        // text-format round-trip and nothing else.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_placeholder_is_int64() {
        assert_eq!(
            infer_placeholder_types("SELECT id FROM t LIMIT $1"),
            vec![Some(SqlDataType::Int64)]
        );
    }

    #[test]
    fn offset_placeholder_is_int64() {
        assert_eq!(
            infer_placeholder_types("SELECT id FROM t LIMIT 10 OFFSET $1"),
            vec![Some(SqlDataType::Int64)]
        );
    }

    #[test]
    fn limit_and_offset_placeholders_are_both_int64() {
        assert_eq!(
            infer_placeholder_types("SELECT id FROM t LIMIT $1 OFFSET $2"),
            vec![Some(SqlDataType::Int64), Some(SqlDataType::Int64)]
        );
    }

    #[test]
    fn double_colon_cast_resolves_target_type() {
        assert_eq!(
            infer_placeholder_types("SELECT $1::INT"),
            vec![Some(SqlDataType::Int64)]
        );
    }

    #[test]
    fn cast_as_syntax_resolves_target_type() {
        assert_eq!(
            infer_placeholder_types("SELECT CAST($1 AS TEXT)"),
            vec![Some(SqlDataType::String)]
        );
    }

    #[test]
    fn cast_target_types_cover_each_mapped_family() {
        let cases: &[(&str, SqlDataType)] = &[
            ("BIGINT", SqlDataType::Int64),
            ("SMALLINT", SqlDataType::Int64),
            ("DOUBLE PRECISION", SqlDataType::Float64),
            ("REAL", SqlDataType::Float64),
            ("VARCHAR(10)", SqlDataType::String),
            ("BOOLEAN", SqlDataType::Bool),
            ("BYTEA", SqlDataType::Bytes),
            ("TIMESTAMP", SqlDataType::Timestamp),
            ("TIMESTAMPTZ", SqlDataType::Timestamptz),
        ];
        for (name, expected) in cases {
            assert_eq!(
                infer_placeholder_types(&format!("SELECT CAST($1 AS {name})")),
                vec![Some(expected.clone())],
                "cast to {name} must resolve to {expected:?}"
            );
        }
    }

    /// Types with no faithful wire representation on the pgwire side stay
    /// unresolved rather than being narrowed to something a client cannot
    /// round-trip.
    #[test]
    fn unmapped_cast_target_stays_none() {
        assert_eq!(infer_placeholder_types("SELECT $1::NUMERIC"), vec![None]);
        assert_eq!(infer_placeholder_types("SELECT $1::UUID"), vec![None]);
    }

    /// Slot assignment follows the placeholder index, not the order the
    /// positions appear in the statement.
    #[test]
    fn out_of_order_indices_land_in_correct_slots() {
        assert_eq!(
            infer_placeholder_types("SELECT id FROM t WHERE a = $2 LIMIT $1"),
            vec![Some(SqlDataType::Int64), None]
        );
    }

    /// A repeated index typed once by a resolvable position keeps that type.
    #[test]
    fn repeated_index_resolved_once_keeps_its_type() {
        assert_eq!(
            infer_placeholder_types("SELECT id FROM t WHERE a = $1 LIMIT $1"),
            vec![Some(SqlDataType::Int64)]
        );
    }

    /// Two resolvable positions that disagree about the same index leave it
    /// unknown — reporting either would over-infer.
    #[test]
    fn conflicting_types_for_one_index_stay_none() {
        assert_eq!(
            infer_placeholder_types("SELECT $1::TEXT FROM t LIMIT $1"),
            vec![None]
        );
    }

    /// A bare comparison against a column is a catalog-backed form this pass
    /// deliberately does not resolve.
    #[test]
    fn column_comparison_stays_none() {
        assert_eq!(
            infer_placeholder_types("SELECT id FROM t WHERE col = $1"),
            vec![None]
        );
    }

    /// Sizing follows the highest index seen, so unmentioned lower indices
    /// still get a slot.
    #[test]
    fn result_is_sized_to_highest_index() {
        let inferred = infer_placeholder_types("SELECT id FROM t WHERE a = $3 LIMIT $1");
        assert_eq!(inferred.len(), 3);
        assert_eq!(inferred[0], Some(SqlDataType::Int64));
        assert_eq!(inferred[1], None);
        assert_eq!(inferred[2], None);
    }

    #[test]
    fn statement_without_placeholders_is_empty() {
        assert!(infer_placeholder_types("SELECT id FROM t").is_empty());
    }

    #[test]
    fn unparseable_sql_returns_empty() {
        assert!(infer_placeholder_types("this is not sql at all $1").is_empty());
        assert!(infer_placeholder_types("").is_empty());
        assert!(infer_placeholder_types("SELECT FROM WHERE $1 $2").is_empty());
    }

    /// A non-`$N` placeholder spelling must neither panic nor claim a slot.
    #[test]
    fn malformed_placeholder_bodies_claim_no_slot() {
        assert!(parse_placeholder_body("$").is_none());
        assert!(parse_placeholder_body("$0").is_none());
        assert!(parse_placeholder_body("$abc").is_none());
        assert!(parse_placeholder_body("?").is_none());
        assert!(parse_placeholder_body("").is_none());
        assert_eq!(parse_placeholder_body("$7"), Some(7));
    }

    /// `?` placeholders carry no position, so nothing is reported for them.
    #[test]
    fn positionless_placeholder_reports_nothing() {
        assert!(infer_placeholder_types("SELECT id FROM t WHERE a = ?").is_empty());
    }

    #[test]
    fn insert_values_placeholders_are_counted_but_untyped() {
        assert_eq!(
            infer_placeholder_types("INSERT INTO t (id, n) VALUES ($1, $2)"),
            vec![None, None]
        );
    }

    #[test]
    fn update_limit_placeholder_is_int64() {
        assert_eq!(
            infer_placeholder_types("UPDATE t SET n = 1 WHERE id > 0 LIMIT $1"),
            vec![Some(SqlDataType::Int64)]
        );
    }

    #[test]
    fn subquery_limit_placeholder_is_int64() {
        assert_eq!(
            infer_placeholder_types("SELECT * FROM (SELECT id FROM t LIMIT $1) s WHERE s.id = $2"),
            vec![Some(SqlDataType::Int64), None]
        );
    }

    #[test]
    fn cte_cast_placeholder_resolves() {
        assert_eq!(
            infer_placeholder_types("WITH x AS (SELECT $1::BIGINT AS v) SELECT v FROM x"),
            vec![Some(SqlDataType::Int64)]
        );
    }
}
