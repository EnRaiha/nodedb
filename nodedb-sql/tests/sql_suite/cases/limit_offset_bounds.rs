// SPDX-License-Identifier: BUSL-1.1

//! A row bound the planner cannot resolve must be rejected, never dropped.
//!
//! `LIMIT` and `OFFSET` values reach the planner as
//! `sqlparser::ast::Expr`. The extractor behind them
//! (`crate::coerce::expr_as_usize_literal`) answers `Option<usize>`, which
//! collapses three distinct inputs into one `None`:
//!
//! - no clause at all,
//! - a clause whose value is not a literal the planner can read,
//! - a clause whose value IS readable and is out of the `usize` domain
//!   (negative, fractional, wider than `usize`, non-numeric text).
//!
//! Every consumer maps that `None` onto a permissive default — `None` limit
//! means unbounded, `unwrap_or(0)` offset means skip nothing. So an invalid
//! bound widens the query instead of failing it: `LIMIT -1` scans the whole
//! collection. PostgreSQL rejects the same input with SQLSTATE `2201W`
//! (`invalid_limit_value`).
//!
//! Each test asserts the rejection AND that no plan came back carrying the
//! permissive default, so a regression to any silent-widening shape fails
//! here rather than returning a plan that quietly reads everything.

use nodedb_sql::types::{CollectionInfo, EngineType, SqlPlan};
use nodedb_sql::{SqlCatalog, SqlCatalogError, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            "articles" | "authors" => Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentStrict,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            }),
            _ => None,
        };
        Ok(info)
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

/// The row bound a plan ended up carrying, for the failure message when a
/// query that must be rejected is planned anyway.
fn bound_of(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { limit, offset, .. }
        | SqlPlan::DocumentIndexLookup { limit, offset, .. }
        | SqlPlan::Subquery { limit, offset, .. } => format!("limit={limit:?} offset={offset}"),
        SqlPlan::Cte { outer, .. } => format!("cte outer -> {}", bound_of(outer)),
        other => format!("{other:?}"),
    }
}

/// Assert the statement is rejected at plan time.
///
/// The guard is the point: an accepted plan is reported with the bound it
/// carries, so the silent-widening failure mode (`limit=None`, `offset=0`)
/// is named in the failure output instead of showing up as a missing error.
fn expect_rejected(sql: &str) {
    match plan_sql(sql, &Catalog) {
        Err(_) => {}
        Ok(plans) => {
            let bounds: Vec<String> = plans.iter().map(bound_of).collect();
            panic!("must reject an out-of-domain row bound, planned instead: {sql} -> {bounds:?}");
        }
    }
}

/// The single plan for a statement that must plan cleanly.
fn plan_one(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).expect("planning must succeed");
    assert_eq!(plans.len(), 1, "expected exactly one plan for: {sql}");
    plans.pop().expect("one plan")
}

// ---------------------------------------------------------------------------
// LIMIT
// ---------------------------------------------------------------------------

/// The reported symptom: a negative LIMIT literal.
#[test]
fn negative_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT -1");
}

/// The untyped-parameter spelling of the same bound. A pgwire driver that
/// sends `Type::UNKNOWN` binds the value as a single-quoted string, so
/// `LIMIT $1` with `-1` reaches the planner as `LIMIT '-1'`.
#[test]
fn negative_limit_as_unknown_param_text_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT '-1'");
}

/// Non-numeric text in a LIMIT is not a bound at all.
#[test]
fn non_numeric_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT 'abc'");
}

/// A fractional LIMIT has no `usize` reading.
#[test]
fn fractional_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT 1.5");
}

/// A LIMIT wider than `usize` cannot be applied and must not be dropped.
#[test]
fn overflowing_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT 99999999999999999999999999");
}

// ---------------------------------------------------------------------------
// OFFSET
// ---------------------------------------------------------------------------

/// A negative OFFSET currently collapses to `unwrap_or(0)` — the rows the
/// query asked to skip are returned instead.
#[test]
fn negative_offset_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET -2");
}

/// The untyped-parameter spelling of a negative OFFSET.
#[test]
fn negative_offset_as_unknown_param_text_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET '-2'");
}

/// Non-numeric text in an OFFSET is not a bound at all.
#[test]
fn non_numeric_offset_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET 'abc'");
}

/// An OFFSET wider than `usize` cannot be applied and must not be dropped.
#[test]
fn overflowing_offset_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET 99999999999999999999999999");
}

// ---------------------------------------------------------------------------
// Nested query shapes — the same extractor, reached through other planners
// ---------------------------------------------------------------------------

/// A derived table carries its own LIMIT clause through the same extractor.
#[test]
fn negative_limit_in_derived_table_is_rejected() {
    expect_rejected("SELECT * FROM (SELECT * FROM articles LIMIT -1) s");
}

/// A CTE body carries its own LIMIT clause through the same extractor.
#[test]
fn negative_limit_in_cte_body_is_rejected() {
    expect_rejected("WITH c AS (SELECT * FROM articles LIMIT -1) SELECT * FROM c");
}

/// `plan_lateral` reads the inner LIMIT with its own copy of the extractor
/// (`limit_from_query`), so it needs its own coverage.
#[test]
fn negative_limit_in_lateral_subquery_is_rejected() {
    expect_rejected(
        "SELECT a.id, e.id FROM authors a \
         JOIN LATERAL (SELECT id FROM articles WHERE articles.id = a.id LIMIT -1) e ON true",
    );
}

// ---------------------------------------------------------------------------
// Controls — valid bounds keep working
// ---------------------------------------------------------------------------

/// `LIMIT 0` is a valid bound meaning "no rows", not a missing bound.
#[test]
fn zero_limit_is_honored() {
    match plan_one("SELECT * FROM articles LIMIT 0") {
        SqlPlan::Scan { limit, .. } => assert_eq!(limit, Some(0)),
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// A plain positive bound still plans.
#[test]
fn positive_limit_and_offset_are_honored() {
    match plan_one("SELECT * FROM articles LIMIT 5 OFFSET 2") {
        SqlPlan::Scan { limit, offset, .. } => {
            assert_eq!(limit, Some(5));
            assert_eq!(offset, 2);
        }
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// No clause at all stays unbounded — the one input for which `None` is the
/// correct answer.
#[test]
fn absent_limit_stays_unbounded() {
    match plan_one("SELECT * FROM articles") {
        SqlPlan::Scan { limit, offset, .. } => {
            assert_eq!(limit, None);
            assert_eq!(offset, 0);
        }
        other => panic!("expected a Scan, got: {other:?}"),
    }
}
