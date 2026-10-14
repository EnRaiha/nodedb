// SPDX-License-Identifier: Apache-2.0

//! Unqualified column references over a join.
//!
//! A join's merged rows key every field `qualifier.column`. The planner
//! rewrites each unqualified reference in a clause read over those rows to
//! its owning relation's qualifier, and refuses a name it cannot place.
//!
//! `articles` declares `id`, `title` and `views`. `orders` and `users` are
//! schemaless.

use super::fixtures::{plan_select_sql, try_plan_select_sql};
use crate::error::SqlError;
use crate::types::*;

const ARTICLES_ORDERS: &str = "FROM articles a JOIN orders o ON a.id = o.article_id";

fn debug(plan: &SqlPlan) -> String {
    format!("{plan:?}")
}

fn column(table: &str, name: &str) -> String {
    format!("Column {{ table: Some({table:?}), name: {name:?} }}")
}

fn bare_column(name: &str) -> String {
    format!("Column {{ table: None, name: {name:?} }}")
}

fn assert_ambiguous(sql: &str, name: &str) {
    match try_plan_select_sql(sql) {
        Err(SqlError::AmbiguousColumn { column }) => assert_eq!(column, name, "{sql}"),
        other => panic!("{sql}: expected AmbiguousColumn({name}), got {other:?}"),
    }
}

#[test]
fn projected_columns_take_their_owners_qualifier() {
    let plan = plan_select_sql(&format!("SELECT title, amount {ARTICLES_ORDERS}"));
    let SqlPlan::Join { projection, .. } = plan else {
        panic!("expected a join plan");
    };
    let names: Vec<String> = projection
        .iter()
        .map(|p| match p {
            Projection::Column(name) => name.clone(),
            other => panic!("expected a column projection, got {other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["a.title".to_string(), "o.amount".to_string()]);
}

#[test]
fn computed_projection_reads_qualified_columns() {
    let plan = plan_select_sql(&format!(
        "SELECT views * 2 AS twice, amount + 1 AS more {ARTICLES_ORDERS}"
    ));
    let text = debug(&plan);
    assert!(text.contains(&column("a", "views")), "{text}");
    assert!(text.contains(&column("o", "amount")), "{text}");
}

#[test]
fn where_reads_qualified_columns() {
    let plan = plan_select_sql(&format!(
        "SELECT a.id {ARTICLES_ORDERS} WHERE amount > 5 AND views IS NULL"
    ));
    let text = debug(&plan);
    assert!(text.contains(&column("o", "amount")), "{text}");
    assert!(text.contains(&column("a", "views")), "{text}");
    assert!(!text.contains(&bare_column("amount")), "{text}");
}

#[test]
fn on_equi_keys_take_their_owners_qualifier() {
    let plan = plan_select_sql("SELECT a.id FROM articles a JOIN orders o ON article_id = id");
    let SqlPlan::Join { on, .. } = plan else {
        panic!("expected a join plan");
    };
    assert_eq!(on, vec![("a.id".to_string(), "o.article_id".to_string())]);
}

#[test]
fn on_residual_reads_qualified_columns() {
    let plan = plan_select_sql(
        "SELECT a.id FROM articles a JOIN orders o ON a.id = o.article_id AND views < qty",
    );
    let SqlPlan::Join { condition, .. } = plan else {
        panic!("expected a join plan");
    };
    let text = format!("{condition:?}");
    assert!(text.contains(&column("a", "views")), "{text}");
    assert!(text.contains(&column("o", "qty")), "{text}");
}

#[test]
fn group_keys_and_aggregate_arguments_take_their_owners_qualifier() {
    let plan = plan_select_sql(&format!(
        "SELECT title, SUM(amount), COUNT(*) FILTER (WHERE qty > 1) {ARTICLES_ORDERS} GROUP BY title"
    ));
    let SqlPlan::Aggregate {
        group_by,
        aggregates,
        ..
    } = plan
    else {
        panic!("expected an aggregate plan");
    };
    assert_eq!(
        format!("{:?}", group_by[0]),
        column("a", "title"),
        "group key"
    );
    let text = format!("{aggregates:?}");
    assert!(text.contains(&column("o", "amount")), "{text}");
    assert!(text.contains(&column("o", "qty")), "{text}");
}

#[test]
fn self_aliased_group_key_takes_its_owners_qualifier() {
    let plan = plan_select_sql(&format!(
        "SELECT title AS title, COUNT(*) {ARTICLES_ORDERS} GROUP BY title"
    ));
    let SqlPlan::Aggregate { group_by, .. } = plan else {
        panic!("expected an aggregate plan");
    };
    assert_eq!(format!("{:?}", group_by[0]), column("a", "title"));
}

#[test]
fn having_reads_group_columns_by_bare_name() {
    for having in ["title <> 'x'", "a.title <> 'x'"] {
        let plan = plan_select_sql(&format!(
            "SELECT title, SUM(amount) {ARTICLES_ORDERS} GROUP BY title HAVING {having}"
        ));
        let SqlPlan::Aggregate { having, .. } = plan else {
            panic!("expected an aggregate plan");
        };
        let text = format!("{having:?}");
        assert!(text.contains(&bare_column("title")), "{text}");
        assert!(!text.contains(&column("a", "title")), "{text}");
    }
}

#[test]
fn having_aggregate_argument_takes_its_owners_qualifier() {
    let plan = plan_select_sql(&format!(
        "SELECT title {ARTICLES_ORDERS} GROUP BY title HAVING SUM(amount) > 10"
    ));
    let SqlPlan::Aggregate { aggregates, .. } = plan else {
        panic!("expected an aggregate plan");
    };
    let text = format!("{aggregates:?}");
    assert!(text.contains(&column("o", "amount")), "{text}");
}

#[test]
fn order_by_over_a_join_sorts_by_the_qualified_column() {
    let plan = plan_select_sql(&format!(
        "SELECT title, amount AS paid {ARTICLES_ORDERS} ORDER BY amount, paid"
    ));
    let SqlPlan::Subquery { sort_keys, .. } = plan else {
        panic!("expected a post-processing tail, got {plan:?}");
    };
    assert_eq!(format!("{:?}", sort_keys[0].expr), column("o", "amount"));
    assert_eq!(
        format!("{:?}", sort_keys[1].expr),
        bare_column("paid"),
        "an output alias stays bare"
    );
}

#[test]
fn order_by_over_a_join_aggregate_sorts_by_the_bare_group_column() {
    let plan = plan_select_sql(&format!(
        "SELECT title, SUM(amount) {ARTICLES_ORDERS} GROUP BY title ORDER BY a.title"
    ));
    let SqlPlan::Aggregate { sort_keys, .. } = plan else {
        panic!("expected an aggregate plan, got {plan:?}");
    };
    assert_eq!(format!("{:?}", sort_keys[0].expr), bare_column("title"));
}

#[test]
fn ambiguous_column_is_refused_in_every_clause() {
    let two_open = "FROM orders o JOIN users u ON o.user_id = u.id";
    assert_ambiguous(&format!("SELECT name {two_open}"), "name");
    assert_ambiguous(&format!("SELECT o.id {two_open} WHERE name = 'x'"), "name");
    assert_ambiguous(&format!("SELECT COUNT(*) {two_open} GROUP BY name"), "name");
    assert_ambiguous(&format!("SELECT SUM(total) {two_open}"), "total");
    assert_ambiguous(&format!("SELECT o.id {two_open} ORDER BY name"), "name");
    assert_ambiguous(
        "SELECT o.id FROM orders o JOIN users u ON o.user_id = u.id AND name = 'x'",
        "name",
    );
    assert_ambiguous(
        "SELECT o.id FROM orders o JOIN users u ON user_id = u.id",
        "user_id",
    );
}

#[test]
fn column_declared_by_both_relations_is_refused() {
    assert_ambiguous(
        "SELECT title FROM articles a JOIN articles b ON a.id = b.id",
        "title",
    );
}

#[test]
fn qualified_column_over_two_schemaless_relations_plans() {
    let plan = plan_select_sql("SELECT u.name FROM orders o JOIN users u ON o.user_id = u.id");
    let SqlPlan::Join { projection, .. } = plan else {
        panic!("expected a join plan");
    };
    assert!(
        matches!(&projection[0], Projection::Column(name) if name == "u.name"),
        "{projection:?}"
    );
}

#[test]
fn using_over_two_schemaless_relations_plans() {
    let plan = plan_select_sql("SELECT o.id FROM orders o JOIN users u USING (tenant)");
    let SqlPlan::Join { on, .. } = plan else {
        panic!("expected a join plan");
    };
    assert_eq!(on, vec![("tenant".to_string(), "tenant".to_string())]);
}

#[test]
fn cross_join_plans_with_no_keys() {
    let plan = plan_select_sql("SELECT a.title, o.amount FROM articles a CROSS JOIN orders o");
    let SqlPlan::Join {
        join_type,
        on,
        condition,
        ..
    } = plan
    else {
        panic!("expected a join plan");
    };
    assert_eq!(join_type, JoinType::Cross);
    assert!(on.is_empty());
    assert!(condition.is_none());
}
