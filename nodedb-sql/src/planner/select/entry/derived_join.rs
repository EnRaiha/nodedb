// SPDX-License-Identifier: Apache-2.0

//! Derived tables as JOIN sides: `JOIN (SELECT ...) AS b ON ...`.
//!
//! A derived side plans to a one-definition CTE whose outer query scans the
//! alias. The alias qualifies the side's columns, and the derived relation
//! declares the columns its body projects.

use super::fixtures::{plan_select_sql, try_plan_select_sql};
use crate::error::SqlError;
use crate::types::*;

const COUNTS: &str = "(SELECT article_id, COUNT(*) AS n FROM orders GROUP BY article_id)";

/// The alias and body of a derived side, panicking on any other side.
fn derived(side: &SqlPlan) -> (&str, &SqlPlan) {
    let SqlPlan::Cte(CtePlan { definitions, outer }) = side else {
        panic!("expected a derived side, got {side:?}");
    };
    let SqlPlan::Scan {
        collection,
        alias: Some(alias),
        ..
    } = outer.as_ref()
    else {
        panic!("expected a scan of the derived alias, got {outer:?}");
    };
    assert_eq!(collection, alias);
    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].0, *alias);
    (alias.as_str(), &definitions[0].1)
}

fn join_sides(plan: &SqlPlan) -> (&SqlPlan, &SqlPlan, &[(String, String)], JoinType) {
    let SqlPlan::Join {
        left,
        right,
        on,
        join_type,
        ..
    } = plan
    else {
        panic!("expected a join, got {plan:?}");
    };
    (left, right, on, *join_type)
}

#[test]
fn derived_right_side_plans_as_a_named_cte() {
    let plan = plan_select_sql(&format!(
        "SELECT a.title, b.n FROM articles a JOIN {COUNTS} b ON a.id = b.article_id"
    ));
    let (left, right, on, join_type) = join_sides(&plan);
    assert!(matches!(left, SqlPlan::Scan { collection, .. } if collection == "articles"));
    let (alias, body) = derived(right);
    assert_eq!(alias, "b");
    assert!(matches!(body, SqlPlan::Aggregate { .. }), "{body:?}");
    assert_eq!(on, [("a.id".to_string(), "b.article_id".to_string())]);
    assert_eq!(join_type, JoinType::Inner);
}

#[test]
fn derived_left_side_plans_as_a_named_cte() {
    let plan = plan_select_sql(
        "SELECT a.title, o.amount FROM (SELECT id, title FROM articles WHERE views > 1) a \
         JOIN orders o ON a.id = o.article_id",
    );
    let (left, right, on, _) = join_sides(&plan);
    let (alias, body) = derived(left);
    assert_eq!(alias, "a");
    assert!(matches!(body, SqlPlan::Scan { collection, .. } if collection == "articles"));
    assert!(matches!(right, SqlPlan::Scan { collection, .. } if collection == "orders"));
    assert_eq!(on, [("a.id".to_string(), "o.article_id".to_string())]);
}

#[test]
fn every_join_type_takes_a_derived_side() {
    for (keyword, join_type) in [
        ("LEFT JOIN", JoinType::Left),
        ("RIGHT JOIN", JoinType::Right),
        ("FULL JOIN", JoinType::Full),
        ("JOIN", JoinType::Inner),
    ] {
        let plan = plan_select_sql(&format!(
            "SELECT a.title, b.n FROM articles a {keyword} {COUNTS} b ON a.id = b.article_id"
        ));
        let (_, right, _, planned) = join_sides(&plan);
        assert_eq!(planned, join_type, "{keyword}");
        assert_eq!(derived(right).0, "b", "{keyword}");
    }
    let plan = plan_select_sql(&format!(
        "SELECT a.title, b.n FROM articles a CROSS JOIN {COUNTS} b"
    ));
    let (_, right, on, join_type) = join_sides(&plan);
    assert_eq!(join_type, JoinType::Cross);
    assert!(on.is_empty());
    assert_eq!(derived(right).0, "b");
}

/// The derived relation declares its projected columns, so an unqualified
/// one resolves to the derived side.
#[test]
fn unqualified_derived_column_takes_the_derived_alias() {
    let plan = plan_select_sql(&format!(
        "SELECT title, n FROM articles a JOIN {COUNTS} b ON a.id = b.article_id"
    ));
    let SqlPlan::Join { projection, .. } = plan else {
        panic!("expected a join");
    };
    let names: Vec<&str> = projection
        .iter()
        .map(|p| match p {
            Projection::Column(name) => name.as_str(),
            other => panic!("expected a column, got {other:?}"),
        })
        .collect();
    assert_eq!(names, ["a.title", "b.n"]);
}

#[test]
fn derived_side_in_a_multi_way_join() {
    let plan = plan_select_sql(
        "SELECT a.title, u.name FROM articles a \
         JOIN orders o ON a.id = o.article_id \
         JOIN (SELECT id, name FROM users) u ON o.user_id = u.id",
    );
    let (left, right, on, _) = join_sides(&plan);
    assert!(matches!(left, SqlPlan::Join { .. }));
    assert_eq!(derived(right).0, "u");
    assert_eq!(on, [("o.user_id".to_string(), "u.id".to_string())]);
}

/// A join body emits its projected columns under their bare names, the
/// names the derived relation declares.
#[test]
fn a_join_body_emits_its_bare_output_names() {
    let plan = plan_select_sql(
        "SELECT a.title, b.name FROM articles a JOIN \
         (SELECT o.article_id, u.name FROM orders o JOIN users u ON o.user_id = u.id) b \
         ON a.id = b.article_id",
    );
    let (_, right, _, _) = join_sides(&plan);
    let (_, body) = derived(right);
    let SqlPlan::Join { projection, .. } = body else {
        panic!("expected a join body, got {body:?}");
    };
    let aliases: Vec<&str> = projection
        .iter()
        .map(|p| match p {
            Projection::Computed { alias, .. } => alias.as_str(),
            other => panic!("expected a renamed column, got {other:?}"),
        })
        .collect();
    assert_eq!(aliases, ["article_id", "name"]);
}

#[test]
fn aggregate_over_a_derived_join_reads_the_derived_column() {
    let plan = plan_select_sql(&format!(
        "SELECT b.n, COUNT(*) FROM articles a JOIN {COUNTS} b ON a.id = b.article_id GROUP BY b.n"
    ));
    let SqlPlan::Aggregate {
        input, group_by, ..
    } = plan
    else {
        panic!("expected an aggregate");
    };
    let (_, right, _, _) = join_sides(&input);
    assert_eq!(derived(right).0, "b");
    assert_eq!(
        format!("{:?}", group_by[0]),
        "Column { table: Some(\"b\"), name: \"n\" }"
    );
}

/// A derived table without LATERAL cannot see the other relations of its
/// FROM clause. PostgreSQL refuses it and points to LATERAL.
#[test]
fn correlated_derived_side_is_refused_with_a_lateral_hint() {
    let err = try_plan_select_sql(
        "SELECT a.title FROM articles a \
         JOIN (SELECT id FROM orders WHERE orders.article_id = a.id) b ON a.id = b.id",
    )
    .expect_err("a correlated derived table needs LATERAL");
    match err {
        SqlError::Unsupported { detail } => {
            assert!(
                detail.contains("LATERAL") && detail.contains("\"a\""),
                "{detail}"
            )
        }
        other => panic!("expected the LATERAL refusal, got {other:?}"),
    }
}

#[test]
fn derived_side_without_an_alias_is_refused() {
    let err = try_plan_select_sql(
        "SELECT a.title FROM articles a JOIN (SELECT id FROM orders) ON a.id = a.id",
    )
    .expect_err("a derived side needs an alias");
    assert!(
        matches!(err, SqlError::Unsupported { ref detail } if detail.contains("alias")),
        "{err:?}"
    );
}

/// The join body every rename test reads.
const JOIN_BODY: &str =
    "(SELECT o.amount, a.title FROM orders o JOIN articles a ON o.article_id = a.id)";

/// A join body's projection, which must emit each column under its output
/// name: `o.amount` as `amount`, `a.title` as `title`.
fn assert_emits_output_names(body: &SqlPlan) {
    let SqlPlan::Join { projection, .. } = body else {
        panic!("expected a join body, got {body:?}");
    };
    let aliases: Vec<&str> = projection
        .iter()
        .map(|item| match item {
            Projection::Computed {
                expr: SqlExpr::Column { table: Some(_), .. },
                alias,
            } => alias.as_str(),
            other => panic!("expected a renamed column, got {other:?}"),
        })
        .collect();
    assert_eq!(aliases, ["amount", "title"]);
}

#[test]
fn a_with_definition_over_a_join_emits_its_output_names() {
    let plan = plan_select_sql(&format!(
        "WITH c AS {JOIN_BODY} SELECT amount, title FROM c"
    ));
    let SqlPlan::Cte(CtePlan { definitions, .. }) = &plan else {
        panic!("expected a CTE plan, got {plan:?}");
    };
    assert_emits_output_names(&definitions[0].1);
}

#[test]
fn a_derived_from_table_over_a_join_emits_its_output_names() {
    let plan = plan_select_sql(&format!("SELECT amount, title FROM {JOIN_BODY} AS c"));
    let SqlPlan::Cte(CtePlan { definitions, .. }) = &plan else {
        panic!("expected a CTE plan, got {plan:?}");
    };
    assert_emits_output_names(&definitions[0].1);
}

/// A later WITH definition reads an earlier one by name.
#[test]
fn a_with_definition_reads_an_earlier_one() {
    let plan = plan_select_sql(
        "WITH c AS (SELECT id, title FROM articles), d AS (SELECT title FROM c) \
         SELECT title FROM d",
    );
    let SqlPlan::Cte(CtePlan { definitions, outer }) = &plan else {
        panic!("expected a CTE plan, got {plan:?}");
    };
    assert_eq!(definitions.len(), 2);
    assert!(
        matches!(&definitions[0].1, SqlPlan::Scan { collection, .. } if collection == "articles"),
        "{:?}",
        definitions[0].1
    );
    assert!(
        matches!(&definitions[1].1, SqlPlan::Scan { collection, .. } if collection == "c"),
        "{:?}",
        definitions[1].1
    );
    assert!(
        matches!(outer.as_ref(), SqlPlan::Scan { collection, .. } if collection == "d"),
        "{outer:?}"
    );
}
