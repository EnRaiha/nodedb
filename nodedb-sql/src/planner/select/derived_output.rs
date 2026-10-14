// SPDX-License-Identifier: Apache-2.0

//! The rows of a CTE or derived-table body, keyed by its output names.
//!
//! The outer query reads a CTE body or a derived table body by the names
//! the body declares (`amount`). A join body emits each projected column
//! under its qualified name (`o.amount`), so a join body is renamed before
//! it becomes a CTE definition, a derived table in FROM, or a derived JOIN
//! side.

use crate::types::*;

/// A body's rows keyed by its output column names.
///
/// Each qualified column of a join body's final projection is renamed to its
/// bare name. A join with no projection, a `*`, and every other body already
/// emit their output names.
pub(crate) fn rekey_join_output(body: SqlPlan) -> SqlPlan {
    match body {
        SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            projection,
            filters,
        } => SqlPlan::Join {
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            projection: bare_named(projection),
            filters,
        },
        SqlPlan::Subquery {
            input,
            filters,
            projection,
            window_functions,
            sort_keys,
            offset,
            distinct,
            limit,
        } if matches!(*input, SqlPlan::Join { .. }) => {
            // An empty tail projection passes the join's own columns
            // through, so the tail takes the join's projection, renamed.
            let projection = if projection.is_empty() {
                match input.as_ref() {
                    SqlPlan::Join { projection, .. } => bare_named(projection.clone()),
                    _ => Vec::new(),
                }
            } else {
                bare_named(projection)
            };
            SqlPlan::Subquery {
                input,
                filters,
                projection,
                window_functions,
                sort_keys,
                offset,
                distinct,
                limit,
            }
        }
        other => other,
    }
}

/// `projection` with each qualified column emitted under its bare name.
fn bare_named(projection: Vec<Projection>) -> Vec<Projection> {
    projection
        .into_iter()
        .map(|item| match item {
            Projection::Column(qualified) => match qualified.rsplit_once('.') {
                Some((table, name)) => Projection::Computed {
                    expr: SqlExpr::Column {
                        table: Some(table.to_string()),
                        name: name.to_string(),
                    },
                    alias: name.to_string(),
                },
                None => Projection::Column(qualified),
            },
            other => other,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_join_columns_are_emitted_under_their_bare_names() {
        let renamed = bare_named(vec![
            Projection::Column("o.amount".into()),
            Projection::Column("id".into()),
            Projection::Star,
        ]);
        assert!(matches!(
            &renamed[0],
            Projection::Computed { alias, expr: SqlExpr::Column { table: Some(t), name } }
                if alias == "amount" && t == "o" && name == "amount"
        ));
        assert!(matches!(&renamed[1], Projection::Column(c) if c == "id"));
        assert!(matches!(&renamed[2], Projection::Star));
    }
}
