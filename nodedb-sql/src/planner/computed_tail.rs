// SPDX-License-Identifier: Apache-2.0

//! Computed SELECT-list items over an access path that evaluates none.
//!
//! A spatial predicate scan and a secondary-index fetch return stored rows
//! and run no expression. A computed item over one of them is evaluated by a
//! [`SqlPlan::Subquery`] tail instead. The body emits every stored column the
//! items read. The tail evaluates each item over those rows, then projects
//! the SELECT list. An `ORDER BY` on a computed alias sorts the tail's rows,
//! which carry the alias.

use crate::types::plan::referenced_columns;
use crate::types::{Projection, SqlPlan};

/// The row bound and dedup a tail applies over its body's rows.
#[derive(Debug, Clone, Copy, Default)]
pub struct TailBounds {
    pub distinct: bool,
    pub limit: Option<usize>,
    pub offset: usize,
}

/// Whether `projection` holds an item the body cannot evaluate.
///
/// A renamed column (`name AS n`) counts: the body emits stored names only.
pub fn needs_computed_tail(projection: &[Projection]) -> bool {
    projection
        .iter()
        .any(|p| matches!(p, Projection::Computed { .. }))
}

/// The projection the body emits for `projection`.
///
/// Without a computed item it is `projection` itself. With one, it is every
/// plain column the list names plus every column an item reads, each once.
/// A star keeps every column, so the result is empty.
pub fn body_projection(projection: &[Projection]) -> Vec<Projection> {
    if !needs_computed_tail(projection) {
        return projection.to_vec();
    }
    if projection
        .iter()
        .any(|p| matches!(p, Projection::Star | Projection::QualifiedStar(_)))
    {
        return Vec::new();
    }
    let mut columns: Vec<String> = Vec::with_capacity(projection.len());
    let mut keep = |name: String| {
        if !columns.contains(&name) {
            columns.push(name);
        }
    };
    for item in projection {
        match item {
            Projection::Column(name) => keep(name.clone()),
            Projection::Computed { expr, .. } | Projection::CpComputed { expr, .. } => {
                referenced_columns(expr).into_iter().for_each(&mut keep);
            }
            Projection::Star | Projection::QualifiedStar(_) => {}
        }
    }
    columns.into_iter().map(Projection::Column).collect()
}

/// Wrap `body` in a tail that evaluates `projection` over its rows.
///
/// `body` must already carry [`body_projection`] of `projection`. Without a
/// computed item `body` returns unchanged, and `bounds` stays on the body.
pub fn with_computed_tail(body: SqlPlan, projection: &[Projection], bounds: TailBounds) -> SqlPlan {
    if !needs_computed_tail(projection) {
        return body;
    }
    SqlPlan::Subquery {
        input: Box::new(body),
        filters: Vec::new(),
        projection: projection.to_vec(),
        window_functions: Vec::new(),
        sort_keys: Vec::new(),
        offset: bounds.offset,
        distinct: bounds.distinct,
        limit: bounds.limit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SqlExpr;

    fn column(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.into(),
        }
    }

    fn computed(alias: &str, expr: SqlExpr) -> Projection {
        Projection::Computed {
            expr,
            alias: alias.into(),
        }
    }

    #[test]
    fn a_plain_projection_needs_no_tail() {
        let projection = vec![Projection::Column("name".into())];
        assert!(!needs_computed_tail(&projection));
        assert_eq!(body_projection(&projection).len(), 1);
    }

    #[test]
    fn the_body_emits_every_column_a_computed_item_reads() {
        let projection = vec![
            Projection::Column("name".into()),
            computed(
                "km",
                SqlExpr::Function {
                    name: "st_x".into(),
                    args: vec![column("location")],
                    distinct: false,
                },
            ),
            computed("n", column("name")),
        ];
        let names: Vec<String> = body_projection(&projection)
            .into_iter()
            .map(|p| match p {
                Projection::Column(name) => name,
                other => panic!("the body projects plain columns only, got {other:?}"),
            })
            .collect();
        assert_eq!(names, ["name", "location"]);
    }

    #[test]
    fn a_star_beside_a_computed_item_keeps_every_column() {
        let projection = vec![Projection::Star, computed("n", column("name"))];
        assert!(body_projection(&projection).is_empty());
    }
}
