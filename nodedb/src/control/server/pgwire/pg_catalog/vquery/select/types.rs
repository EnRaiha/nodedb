// SPDX-License-Identifier: BUSL-1.1

//! Lowered SELECT representation: projection, FROM/JOIN tree, predicates.

use super::super::expr::Expr;

#[derive(Debug, Clone)]
pub struct VSelect {
    pub projection: Vec<VProj>,
    pub from: FromClause,
    pub filter: Option<Expr>,
    pub order_by: Vec<(Expr, bool /*asc*/)>,
    pub limit: Option<usize>,
    pub offset: usize,
    /// True if any projection item is a top-level aggregate. The whole
    /// projection is then evaluated once over the row set (single implicit
    /// group spanning all rows; no GROUP BY).
    pub has_aggregate: bool,
}

#[derive(Debug, Clone)]
pub enum VProj {
    Star,
    /// `t.*` — every column of the named relation alias.
    QualifiedStar(String),
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

/// The FROM clause: an optional base relation plus zero or more joins. A
/// missing base (`None`) is a no-`FROM` scalar SELECT.
#[derive(Debug, Clone, Default)]
pub struct FromClause {
    pub base: Option<FromRel>,
    pub joins: Vec<JoinSpec>,
}

impl FromClause {
    /// Every relation referenced (base first, then joined), in order.
    pub fn relations(&self) -> Vec<&FromRel> {
        let mut out: Vec<&FromRel> = Vec::new();
        if let Some(base) = &self.base {
            out.push(base);
        }
        for j in &self.joins {
            out.push(&j.rel);
        }
        out
    }
}

/// A relation in the FROM clause. `alias` defaults to the table name when the
/// query supplies none, so qualified references always have a key to match.
#[derive(Debug, Clone)]
pub struct FromRel {
    pub table: String,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct JoinSpec {
    pub rel: FromRel,
    pub kind: JoinKind,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}
