// SPDX-License-Identifier: BUSL-1.1

//! sqlparser AST → internal [`VSelect`] (projection, FROM/JOIN, predicates).

use sqlparser::ast::{
    Expr as SqlExpr, GroupByExpr, Join, JoinConstraint, JoinOperator, LimitClause, ObjectName,
    ObjectNamePart, OrderByExpr, OrderByKind, Query, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::super::expr::Expr;
use super::super::value::VValue;
use super::error::ParseError;
use super::lower::lower_expr;
use super::types::{FromClause, FromRel, JoinKind, JoinSpec, VProj, VSelect};

pub fn parse_select(sql: &str) -> Result<VSelect, ParseError> {
    parse_select_with_params(sql, &[])
}

/// Parse a SELECT, binding `$N` placeholders to concrete values from `params`
/// before lowering.
pub fn parse_select_with_params(
    sql: &str,
    params: &[nodedb_sql::ParamValue],
) -> Result<VSelect, ParseError> {
    let dialect = PostgreSqlDialect {};
    let mut stmts =
        Parser::parse_sql(&dialect, sql).map_err(|e| ParseError::Parse(e.to_string()))?;
    if stmts.len() != 1 {
        return Err(ParseError::Unsupported(
            "expected exactly one SQL statement".into(),
        ));
    }
    let mut stmt = stmts.pop().unwrap();
    if !params.is_empty() {
        nodedb_sql::params::bind_params(&mut stmt, params);
    }
    let Statement::Query(query) = stmt else {
        return Err(ParseError::Unsupported(
            "expected a SELECT statement".into(),
        ));
    };
    select_from_query(*query)
}

fn select_from_query(query: Query) -> Result<VSelect, ParseError> {
    if query.with.is_some() {
        return Err(ParseError::Unsupported("WITH (CTE) not supported".into()));
    }
    let SetExpr::Select(select) = *query.body else {
        return Err(ParseError::Unsupported(
            "compound SELECT (UNION/INTERSECT/EXCEPT) not supported".into(),
        ));
    };

    let group_by_empty = matches!(
        &select.group_by,
        GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty()
    );
    if !group_by_empty {
        return Err(ParseError::Unsupported("GROUP BY not supported".into()));
    }
    if select.having.is_some() {
        return Err(ParseError::Unsupported("HAVING not supported".into()));
    }
    if select.distinct.is_some() {
        return Err(ParseError::Unsupported("DISTINCT not supported".into()));
    }

    let from = lower_from(select.from)?;

    let mut projection = Vec::with_capacity(select.projection.len());
    let mut has_aggregate = false;
    for item in select.projection {
        match item {
            SelectItem::Wildcard(_) => projection.push(VProj::Star),
            SelectItem::QualifiedWildcard(kind, _) => {
                projection.push(VProj::QualifiedStar(qualified_wildcard_alias(&kind)));
            }
            SelectItem::UnnamedExpr(e) => {
                let expr = lower_expr(e)?;
                has_aggregate |= matches!(expr, Expr::Aggregate(_, _));
                projection.push(VProj::Expr { expr, alias: None });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = lower_expr(expr)?;
                has_aggregate |= matches!(expr, Expr::Aggregate(_, _));
                projection.push(VProj::Expr {
                    expr,
                    alias: Some(alias.value),
                });
            }
        }
    }

    let filter = match select.selection {
        Some(e) => Some(lower_expr(e)?),
        None => None,
    };

    let mut order_by_items: Vec<(Expr, bool)> = Vec::new();
    if let Some(ob) = query.order_by {
        match ob.kind {
            OrderByKind::Expressions(exprs) => {
                for OrderByExpr { expr, options, .. } in exprs {
                    order_by_items.push((lower_expr(expr)?, options.asc.unwrap_or(true)));
                }
            }
            OrderByKind::All(_) => {
                return Err(ParseError::Unsupported(
                    "ORDER BY ALL not supported on virtual tables".into(),
                ));
            }
        }
    }

    let (limit, offset) = lower_limit(query.limit_clause)?;

    Ok(VSelect {
        projection,
        from,
        filter,
        order_by: order_by_items,
        limit,
        offset,
        has_aggregate,
    })
}

fn lower_from(from: Vec<TableWithJoins>) -> Result<FromClause, ParseError> {
    let mut iter = from.into_iter();
    let Some(first) = iter.next() else {
        return Ok(FromClause::default());
    };
    let base = Some(lower_table_factor(first.relation)?);
    let mut joins: Vec<JoinSpec> = Vec::new();
    for j in first.joins {
        joins.push(lower_join(j)?);
    }
    // `FROM a, b` — comma-separated relations are cross joins.
    for twj in iter {
        joins.push(JoinSpec {
            rel: lower_table_factor(twj.relation)?,
            kind: JoinKind::Cross,
            on: None,
        });
        for j in twj.joins {
            joins.push(lower_join(j)?);
        }
    }
    Ok(FromClause { base, joins })
}

fn lower_join(join: Join) -> Result<JoinSpec, ParseError> {
    let rel = lower_table_factor(join.relation)?;
    let (kind, constraint) = match join.join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => (JoinKind::Inner, Some(c)),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinKind::Left, Some(c)),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (JoinKind::Right, Some(c)),
        JoinOperator::FullOuter(c) => (JoinKind::Full, Some(c)),
        JoinOperator::CrossJoin(c) => (JoinKind::Cross, Some(c)),
        other => {
            return Err(ParseError::Unsupported(format!(
                "join type not supported on virtual catalog tables: {other:?}"
            )));
        }
    };
    let on = match constraint {
        Some(JoinConstraint::On(expr)) => Some(lower_expr(expr)?),
        Some(JoinConstraint::None) | None => None,
        Some(JoinConstraint::Using(_)) => {
            return Err(ParseError::Unsupported(
                "USING join clause not supported on virtual catalog tables".into(),
            ));
        }
        Some(JoinConstraint::Natural) => {
            return Err(ParseError::Unsupported(
                "NATURAL join not supported on virtual catalog tables".into(),
            ));
        }
    };
    if kind != JoinKind::Cross && on.is_none() {
        return Err(ParseError::Unsupported(
            "join without an ON clause not supported on virtual catalog tables".into(),
        ));
    }
    Ok(JoinSpec { rel, kind, on })
}

fn lower_table_factor(tf: TableFactor) -> Result<FromRel, ParseError> {
    match tf {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            if args.is_some() {
                return Err(ParseError::Unsupported(
                    "table-valued functions not supported on virtual catalog tables".into(),
                ));
            }
            let table = object_name_key(&name);
            let alias = alias.map(|a| a.name.value).unwrap_or_else(|| table.clone());
            Ok(FromRel { table, alias })
        }
        other => Err(ParseError::Unsupported(format!(
            "FROM item not supported on virtual catalog tables: {other:?}"
        ))),
    }
}

/// Normalize a (possibly schema-qualified) relation name to the lookup key:
/// `pg_catalog.pg_class` → `pg_class`, `_system.audit_log` → `_system.audit_log`.
fn object_name_key(name: &ObjectName) -> String {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(id) => Some(id.value.to_ascii_lowercase()),
            ObjectNamePart::Function(_) => None,
        })
        .collect();
    match parts.as_slice() {
        [schema, rest @ ..] if schema == "pg_catalog" && !rest.is_empty() => rest.join("."),
        [schema, ..] if schema == "_system" => parts.join("."),
        _ => parts.last().cloned().unwrap_or_default(),
    }
}

fn qualified_wildcard_alias(kind: &sqlparser::ast::SelectItemQualifiedWildcardKind) -> String {
    use sqlparser::ast::SelectItemQualifiedWildcardKind as Kind;
    match kind {
        Kind::ObjectName(name) => name
            .0
            .iter()
            .filter_map(|p| match p {
                ObjectNamePart::Identifier(id) => Some(id.value.clone()),
                ObjectNamePart::Function(_) => None,
            })
            .next_back()
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn lower_limit(clause: Option<LimitClause>) -> Result<(Option<usize>, usize), ParseError> {
    match clause {
        None => Ok((None, 0)),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return Err(ParseError::Unsupported(
                    "LIMIT BY not supported on virtual tables".into(),
                ));
            }
            let lim = match limit {
                Some(e) => Some(literal_usize(e)?),
                None => None,
            };
            let off = match offset {
                Some(o) => literal_usize(o.value)?,
                None => 0,
            };
            Ok((lim, off))
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            Ok((Some(literal_usize(limit)?), literal_usize(offset)?))
        }
    }
}

fn literal_usize(e: SqlExpr) -> Result<usize, ParseError> {
    match lower_expr(e)? {
        Expr::Literal(VValue::Int4(i)) if i >= 0 => Ok(i as usize),
        Expr::Literal(VValue::Int8(i)) if i >= 0 => Ok(i as usize),
        _ => Err(ParseError::Unsupported(
            "LIMIT/OFFSET must be a non-negative integer literal".into(),
        )),
    }
}
