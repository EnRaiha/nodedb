// SPDX-License-Identifier: BUSL-1.1

//! WHERE, RETURN, and ORDER BY clause parsers.
//!
//! Each parser reads its whole section. Text it cannot read is a
//! `BadRequest` error, never a dropped predicate or column.

use super::super::ast::*;
use super::helpers::{
    bad, find_keyword_all, find_top_level_operator, is_word_byte, single_quoted,
    split_top_level_commas, split_top_level_keyword, top_level_mask,
};
use super::parse_match_clauses;

/// Parse the WHERE section: predicates joined by `AND`.
pub(super) fn parse_where(text: &str) -> crate::Result<Vec<WherePredicate>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(bad("MATCH: WHERE needs a predicate"));
    }
    split_top_level_keyword(trimmed, "AND")
        .into_iter()
        .map(|part| parse_predicate(part.trim()))
        .collect()
}

fn parse_predicate(part: &str) -> crate::Result<WherePredicate> {
    if part.is_empty() {
        return Err(bad("MATCH: empty predicate next to AND in WHERE"));
    }
    if split_top_level_keyword(part, "OR").len() > 1 {
        return Err(bad(format!(
            "MATCH: OR is not supported in WHERE; predicates combine with AND only: '{part}'"
        )));
    }
    let mask = top_level_mask(part);
    if let Some(&(0, end)) = find_keyword_all(part, &mask, "NOT EXISTS").first() {
        return parse_not_exists(part[end..].trim());
    }
    parse_comparison_predicate(part)
}

/// Parse `{ MATCH ... }` after `NOT EXISTS`. The braces must enclose the
/// whole rest of the predicate and hold exactly one MATCH clause.
fn parse_not_exists(rest: &str) -> crate::Result<WherePredicate> {
    let mask = top_level_mask(rest);
    let enclosed = rest.len() >= 2
        && rest.starts_with('{')
        && rest.ends_with('}')
        && mask[1..rest.len() - 1].iter().all(|top| !top);
    if !enclosed {
        return Err(bad(format!(
            "MATCH: NOT EXISTS requires {{ MATCH ... }}, found '{rest}'"
        )));
    }
    let mut inner = parse_match_clauses(rest[1..rest.len() - 1].trim())?;
    if inner.len() != 1 {
        return Err(bad(format!(
            "MATCH: NOT EXISTS takes exactly one MATCH clause, found {}",
            inner.len()
        )));
    }
    let sub_pattern = inner.remove(0);
    Ok(WherePredicate::NotExists { sub_pattern })
}

/// Parse a comparison predicate: `a.name = 'Alice'`, `a.age > 25`, `a = 'n0'`.
fn parse_comparison_predicate(text: &str) -> crate::Result<WherePredicate> {
    let (pos, op_str) = find_top_level_operator(text).ok_or_else(|| {
        bad(format!(
            "MATCH: unsupported WHERE predicate '{text}'; expected \
             <binding>[.<field>] <op> <value> or NOT EXISTS {{ MATCH ... }}"
        ))
    })?;
    let op = match op_str {
        "<=" => ComparisonOp::Lte,
        ">=" => ComparisonOp::Gte,
        "<>" | "!=" => ComparisonOp::Neq,
        "<" => ComparisonOp::Lt,
        ">" => ComparisonOp::Gt,
        _ => ComparisonOp::Eq,
    };
    let lhs = text[..pos].trim();
    let rhs = text[pos + op_str.len()..].trim();

    let is_name = |s: &str| !s.is_empty() && s.bytes().all(is_word_byte);
    let (binding, field) = match lhs.split_once('.') {
        Some((b, f)) if is_name(b) && is_name(f) => (b.to_string(), f.to_string()),
        None if is_name(lhs) => (lhs.to_string(), String::new()),
        _ => {
            return Err(bad(format!(
                "MATCH: WHERE left side must be <binding> or <binding>.<field>, found '{lhs}'"
            )));
        }
    };

    let literal = if rhs.starts_with('\'') {
        single_quoted(rhs)
    } else if let Some(rest) = rhs.strip_prefix('"') {
        rest.strip_suffix('"')
            .filter(|inner| !inner.contains('"'))
            .map(str::to_string)
    } else {
        (!rhs.is_empty() && !rhs.contains(char::is_whitespace)).then(|| rhs.to_string())
    };
    let value = literal.ok_or_else(|| {
        bad(format!(
            "MATCH: WHERE value must be one literal or word, found '{rhs}'"
        ))
    })?;

    Ok(if op == ComparisonOp::Eq {
        WherePredicate::Equals {
            binding,
            field,
            value,
        }
    } else {
        WherePredicate::Comparison {
            binding,
            field,
            op,
            value,
        }
    })
}

/// Parse the RETURN section into columns and the DISTINCT flag. `*` means
/// every bound variable and gives no columns.
pub(super) fn parse_return(text: &str) -> crate::Result<(Vec<ReturnColumn>, bool)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(bad("MATCH: RETURN needs at least one column"));
    }
    if trimmed == "*" {
        return Ok((Vec::new(), false));
    }

    let mask = top_level_mask(trimmed);
    let (effective, distinct) = match find_keyword_all(trimmed, &mask, "DISTINCT").first() {
        Some(&(0, end)) => (trimmed[end..].trim(), true),
        _ => (trimmed, false),
    };
    if effective.is_empty() {
        return Err(bad("MATCH: RETURN DISTINCT needs at least one column"));
    }

    let columns = split_top_level_commas(effective)
        .into_iter()
        .map(parse_return_column)
        .collect::<crate::Result<Vec<_>>>()?;
    Ok((columns, distinct))
}

fn parse_return_column(col: &str) -> crate::Result<ReturnColumn> {
    let col = col.trim();
    if col.is_empty() {
        return Err(bad("MATCH: empty column in RETURN"));
    }
    let mask = top_level_mask(col);
    let Some(&(as_start, as_end)) = find_keyword_all(col, &mask, "AS").last() else {
        return Ok(ReturnColumn {
            expr: col.to_string(),
            alias: None,
        });
    };
    let expr = col[..as_start].trim();
    let alias = col[as_end..].trim();
    if expr.is_empty() || alias.is_empty() || alias.contains(char::is_whitespace) {
        return Err(bad(format!(
            "MATCH: RETURN column must be <expr> [AS <alias>], found '{col}'"
        )));
    }
    Ok(ReturnColumn {
        expr: expr.to_string(),
        alias: Some(alias.to_string()),
    })
}

/// Parse the ORDER BY section. Each column takes an optional `ASC` or
/// `DESC`, in any case.
pub(super) fn parse_order_by(text: &str) -> crate::Result<Vec<OrderByColumn>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(bad("MATCH: ORDER BY needs at least one column"));
    }
    split_top_level_commas(trimmed)
        .into_iter()
        .map(|col| {
            let col = col.trim();
            let (expr, ascending) = match col.rsplit_once(char::is_whitespace) {
                Some((head, dir)) if dir.eq_ignore_ascii_case("DESC") => (head.trim(), false),
                Some((head, dir)) if dir.eq_ignore_ascii_case("ASC") => (head.trim(), true),
                _ => (col, true),
            };
            if expr.is_empty() {
                return Err(bad("MATCH: empty column in ORDER BY"));
            }
            Ok(OrderByColumn {
                expr: expr.to_string(),
                ascending,
            })
        })
        .collect()
}
