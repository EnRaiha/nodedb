// SPDX-License-Identifier: BUSL-1.1

//! SQL parsing helpers for `CREATE CONTINUOUS AGGREGATE`.
//!
//! The parsing logic, keyword tables, auto-alias derivation, and SQLSTATE
//! codes live here. Errors are the protocol-neutral [`DdlError`].

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from, rfind_ascii_case_insensitive,
};

use crate::control::server::shared::ddl::sql_parse::{
    parse_ident_or_wildcard_token, parse_ident_token,
};
use crate::engine::timeseries::continuous_agg::{
    AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy,
};

use super::super::super::result::DdlError;

const KW_CONTINUOUS_AGGREGATE: &str = "CONTINUOUS AGGREGATE ";
const KW_ON: &str = " ON ";
const KW_BUCKET: &str = "BUCKET";
const KW_AGGREGATE: &str = "AGGREGATE ";
const KW_GROUP_BY: &str = "GROUP BY ";
const KW_AS: &str = " AS ";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Parse CREATE CONTINUOUS AGGREGATE SQL.
///
/// Syntax:
/// ```text
/// CREATE CONTINUOUS AGGREGATE <name> ON <source>
///   BUCKET '<interval>'
///   AGGREGATE <func>(col) [AS alias], ...
///   [GROUP BY col, ...]
///   [WITH (refresh_policy = '...', retention = '...')]
/// ```
pub(super) fn parse_create_sql(sql: &str) -> Result<ContinuousAggregateDef, DdlError> {
    // Extract name: word after "CONTINUOUS AGGREGATE"
    let ca_pos = find_ascii_case_insensitive(sql, KW_CONTINUOUS_AGGREGATE)
        .ok_or_else(|| err("42601", "expected CONTINUOUS AGGREGATE keyword".to_string()))?;
    let after_ca_start = ca_pos + KW_CONTINUOUS_AGGREGATE.len();
    let after_ca = sql[after_ca_start..].trim_start();
    let name = parse_ident_token(
        after_ca
            .split_whitespace()
            .next()
            .ok_or_else(|| err("42601", "missing aggregate name".to_string()))?,
    )?;

    // Extract source: word after "ON"
    let on_pos = find_ascii_case_insensitive_from(sql, KW_ON, after_ca_start)
        .ok_or_else(|| err("42601", "expected ON <source> clause".to_string()))?;
    let after_on_start = on_pos + KW_ON.len();
    let after_on = sql[after_on_start..].trim_start();
    let source = parse_ident_token(
        after_on
            .split_whitespace()
            .next()
            .ok_or_else(|| err("42601", "missing source collection name".to_string()))?,
    )?;

    // Extract bucket interval: between BUCKET ' and '
    let bucket_interval = extract_quoted_value(sql, sql, KW_BUCKET)
        .ok_or_else(|| err("42601", "expected BUCKET '<interval>' clause".to_string()))?;

    let bucket_interval_ms = nodedb_types::kv_parsing::parse_interval_to_ms(&bucket_interval)
        .map_err(|e| err("42601", format!("invalid bucket interval: {e}")))?
        as i64;

    // Extract aggregates: between AGGREGATE and GROUP BY / WITH / end
    let aggregates = extract_aggregates(sql, sql)?;
    if aggregates.is_empty() {
        return Err(err(
            "42601",
            "expected AGGREGATE <func>(col), ... clause".to_string(),
        ));
    }

    // Extract GROUP BY columns (optional).
    let group_by = extract_group_by(sql, sql)?;

    // Extract WITH options (optional).
    let (refresh_policy, retention_period_ms) = extract_with_options(sql, sql)?;

    Ok(ContinuousAggregateDef {
        // Placeholder: the caller (`create_continuous_aggregate`) rebuilds
        // the def with the session's real `database_id`. This parse-only
        // intermediate never reaches the catalog or Data Plane.
        database_id: nodedb_types::DatabaseId::DEFAULT.as_u64(),
        name,
        source,
        bucket_interval,
        bucket_interval_ms,
        group_by,
        aggregates,
        refresh_policy,
        retention_period_ms,
        stale: false,
        // The caller stamps the CREATE's incarnation.
        incarnation: nodedb_types::Hlc::ZERO,
    })
}

/// Extract a quoted value after a keyword: `KEYWORD 'value'`.
pub(super) fn extract_quoted_value(_upper: &str, sql: &str, keyword: &str) -> Option<String> {
    let pos = find_ascii_case_insensitive(sql, keyword)?;
    let after = sql[pos + keyword.len()..].trim_start();
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

/// Extract aggregate expressions from AGGREGATE clause.
///
/// Parses: `AGGREGATE sum(value) AS value_sum, count(*) AS row_count, avg(cpu)`
fn extract_aggregates(_upper: &str, sql: &str) -> Result<Vec<AggregateExpr>, DdlError> {
    // Find standalone AGGREGATE keyword. Skip past "CONTINUOUS AGGREGATE" by
    // searching after the BUCKET clause (which always precedes AGGREGATE).
    let search_start = find_ascii_case_insensitive(sql, KW_BUCKET).unwrap_or(0);
    let agg_pos = match find_ascii_case_insensitive_from(sql, KW_AGGREGATE, search_start) {
        Some(position) => position,
        None => return Ok(Vec::new()),
    };
    let after_agg_start = agg_pos + KW_AGGREGATE.len();

    // Find end: GROUP BY, WITH, or end of string.
    let end_pos = [KW_GROUP_BY, "WITH (", "WITH("]
        .iter()
        .filter_map(|kw| find_ascii_case_insensitive_from(sql, kw, after_agg_start))
        .min()
        .unwrap_or(sql.len());

    let agg_str = sql[after_agg_start..end_pos].trim().trim_end_matches(',');
    let mut exprs = Vec::new();

    for part in split_top_level(agg_str) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let expr = parse_single_aggregate(part)?;
        exprs.push(expr);
    }

    Ok(exprs)
}

/// Split `s` on the commas outside parentheses, so a two-argument function
/// such as `percentile(v, 0.99)` stays one expression.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Parse a single aggregate expression: `func(col) [AS alias]`.
fn parse_single_aggregate(s: &str) -> Result<AggregateExpr, DdlError> {
    // Split on AS for alias.
    let (func_part, alias) = if let Some(as_pos) = find_ascii_case_insensitive(s, KW_AS) {
        (
            &s[..as_pos],
            Some(parse_ident_token(s[as_pos + KW_AS.len()..].trim())?),
        )
    } else {
        (s, None)
    };
    let func_part = func_part.trim();

    // Parse func(col).
    let open = func_part
        .find('(')
        .ok_or_else(|| err("42601", format!("expected function(column) syntax: {s}")))?;
    let close = func_part
        .rfind(')')
        .ok_or_else(|| err("42601", format!("missing closing parenthesis: {s}")))?;

    // The function word is matched against a fixed set, so it lowercases as a
    // keyword. The first argument names a column, or `*` for `count(*)`. The
    // sketch functions take a second, numeric argument.
    let func_name = func_part[..open].trim().to_lowercase();
    let args: Vec<&str> = func_part[open + 1..close]
        .split(',')
        .map(str::trim)
        .collect();
    let col_name = parse_ident_or_wildcard_token(args.first().copied().unwrap_or_default())?;
    let arity = |expected: usize| {
        if args.len() == expected {
            Ok(())
        } else {
            Err(err(
                "42601",
                format!(
                    "{func_name} takes {expected} argument(s), got {}: {s}",
                    args.len()
                ),
            ))
        }
    };

    let function = match func_name.as_str() {
        "sum"
        | "count"
        | "min"
        | "max"
        | "avg"
        | "first"
        | "last"
        | "count_distinct"
        | "approx_count_distinct" => {
            arity(1)?;
            match func_name.as_str() {
                "sum" => AggFunction::Sum,
                "count" => AggFunction::Count,
                "min" => AggFunction::Min,
                "max" => AggFunction::Max,
                "avg" => AggFunction::Avg,
                "first" => AggFunction::First,
                "last" => AggFunction::Last,
                _ => AggFunction::CountDistinct,
            }
        }
        "percentile" | "ts_percentile" | "approx_percentile" => {
            arity(2)?;
            let q: f64 = args[1].parse().map_err(|_| {
                err(
                    "22023",
                    format!("{func_name} quantile '{}' is not a number", args[1]),
                )
            })?;
            if !(0.0..=1.0).contains(&q) {
                return Err(err(
                    "22023",
                    format!("{func_name} quantile {q} is outside [0, 1]"),
                ));
            }
            AggFunction::Percentile(q)
        }
        "topk" | "approx_topk" => {
            arity(2)?;
            let k: usize = args[1].parse().map_err(|_| {
                err(
                    "22023",
                    format!("{func_name} k '{}' is not a count", args[1]),
                )
            })?;
            if k == 0 {
                return Err(err("22023", format!("{func_name} k must be at least 1")));
            }
            AggFunction::TopK(k)
        }
        other => {
            return Err(err("42601", format!("unknown aggregate function: {other}")));
        }
    };

    let output_column = alias.unwrap_or_else(|| {
        if col_name == "*" {
            func_name.clone()
        } else {
            format!("{func_name}_{col_name}")
        }
    });

    Ok(AggregateExpr {
        function,
        source_column: col_name,
        output_column,
    })
}

/// Extract GROUP BY columns.
///
/// Returns an error when a column token is not a usable SQL identifier.
fn extract_group_by(_upper: &str, sql: &str) -> Result<Vec<String>, DdlError> {
    let gb_pos = match find_ascii_case_insensitive(sql, KW_GROUP_BY) {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };
    let after_gb_start = gb_pos + KW_GROUP_BY.len();

    // Find end: WITH or end of string.
    let end_pos = ["WITH (", "WITH("]
        .iter()
        .filter_map(|kw| find_ascii_case_insensitive_from(sql, kw, after_gb_start))
        .min()
        .unwrap_or(sql.len());

    sql[after_gb_start..end_pos]
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(parse_ident_token)
        .collect()
}

/// Parse the trailing `WITH (...)` options into the refresh policy and the
/// aggregate's retention period in milliseconds.
///
/// - `refresh_policy` / `refresh`: `on_flush`, `on_seal`, `manual`, or an
///   interval (`'5m'`), which means `Periodic`.
/// - `refresh_interval`: an interval, which means `Periodic`.
/// - `retention` / `retention_period`: an interval.
///
/// An unknown option, or a value that does not parse, is an error: a
/// misspelled option never falls back to a default silently.
pub(super) fn extract_with_options(
    _upper: &str,
    sql: &str,
) -> Result<(RefreshPolicy, u64), DdlError> {
    let mut refresh = RefreshPolicy::OnFlush;
    let mut retention_ms = 0u64;

    let Some(with_pos) = rfind_ascii_case_insensitive(sql, "WITH") else {
        return Ok((refresh, retention_ms));
    };
    let after_with = sql[with_pos + 4..].trim_start();
    let (Some(open), Some(close)) = (after_with.find('('), after_with.rfind(')')) else {
        return Ok((refresh, retention_ms));
    };
    if close <= open {
        return Ok((refresh, retention_ms));
    }

    let interval = |key: &str, val: &str| {
        nodedb_types::kv_parsing::parse_interval_to_ms(val).map_err(|_| {
            DdlError::new(
                "22023",
                format!("continuous aggregate option {key} = '{val}' is not an interval"),
            )
        })
    };
    let inner = &after_with[open + 1..close];
    for pair in inner.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let Some(eq) = pair.find('=') else {
            return Err(DdlError::new(
                "42601",
                format!("continuous aggregate option '{pair}' needs '= <value>'"),
            ));
        };
        let key = pair[..eq].trim().to_lowercase();
        let val = pair[eq + 1..].trim().trim_matches('\'').trim_matches('"');
        match key.as_str() {
            "refresh_policy" | "refresh" => {
                refresh = match val.to_lowercase().as_str() {
                    "on_flush" | "onflush" => RefreshPolicy::OnFlush,
                    "on_seal" | "onseal" => RefreshPolicy::OnSeal,
                    "manual" => RefreshPolicy::Manual,
                    other => RefreshPolicy::Periodic(interval(&key, other)?),
                };
            }
            "refresh_interval" => refresh = RefreshPolicy::Periodic(interval(&key, val)?),
            "retention" | "retention_period" => retention_ms = interval(&key, val)?,
            other => {
                return Err(DdlError::new(
                    "42601",
                    format!(
                        "unknown continuous aggregate option '{other}'; expected refresh_policy, \
                         refresh, refresh_interval, retention, or retention_period"
                    ),
                ));
            }
        }
    }

    Ok((refresh, retention_ms))
}

#[cfg(test)]
mod tests {
    use super::parse_create_sql;

    #[test]
    fn count_star_keeps_the_wildcard_as_its_source_column() {
        let def = parse_create_sql(
            "CREATE CONTINUOUS AGGREGATE m1 ON metrics BUCKET '1h' AGGREGATE count(*)",
        )
        .expect("count(*) is a valid aggregate");
        assert_eq!(def.aggregates[0].source_column, "*");
        // A wildcard argument derives the bare function name as the alias.
        assert_eq!(def.aggregates[0].output_column, "count");
    }

    #[test]
    fn quoted_aggregate_column_keeps_its_case() {
        let def = parse_create_sql(
            "CREATE CONTINUOUS AGGREGATE m1 ON \"MiXeD\" BUCKET '1h' AGGREGATE sum(\"VaLuE\")",
        )
        .expect("quoted names are valid");
        assert_eq!(def.source, "MiXeD");
        assert_eq!(def.aggregates[0].source_column, "VaLuE");
    }

    #[test]
    fn wildcard_is_rejected_outside_a_function_argument() {
        // `*` is legal only in the argument slot, never as an aggregate item,
        // a source, or a GROUP BY column.
        for sql in [
            "CREATE CONTINUOUS AGGREGATE m1 ON metrics BUCKET '1h' AGGREGATE *",
            "CREATE CONTINUOUS AGGREGATE m1 ON * BUCKET '1h' AGGREGATE count(*)",
            "CREATE CONTINUOUS AGGREGATE m1 ON metrics BUCKET '1h' AGGREGATE count(*) GROUP BY *",
        ] {
            assert!(parse_create_sql(sql).is_err(), "{sql}");
        }
    }
}
