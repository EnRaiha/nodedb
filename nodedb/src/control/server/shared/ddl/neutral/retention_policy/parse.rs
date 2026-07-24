// SPDX-License-Identifier: BUSL-1.1

mod lex;

use self::lex::{
    consume_keyword, find_matching_paren, parse_identifier_token, parse_single_quoted_string,
    split_top_level_commas,
};
use super::super::super::result::DdlError;
use crate::engine::timeseries::continuous_agg::{AggFunction, AggregateExpr};
use crate::engine::timeseries::retention_policy::types::{
    ArchiveTarget, RetentionPolicyDef, TierDef,
};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

pub(super) struct ParsedRetentionPolicy {
    pub name: String,
    pub collection: String,
    pub tiers: Vec<TierDef>,
    pub tier_count: usize,
    pub eval_interval_ms: u64,
}

pub(super) fn parse_create_retention_policy(sql: &str) -> Result<ParsedRetentionPolicy, DdlError> {
    let trimmed = sql.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    const PREFIX: &str = "CREATE RETENTION POLICY";
    let prefix = trimmed
        .get(..PREFIX.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(PREFIX))
        .ok_or_else(|| err("42601", "expected CREATE RETENTION POLICY".to_string()))?;
    if prefix.len() == trimmed.len()
        || !trimmed[PREFIX.len()..]
            .chars()
            .next()
            .is_some_and(char::is_whitespace)
    {
        return Err(err("42601", "expected policy name".to_string()));
    }

    let (name, after_name) = parse_identifier_token(trimmed[PREFIX.len()..].trim_start())?;
    if !after_name.chars().next().is_some_and(char::is_whitespace) {
        return Err(err("42601", "expected ON <collection>".to_string()));
    }
    let after_on = consume_keyword(after_name.trim_start(), "ON")?;
    let (collection, after_collection) = parse_identifier_token(after_on)?;
    let after_collection = after_collection.trim_start();
    if !after_collection.starts_with('(') {
        return Err(err(
            "42601",
            "expected '(' after collection name".to_string(),
        ));
    }

    // Find the tier body between balanced outer parentheses.
    let body_start = trimmed.len() - after_collection.len();
    let body_end = find_matching_paren(trimmed, body_start)?
        .ok_or_else(|| err("42601", "missing closing ')'".to_string()))?;
    if body_end <= body_start {
        return Err(err("42601", "empty tier definition body".to_string()));
    }
    let body = &trimmed[body_start + 1..body_end];

    // Parse tiers from body.
    let tiers = parse_tiers(body)?;
    if tiers.is_empty() {
        return Err(err(
            "42601",
            "at least one tier (RAW) is required".to_string(),
        ));
    }
    if !tiers[0].is_raw() {
        return Err(err("42601", "first tier must be RAW".to_string()));
    }

    let tier_count = tiers.len();

    // Parse optional WITH clause after the closing ')'.
    let eval_interval_ms = parse_with_clause(trimmed, body_end)?;

    Ok(ParsedRetentionPolicy {
        name,
        collection,
        tiers,
        tier_count,
        eval_interval_ms,
    })
}

fn parse_tiers(body: &str) -> Result<Vec<TierDef>, DdlError> {
    let mut tiers = Vec::new();
    let mut archive_seen = false;

    for clause in split_top_level_commas(body)? {
        let clause = clause.trim();
        if clause.is_empty() {
            return Err(err("42601", "empty tier clause".to_string()));
        }
        if archive_seen {
            return Err(err(
                "42601",
                "ARCHIVE TO must be the final tier clause".to_string(),
            ));
        }

        if let Ok(after_raw) = consume_keyword(clause, "RAW") {
            if !tiers.is_empty() {
                return Err(err(
                    "42601",
                    "RAW must appear exactly once as the first tier".to_string(),
                ));
            }
            let retain_ms = parse_retain_clause(after_raw)?;
            tiers.push(TierDef {
                tier_index: 0,
                resolution_ms: 0,
                aggregates: Vec::new(),
                retain_ms,
                archive: None,
            });
            continue;
        }

        if let Ok(after_downsample) = consume_keyword(clause, "DOWNSAMPLE") {
            if tiers.is_empty() {
                return Err(err(
                    "42601",
                    "DOWNSAMPLE must follow the RAW tier".to_string(),
                ));
            }
            let after_to = consume_keyword(after_downsample, "TO")?;
            let (interval, after_interval) = parse_single_quoted_string(after_to)?;
            let resolution_ms =
                nodedb_types::kv_parsing::parse_interval_to_ms(&interval).map_err(|error| {
                    err(
                        "42601",
                        format!("invalid downsample interval '{interval}': {error}"),
                    )
                })?;
            if resolution_ms == 0 {
                return Err(err(
                    "42601",
                    "DOWNSAMPLE resolution must be greater than zero".to_string(),
                ));
            }
            let after_aggregate = consume_keyword(after_interval.trim_start(), "AGGREGATE")?;
            let (aggregates, after_aggregates) = parse_aggregate_list(after_aggregate)?;
            let retain_ms = parse_retain_clause(after_aggregates)?;
            tiers.push(TierDef {
                tier_index: tiers.len() as u32,
                resolution_ms,
                aggregates,
                retain_ms,
                archive: None,
            });
            continue;
        }

        if let Ok(after_archive) = consume_keyword(clause, "ARCHIVE") {
            let after_to = consume_keyword(after_archive, "TO")?;
            let (url, trailing) = parse_single_quoted_string(after_to)?;
            if !trailing.trim().is_empty() {
                return Err(err(
                    "42601",
                    "unexpected tokens after ARCHIVE URL".to_string(),
                ));
            }
            let last = tiers.last_mut().ok_or_else(|| {
                err(
                    "42601",
                    "ARCHIVE TO must follow at least one tier".to_string(),
                )
            })?;
            if last.archive.is_some() {
                return Err(err("42601", "duplicate ARCHIVE TO clause".to_string()));
            }
            last.archive = Some(ArchiveTarget::S3 { url });
            archive_seen = true;
            continue;
        }

        let preview: String = clause.chars().take(40).collect();
        return Err(err("42601", format!("unexpected tier clause: {preview}")));
    }

    Ok(tiers)
}

fn parse_retain_clause(input: &str) -> Result<u64, DdlError> {
    let after_retain = consume_keyword(input.trim_start(), "RETAIN")?;
    let (value, trailing) = parse_single_quoted_string(after_retain)?;
    if !trailing.trim().is_empty() {
        return Err(err(
            "42601",
            "unexpected tokens after RETAIN duration".to_string(),
        ));
    }
    if value.eq_ignore_ascii_case("forever") {
        return Ok(0);
    }
    nodedb_types::kv_parsing::parse_interval_to_ms(&value).map_err(|error| {
        err(
            "42601",
            format!("invalid retain duration '{value}': {error}"),
        )
    })
}

fn parse_aggregate_list(input: &str) -> Result<(Vec<AggregateExpr>, &str), DdlError> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(err("42601", "expected '(' after AGGREGATE".to_string()));
    }
    let close = find_matching_paren(input, 0)?.ok_or_else(|| {
        err(
            "42601",
            "missing ')' after AGGREGATE expressions".to_string(),
        )
    })?;
    let expressions = &input[1..close];
    if expressions.trim().is_empty() {
        return Err(err("42601", "empty AGGREGATE expression list".to_string()));
    }

    let mut aggregates = Vec::new();
    for expression in split_top_level_commas(expressions)? {
        let expression = expression.trim();
        if expression.is_empty() {
            return Err(err("42601", "empty AGGREGATE expression".to_string()));
        }
        aggregates.push(parse_agg_expr(expression)?);
    }
    Ok((aggregates, &input[close + 1..]))
}

pub(super) fn parse_agg_expr(s: &str) -> Result<AggregateExpr, DdlError> {
    let expression = s.trim();
    let open = expression
        .find('(')
        .ok_or_else(|| err("42601", format!("expected func(col): {s}")))?;
    let close = find_matching_paren(expression, open)?
        .ok_or_else(|| err("42601", format!("missing ')': {s}")))?;

    let func_name = expression[..open].trim().to_lowercase();
    if func_name.is_empty()
        || !func_name
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(err("42601", format!("invalid aggregate function: {s}")));
    }

    let argument = expression[open + 1..close].trim();
    let col_name = if argument == "*" {
        "*".to_string()
    } else {
        let (column, trailing) = parse_identifier_token(argument)?;
        if !trailing.trim().is_empty() {
            return Err(err(
                "42601",
                format!("invalid aggregate column: {argument}"),
            ));
        }
        column
    };

    let trailing = expression[close + 1..].trim();
    let alias = if trailing.is_empty() {
        None
    } else {
        let alias_input = consume_keyword(trailing, "AS")?;
        let (alias, rest) = parse_identifier_token(alias_input)?;
        if !rest.trim().is_empty() {
            return Err(err(
                "42601",
                "unexpected tokens after aggregate alias".to_string(),
            ));
        }
        Some(alias)
    };

    let function = match func_name.as_str() {
        "sum" => AggFunction::Sum,
        "count" => AggFunction::Count,
        "min" => AggFunction::Min,
        "max" => AggFunction::Max,
        "avg" => AggFunction::Avg,
        "first" => AggFunction::First,
        "last" => AggFunction::Last,
        "count_distinct" => AggFunction::CountDistinct,
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

fn parse_with_clause(sql: &str, body_end: usize) -> Result<u64, DdlError> {
    let after_body = sql[body_end + 1..].trim();
    if after_body.is_empty() {
        return Ok(RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS);
    }
    let after_with = consume_keyword(after_body, "WITH")?;
    let inner = after_with
        .trim_start()
        .strip_prefix('(')
        .ok_or_else(|| err("42601", "expected '(' after WITH".to_string()))?;
    let after_key = consume_keyword(inner.trim_start(), "EVAL_INTERVAL")?;
    let after_equals = after_key
        .trim_start()
        .strip_prefix('=')
        .ok_or_else(|| err("42601", "expected '=' after EVAL_INTERVAL".to_string()))?;
    let (interval, after_interval) = parse_single_quoted_string(after_equals.trim_start())?;
    let trailing = after_interval.trim_start();
    let trailing = trailing
        .strip_prefix(')')
        .ok_or_else(|| err("42601", "expected ')' after EVAL_INTERVAL".to_string()))?
        .trim();
    if !trailing.is_empty() {
        return Err(err(
            "42601",
            "unexpected trailing tokens in WITH clause".to_string(),
        ));
    }
    nodedb_types::kv_parsing::parse_interval_to_ms(&interval).map_err(|error| {
        err(
            "42601",
            format!("invalid EVAL_INTERVAL '{interval}': {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::retention_policy::types::{ArchiveTarget, RetentionPolicyDef};

    #[test]
    fn parse_basic_policy() {
        let sql = "CREATE RETENTION POLICY sensor_policy ON sensor_data (\
                    RAW RETAIN '7 days', \
                    DOWNSAMPLE TO '1 minute' AGGREGATE (AVG(value), MIN(value), MAX(value), COUNT(*)) RETAIN '90 days', \
                    DOWNSAMPLE TO '1 hour' AGGREGATE (AVG(value), MIN(value), MAX(value), COUNT(*)) RETAIN '2 years', \
                    ARCHIVE TO 's3://bucket/sensor-data/'\
                    )";
        let parsed = parse_create_retention_policy(sql).unwrap();
        assert_eq!(parsed.name, "sensor_policy");
        assert_eq!(parsed.collection, "sensor_data");
        assert_eq!(parsed.tiers.len(), 3);

        assert!(parsed.tiers[0].is_raw());
        assert_eq!(parsed.tiers[0].retain_ms, 604_800_000);

        assert_eq!(parsed.tiers[1].resolution_ms, 60_000);
        assert_eq!(parsed.tiers[1].aggregates.len(), 4);
        assert_eq!(parsed.tiers[1].retain_ms, 7_776_000_000);

        assert_eq!(parsed.tiers[2].resolution_ms, 3_600_000);
        assert_eq!(parsed.tiers[2].aggregates.len(), 4);
        assert!(matches!(
            &parsed.tiers[2].archive,
            Some(ArchiveTarget::S3 { url }) if url == "s3://bucket/sensor-data/"
        ));

        assert_eq!(
            parsed.eval_interval_ms,
            RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS
        );
    }

    #[test]
    fn collection_after_unicode_policy_name_preserves_original_offsets() {
        let parsed = parse_create_retention_policy(
            "CREATE RETENTION POLICY rpﬀﬀ ON metrics (RAW RETAIN '30 days')",
        )
        .expect("retention policy should parse");
        assert_eq!(parsed.name, "rpﬀﬀ");
        assert_eq!(parsed.collection, "metrics");
    }

    #[test]
    fn parse_with_eval_interval() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics (\
                    RAW RETAIN '30 days'\
                    ) WITH (EVAL_INTERVAL = '30m')";
        let parsed = parse_create_retention_policy(sql).unwrap();
        assert_eq!(parsed.eval_interval_ms, 1_800_000);
    }

    #[test]
    fn quoted_identifiers_round_trip_and_decode_doubled_quotes() {
        let parsed = parse_create_retention_policy(
            "CREATE RETENTION POLICY \"Policy Name\" ON \"Metrics \"\"Primary\"\"\" (RAW RETAIN '7d')",
        )
        .expect("quoted identifiers parse");
        assert_eq!(parsed.name, "Policy Name");
        assert_eq!(parsed.collection, "Metrics \"Primary\"");
    }

    #[test]
    fn malformed_with_clause_never_falls_back_to_default() {
        for sql in [
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d') WITH",
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d') WITH (OTHER = '1h')",
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d') WITH (EVAL_INTERVAL = 1h)",
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d') WITH (EVAL_INTERVAL = 'bogus')",
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d') WITH (EVAL_INTERVAL = '1h', OTHER = '2h')",
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d') WITH (EVAL_INTERVAL = '1h', EVAL_INTERVAL = '2h')",
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d') WITH (EVAL_INTERVAL = '1h') trailing",
        ] {
            assert!(parse_create_retention_policy(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn parse_forever_retain() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics (\
                    RAW RETAIN 'forever'\
                    )";
        let parsed = parse_create_retention_policy(sql).unwrap();
        assert_eq!(parsed.tiers[0].retain_ms, 0);
    }

    #[test]
    fn parse_errors_no_raw() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics (\
                    DOWNSAMPLE TO '1h' AGGREGATE (AVG(v)) RETAIN '30d'\
                    )";
        assert!(parse_create_retention_policy(sql).is_err());
    }

    #[test]
    fn parse_errors_empty_body() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics ()";
        assert!(parse_create_retention_policy(sql).is_err());
    }

    #[test]
    fn parse_agg_with_alias() {
        let expr = parse_agg_expr("AVG(temperature) AS avg_temp").unwrap();
        assert!(matches!(expr.function, AggFunction::Avg));
        assert_eq!(expr.source_column, "temperature");
        assert_eq!(expr.output_column, "avg_temp");
    }

    #[test]
    fn parse_agg_auto_alias() {
        let expr = parse_agg_expr("COUNT(*)").unwrap();
        assert!(matches!(expr.function, AggFunction::Count));
        assert_eq!(expr.output_column, "count");
    }

    #[test]
    fn split_commas_respects_parens() {
        let input = "RAW RETAIN '7d', DOWNSAMPLE TO '1m' AGGREGATE (AVG(v), MAX(v)) RETAIN '90d'";
        let parts = split_top_level_commas(input).expect("top-level commas split");
        assert_eq!(parts.len(), 2);
        assert!(parts[0].trim().starts_with("RAW"));
        assert!(parts[1].trim().starts_with("DOWNSAMPLE"));
    }

    #[test]
    fn aggregate_list_does_not_split_commas_inside_quoted_identifiers() {
        let tiers = parse_tiers(
            "RAW RETAIN '7d', DOWNSAMPLE TO '1m' AGGREGATE (AVG(\"value,secondary\"), MAX(value)) RETAIN '1d'",
        )
        .expect("aggregate list parses");
        let aggregates = &tiers[1].aggregates;
        assert_eq!(aggregates.len(), 2);
        assert_eq!(aggregates[0].source_column, "value,secondary");
        assert_eq!(aggregates[1].source_column, "value");

        let expression = parse_agg_expr("AVG(\"value AS secondary\") AS \"Average Value\"")
            .expect("quoted column and alias parse");
        assert_eq!(expression.source_column, "value AS secondary");
        assert_eq!(expression.output_column, "Average Value");
    }

    #[test]
    fn archive_literal_preserves_parenthesis_comma_and_doubled_apostrophe() {
        let parsed = parse_create_retention_policy(
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d', ARCHIVE TO 's3://bucket/a),part/o''reilly')",
        )
        .expect("quoted archive URL parses");
        assert!(matches!(
            &parsed.tiers[0].archive,
            Some(ArchiveTarget::S3 { url }) if url == "s3://bucket/a),part/o'reilly"
        ));
    }

    #[test]
    fn malformed_quoted_tier_text_is_rejected() {
        for sql in [
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d)",
            "CREATE RETENTION POLICY p ON metrics (RAW RETAIN '7d', ARCHIVE TO 's3://bucket/o'reilly')",
        ] {
            assert!(parse_create_retention_policy(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn tiers_enforce_semantic_sequence() {
        for body in [
            "DOWNSAMPLE TO '1m' AGGREGATE (AVG(value)) RETAIN '1d'",
            "RAW RETAIN '7d', RAW RETAIN '1d'",
            "RAW RETAIN '7d', DOWNSAMPLE TO '0ms' AGGREGATE (AVG(value)) RETAIN '1d'",
            "RAW RETAIN '7d', ARCHIVE TO 's3://bucket/one', ARCHIVE TO 's3://bucket/two'",
            "RAW RETAIN '7d', ARCHIVE TO 's3://bucket/path', DOWNSAMPLE TO '1m' AGGREGATE (AVG(value)) RETAIN '1d'",
        ] {
            assert!(parse_tiers(body).is_err(), "{body}");
        }
    }

    #[test]
    fn tiers_reject_keyword_prefixes_and_ignored_junk() {
        for body in [
            "RAWX RETAIN '7d'",
            "DOWNSAMPLEX TO '1m' AGGREGATE (AVG(value)) RETAIN '1d'",
            "DOWNSAMPLE TO '1m' AGGREGATE (AVG(value)) unexpected RETAIN '1d'",
            "RAW RETAIN '7d' trailing",
            "ARCHIVE TO 's3://bucket/path' trailing",
        ] {
            assert!(parse_tiers(body).is_err(), "{body}");
        }
    }
}
