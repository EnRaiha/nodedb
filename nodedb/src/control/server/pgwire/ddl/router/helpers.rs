// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Execute `NDB_CHUNK_TEXT(text, chunk_size, overlap, strategy)` and return rows.
///
/// Parses named (`text => '...'`) or positional arguments from the SQL string,
/// delegates to `nodedb_query::chunk_text`, and returns a pgwire result set.
pub(super) fn execute_chunk_text(
    sql: &str,
) -> pgwire::error::PgWireResult<Vec<pgwire::api::results::Response>> {
    use futures::stream;
    use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};

    // Extract the parenthesized args from NDB_CHUNK_TEXT(...).
    let upper = sql.to_uppercase();
    let fn_pos = upper.find("NDB_CHUNK_TEXT(").ok_or_else(|| {
        super::super::super::types::sqlstate_error("42601", "expected NDB_CHUNK_TEXT(...)")
    })?;
    let paren_start = fn_pos
        + sql[fn_pos..].find('(').ok_or_else(|| {
            super::super::super::types::sqlstate_error("42601", "expected NDB_CHUNK_TEXT(...)")
        })?;
    let paren_end = sql.rfind(')').ok_or_else(|| {
        super::super::super::types::sqlstate_error("42601", "expected closing ) for NDB_CHUNK_TEXT")
    })?;

    let inner = &sql[paren_start + 1..paren_end];

    // Parse named or positional arguments.
    let mut text_val = String::new();
    let mut chunk_size = 0usize;
    let mut overlap = 0usize;
    let mut strategy_str = String::from("character");

    // Split on commas, but respect quoted strings.
    let args = split_args_respecting_quotes(inner);

    if args.is_empty() {
        return Err(super::super::super::types::sqlstate_error(
            "42601",
            "NDB_CHUNK_TEXT requires at least text and chunk_size arguments",
        ));
    }

    // Detect named vs positional by checking if first arg contains `=>`.
    let is_named = args[0].contains("=>");

    if is_named {
        for arg in &args {
            if let Some((key, val)) = arg.split_once("=>") {
                let key = key.trim().to_lowercase();
                let val = val.trim().trim_matches('\'').trim_matches('"');
                match key.as_str() {
                    "text" => text_val = val.to_string(),
                    "chunk_size" => {
                        chunk_size = val.parse().map_err(|_| {
                            super::super::super::types::sqlstate_error(
                                "22023",
                                &format!("invalid chunk_size: {val}"),
                            )
                        })?;
                    }
                    "overlap" => {
                        overlap = val.parse().map_err(|_| {
                            super::super::super::types::sqlstate_error(
                                "22023",
                                &format!("invalid overlap: {val}"),
                            )
                        })?;
                    }
                    "strategy" => strategy_str = val.to_string(),
                    other => {
                        return Err(super::super::super::types::sqlstate_error(
                            "42601",
                            &format!("unknown NDB_CHUNK_TEXT parameter: {other}"),
                        ));
                    }
                }
            }
        }
    } else {
        // Positional: NDB_CHUNK_TEXT('text', chunk_size, overlap, 'strategy')
        if args.len() < 2 {
            return Err(super::super::super::types::sqlstate_error(
                "42601",
                "NDB_CHUNK_TEXT requires at least (text, chunk_size)",
            ));
        }
        text_val = args[0]
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        chunk_size = args[1].trim().parse().map_err(|_| {
            super::super::super::types::sqlstate_error("22023", "invalid chunk_size")
        })?;
        if args.len() > 2 {
            overlap = args[2].trim().parse().map_err(|_| {
                super::super::super::types::sqlstate_error("22023", "invalid overlap")
            })?;
        }
        if args.len() > 3 {
            strategy_str = args[3]
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
        }
    }

    if text_val.is_empty() {
        return Err(super::super::super::types::sqlstate_error(
            "42601",
            "NDB_CHUNK_TEXT: text argument is required",
        ));
    }
    if chunk_size == 0 {
        return Err(super::super::super::types::sqlstate_error(
            "42601",
            "NDB_CHUNK_TEXT: chunk_size must be > 0",
        ));
    }

    let strategy = nodedb_query::ChunkStrategy::parse(&strategy_str).ok_or_else(|| {
        super::super::super::types::sqlstate_error(
            "22023",
            &format!(
                "unknown strategy '{strategy_str}'; supported: character, sentence, paragraph"
            ),
        )
    })?;

    let chunks = nodedb_query::chunk_text(&text_val, chunk_size, overlap, strategy)
        .map_err(|e| super::super::super::types::sqlstate_error("22023", &e.to_string()))?;

    let schema = std::sync::Arc::new(vec![
        super::super::super::types::text_field("index"),
        super::super::super::types::text_field("start"),
        super::super::super::types::text_field("end"),
        super::super::super::types::text_field("text"),
    ]);

    let rows: Vec<_> = chunks
        .iter()
        .map(|c| {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&c.index.to_string());
            let _ = encoder.encode_field(&c.start.to_string());
            let _ = encoder.encode_field(&c.end.to_string());
            let _ = encoder.encode_field(&c.text);
            Ok(encoder.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Split a comma-separated argument string, respecting single-quoted strings.
fn split_args_respecting_quotes(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for ch in s.chars() {
        match ch {
            '\'' if !in_quote => {
                in_quote = true;
                current.push(ch);
            }
            '\'' if in_quote => {
                in_quote = false;
                current.push(ch);
            }
            ',' if !in_quote => {
                args.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        args.push(current);
    }
    args
}

/// EXPLAIN TIERS ON <collection> — show AUTO_TIER routing plan.
pub(super) fn explain_tiers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    use super::super::super::types::{sqlstate_error, text_field};
    use futures::stream;
    use pgwire::api::results::{DataRowEncoder, QueryResponse};
    use std::sync::Arc;

    // EXPLAIN TIERS ON <collection> [RANGE <start_ms> <end_ms>]
    if parts.len() < 4 || !parts[2].eq_ignore_ascii_case("ON") {
        return Err(sqlstate_error(
            "42601",
            "syntax: EXPLAIN TIERS ON <collection> [RANGE <start_ms> <end_ms>]",
        ));
    }
    let collection = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    let policy = state
        .retention_policy_registry
        .get_for_collection(database_id.as_u64(), tenant_id, &collection)
        .ok_or_else(|| {
            sqlstate_error(
                "42704",
                &format!("no retention policy found for '{collection}'"),
            )
        })?;

    if !policy.auto_tier {
        return Err(sqlstate_error(
            "42809",
            &format!("AUTO_TIER is not enabled on '{collection}'"),
        ));
    }

    // Optional RANGE clause: EXPLAIN TIERS ON coll RANGE 1700000000 1710000000
    let time_range = if parts.len() >= 7 && parts[4].eq_ignore_ascii_case("RANGE") {
        let start = parts[5]
            .parse::<i64>()
            .map_err(|_| sqlstate_error("42601", &format!("invalid RANGE start: {}", parts[5])))?;
        let end = parts[6]
            .parse::<i64>()
            .map_err(|_| sqlstate_error("42601", &format!("invalid RANGE end: {}", parts[6])))?;
        (start, end)
    } else {
        (0i64, i64::MAX)
    };
    let explanation =
        crate::control::planner::auto_tier::explain_tier_selection(&policy, time_range);

    let schema = Arc::new(vec![text_field("plan")]);
    let mut rows = Vec::new();
    for line in explanation.lines() {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&line)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
