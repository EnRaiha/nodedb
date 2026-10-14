// SPDX-License-Identifier: BUSL-1.1

//! POST `/obsv/api/v1/annotations` — Grafana annotation query.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::control::promql::{self, PromqlError};
use crate::control::server::http::admission::admit_without_rate_limit;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;
use crate::control::server::http::routes::query::{DatabaseQueryParam, resolve_database_id};

use crate::control::server::http::routes::promql::helpers::{
    now_ms, prom_error_from, promql_error,
};
use crate::control::server::http::routes::promql::params::parse_time_ms;
use crate::control::server::http::routes::promql::source::{
    ReadScope, SampleBudget, load_series, selector_reads,
};

/// Annotations use a 60s step: event markers need no finer grain.
const ANNOTATION_STEP_MS: i64 = 60_000;

pub async fn annotations(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> Response {
    let database_id = match resolve_database_id(&headers, &DatabaseQueryParam::default(), &state) {
        Ok(id) => id,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = admit_without_rate_limit(&state, &identity.0, database_id, peer.as_str()) {
        return error.into_response();
    }

    let query = body
        .pointer("/annotation/query")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let now = now_ms();
    let from_ms = match range_bound(&body, "/range/from", 0) {
        Ok(ms) => ms,
        Err(e) => return promql_error(&e).into_response(),
    };
    let to_ms = match range_bound(&body, "/range/to", now) {
        Ok(ms) => ms,
        Err(e) => return promql_error(&e).into_response(),
    };

    if query.is_empty() {
        return empty_annotations();
    }

    let tokens = match promql::lexer::tokenize(query) {
        Ok(t) => t,
        Err(e) => {
            tracing::debug!(error = %e, query, "annotation query tokenize failed");
            return empty_annotations();
        }
    };
    let expr = match promql::parse(&tokens) {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!(error = %e, query, "annotation query parse failed");
            return empty_annotations();
        }
    };

    let tuning = &state.shared.tuning.promql;
    let lookback_ms = tuning.lookback_delta_ms;
    let scope = ReadScope::new(&state.shared, &identity.0, database_id);
    let mut budget = SampleBudget::new(tuning.max_samples);
    let reads = selector_reads(&expr, from_ms, to_ms, lookback_ms);
    let series = match load_series(&scope, &reads, &mut budget, now).await {
        Ok(series) => series,
        Err(e) => return prom_error_from(&e).into_response(),
    };
    let ctx = promql::EvalContext {
        series: series.into(),
        timestamp_ms: to_ms,
        lookback_ms,
    };
    let matrix = match promql::evaluate_range(&ctx, &expr, from_ms, to_ms, ANNOTATION_STEP_MS) {
        Ok(promql::Value::Matrix(matrix)) => matrix,
        Ok(promql::Value::Scalar(..) | promql::Value::Vector(_)) => Vec::new(),
        Err(e) => return promql_error(&e).into_response(),
    };

    let mut result_annotations: Vec<serde_json::Value> = Vec::new();
    for rs in &matrix {
        let title = rs
            .labels
            .get("__name__")
            .cloned()
            .unwrap_or_else(|| "annotation".into());
        let tags: Vec<String> = rs
            .labels
            .iter()
            .filter(|(k, _)| k.as_str() != "__name__")
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        for sample in &rs.samples {
            if sample.value != 0.0 && !sample.value.is_nan() {
                result_annotations.push(serde_json::json!({
                    "time": sample.timestamp_ms,
                    "title": title,
                    "text": format!("value={}", sample.value),
                    "tags": tags,
                }));
            }
        }
    }

    match sonic_rs::to_string(&result_annotations) {
        Ok(body) => (StatusCode::OK, [("content-type", "application/json")], body).into_response(),
        Err(e) => crate::control::server::http::routes::promql::helpers::prom_error(
            "internal",
            &format!("annotation response encode: {e}"),
        )
        .into_response(),
    }
}

/// A range bound of the annotation request: epoch milliseconds, Unix
/// seconds or RFC 3339. `default` when the request omits it.
fn range_bound(body: &serde_json::Value, pointer: &str, default: i64) -> Result<i64, PromqlError> {
    match body.pointer(pointer) {
        None | Some(serde_json::Value::Null) => Ok(default),
        Some(serde_json::Value::Number(n)) => n.as_i64().ok_or_else(|| bad_bound(pointer)),
        Some(serde_json::Value::String(s)) => match s.parse::<i64>() {
            Ok(ms) => Ok(ms),
            Err(_) => parse_time_ms(s).ok_or_else(|| bad_bound(pointer)),
        },
        Some(_) => Err(bad_bound(pointer)),
    }
}

fn bad_bound(pointer: &str) -> PromqlError {
    PromqlError::BadParameter {
        param: pointer.trim_start_matches('/').replace('/', "."),
        detail: "expected epoch milliseconds or an RFC 3339 timestamp".to_owned(),
    }
}

/// Grafana treats an unparseable or empty annotation query as "no
/// annotations", not an error, so every such branch returns the same empty
/// array body.
fn empty_annotations() -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        "[]".to_string(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_bounds_accept_millis_and_rfc3339() {
        let body = serde_json::json!({
            "range": { "from": "1000", "to": "1970-01-01T00:00:02Z" }
        });
        assert_eq!(range_bound(&body, "/range/from", 0).unwrap(), 1000);
        assert_eq!(range_bound(&body, "/range/to", 0).unwrap(), 2000);
        assert_eq!(range_bound(&body, "/range/missing", 7).unwrap(), 7);
        let bad = serde_json::json!({ "range": { "from": "yesterday" } });
        assert!(range_bound(&bad, "/range/from", 0).is_err());
    }
}
