// SPDX-License-Identifier: BUSL-1.1

//! GET/POST `/obsv/api/v1/query_range` — range PromQL query.

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::admission::{admit, identity_database};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::RangeQueryParams;
use crate::control::server::http::routes::promql::helpers::{
    fetch_series_for_query, parse_step, prom_error, prom_success,
};

pub async fn range_query(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    Query(params): Query<RangeQueryParams>,
) -> Response {
    let rate_limit_headers = match admit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
        "promql_query_range",
    ) {
        Ok(headers) => headers,
        Err(error) => return error.into_response(),
    };

    let start_ms = (params.start * 1000.0) as i64;
    let end_ms = (params.end * 1000.0) as i64;
    let step_ms = parse_step(&params.step).unwrap_or(15_000);

    if step_ms <= 0 {
        return (
            rate_limit_headers,
            prom_error("bad_data", "step must be positive"),
        )
            .into_response();
    }
    if end_ms < start_ms {
        return (
            rate_limit_headers,
            prom_error("bad_data", "end must be >= start"),
        )
            .into_response();
    }

    let tokens = match promql::lexer::tokenize(&params.query) {
        Ok(t) => t,
        Err(e) => {
            return (rate_limit_headers, prom_error("bad_data", &e.to_string())).into_response();
        }
    };
    let expr = match promql::parse(&tokens) {
        Ok(e) => e,
        Err(e) => {
            return (rate_limit_headers, prom_error("bad_data", &e.to_string())).into_response();
        }
    };

    let series = fetch_series_for_query(
        &state,
        start_ms - promql::types::DEFAULT_LOOKBACK_MS,
        end_ms,
    )
    .await;

    let ctx = promql::EvalContext {
        series,
        timestamp_ms: start_ms,
        lookback_ms: promql::types::DEFAULT_LOOKBACK_MS,
    };

    match promql::evaluate_range(&ctx, &expr, start_ms, end_ms, step_ms) {
        Ok(value) => (rate_limit_headers, prom_success(value)).into_response(),
        Err(e) => (rate_limit_headers, prom_error("execution", &e.to_string())).into_response(),
    }
}
