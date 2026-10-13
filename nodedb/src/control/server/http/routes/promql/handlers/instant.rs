// SPDX-License-Identifier: BUSL-1.1

//! GET/POST `/obsv/api/v1/query` — instant PromQL query.

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::helpers::{
    begin_request, now_ms, prom_error_from, prom_success, promql_error,
};
use crate::control::server::http::routes::promql::source::{
    ReadScope, SampleBudget, load_series, selector_reads,
};

pub async fn instant_query(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let request = match begin_request(
        &state,
        &identity.0,
        peer.as_str(),
        &headers,
        raw_query.as_deref(),
        &body,
        "promql_query",
    ) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let rl = request.rate_limit_headers;
    let now = now_ms();

    let parsed = request.params.required("query").and_then(|query| {
        let tokens = promql::lexer::tokenize(query)?;
        promql::parse(&tokens)
    });
    let expr = match parsed {
        Ok(expr) => expr,
        Err(e) => return (rl, promql_error(&e)).into_response(),
    };
    let ts_ms = match request.params.time_ms("time") {
        Ok(ts) => ts.unwrap_or(now),
        Err(e) => return (rl, promql_error(&e)).into_response(),
    };

    let tuning = &state.shared.tuning.promql;
    let lookback_ms = tuning.lookback_delta_ms;
    let scope = ReadScope::new(&state.shared, &identity.0, request.database_id);
    let mut budget = SampleBudget::new(tuning.max_samples);
    let reads = selector_reads(&expr, ts_ms, ts_ms, lookback_ms);
    let series = match load_series(&scope, &reads, &mut budget, now).await {
        Ok(series) => series,
        Err(e) => return (rl, prom_error_from(&e)).into_response(),
    };

    let ctx = promql::EvalContext {
        series: series.into(),
        timestamp_ms: ts_ms,
        lookback_ms,
    };
    match promql::evaluate_instant(&ctx, &expr) {
        Ok(value) => (rl, prom_success(value)).into_response(),
        Err(e) => (rl, promql_error(&e)).into_response(),
    }
}
