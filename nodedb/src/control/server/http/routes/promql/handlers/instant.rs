// SPDX-License-Identifier: BUSL-1.1

//! GET/POST `/obsv/api/v1/query` — instant PromQL query.

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::admission::{admit, identity_database};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::InstantQueryParams;
use crate::control::server::http::routes::promql::helpers::{
    fetch_series_for_query, prom_error, prom_success,
};

pub async fn instant_query(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    Query(params): Query<InstantQueryParams>,
) -> Response {
    let rate_limit_headers = match admit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
        "promql_query",
    ) {
        Ok(headers) => headers,
        Err(error) => return error.into_response(),
    };

    let ts_ms = params.time.map(|t| (t * 1000.0) as i64).unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    });

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

    let series =
        fetch_series_for_query(&state, ts_ms - promql::types::DEFAULT_LOOKBACK_MS, ts_ms).await;

    let ctx = promql::EvalContext {
        series,
        timestamp_ms: ts_ms,
        lookback_ms: promql::types::DEFAULT_LOOKBACK_MS,
    };

    match promql::evaluate_instant(&ctx, &expr) {
        Ok(value) => (rate_limit_headers, prom_success(value)).into_response(),
        Err(e) => (rate_limit_headers, prom_error("execution", &e.to_string())).into_response(),
    }
}
