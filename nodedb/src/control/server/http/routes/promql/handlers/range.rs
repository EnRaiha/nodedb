// SPDX-License-Identifier: BUSL-1.1

//! GET/POST `/obsv/api/v1/query_range` — range PromQL query.

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};

use crate::control::promql::{self, PromqlError};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::helpers::{
    begin_request, now_ms, parse_step, prom_error_from, prom_success, promql_error,
};
use crate::control::server::http::routes::promql::params::PromParams;
use crate::control::server::http::routes::promql::source::{
    ReadScope, SampleBudget, load_series, selector_reads,
};

/// The parsed shape of a range query.
#[derive(Debug)]
struct RangeQuery {
    expr: promql::ast::Expr,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
}

pub async fn range_query(
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
        "promql_query_range",
    ) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let rl = request.rate_limit_headers;
    let tuning = &state.shared.tuning.promql;

    let query = match parse_range_query(&request.params, tuning.max_points_per_series) {
        Ok(query) => query,
        Err(e) => return (rl, promql_error(&e)).into_response(),
    };

    let lookback_ms = tuning.lookback_delta_ms;
    let scope = ReadScope::new(&state.shared, &identity.0, request.database_id);
    let mut budget = SampleBudget::new(tuning.max_samples);
    let reads = selector_reads(&query.expr, query.start_ms, query.end_ms, lookback_ms);
    let series = match load_series(&scope, &reads, &mut budget, now_ms()).await {
        Ok(series) => series,
        Err(e) => return (rl, prom_error_from(&e)).into_response(),
    };

    let ctx = promql::EvalContext {
        series: series.into(),
        timestamp_ms: query.start_ms,
        lookback_ms,
    };
    match promql::evaluate_range(
        &ctx,
        &query.expr,
        query.start_ms,
        query.end_ms,
        query.step_ms,
    ) {
        Ok(value) => (rl, prom_success(value)).into_response(),
        Err(e) => (rl, promql_error(&e)).into_response(),
    }
}

fn parse_range_query(
    params: &PromParams,
    max_points_per_series: usize,
) -> Result<RangeQuery, PromqlError> {
    let tokens = promql::lexer::tokenize(params.required("query")?)?;
    let expr = promql::parse(&tokens)?;
    let start_ms = params.time_ms("start")?.ok_or_else(|| missing("start"))?;
    let end_ms = params.time_ms("end")?.ok_or_else(|| missing("end"))?;
    let step_ms = parse_step(params.required("step")?)?;
    if end_ms < start_ms {
        return Err(PromqlError::BadParameter {
            param: "end".to_owned(),
            detail: "end timestamp must not be before start time".to_owned(),
        });
    }
    let points = (end_ms - start_ms) / step_ms + 1;
    if !usize::try_from(points).is_ok_and(|p| p <= max_points_per_series) {
        return Err(PromqlError::TooManyPoints {
            limit: max_points_per_series,
        });
    }
    Ok(RangeQuery {
        expr,
        start_ms,
        end_ms,
        step_ms,
    })
}

fn missing(param: &str) -> PromqlError {
    PromqlError::BadParameter {
        param: param.to_owned(),
        detail: "the parameter is required".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(query: &str) -> PromParams {
        PromParams::from_request(Some(query), &HeaderMap::new(), &[]).unwrap()
    }

    #[test]
    fn valid_range_parses() {
        let q = parse_range_query(&params("query=up&start=0&end=30&step=15"), 100).unwrap();
        assert_eq!((q.start_ms, q.end_ms, q.step_ms), (0, 30_000, 15_000));
    }

    #[test]
    fn too_many_points_is_refused() {
        let err = parse_range_query(&params("query=up&start=0&end=100&step=1"), 10).unwrap_err();
        assert!(matches!(err, PromqlError::TooManyPoints { limit: 10 }));
    }

    #[test]
    fn bad_step_and_reversed_range_are_refused() {
        assert!(parse_range_query(&params("query=up&start=0&end=30&step=x"), 100).is_err());
        assert!(parse_range_query(&params("query=up&start=30&end=0&step=1"), 100).is_err());
        assert!(parse_range_query(&params("query=up&end=0&step=1"), 100).is_err());
    }
}
