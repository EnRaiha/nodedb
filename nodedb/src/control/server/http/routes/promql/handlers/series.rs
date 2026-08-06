// SPDX-License-Identifier: BUSL-1.1

//! GET `/obsv/api/v1/series` — find series by label matchers.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::admission::{admit, identity_database};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::SeriesParams;
use crate::control::server::http::routes::promql::helpers::{
    fetch_series_for_query, now_ms, parse_series_matcher,
};

pub async fn series_query(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    Query(params): Query<SeriesParams>,
) -> Response {
    let rate_limit_headers = match admit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
        "promql_series",
    ) {
        Ok(headers) => headers,
        Err(error) => return error.into_response(),
    };

    let end_ms = params.end.map(|t| (t * 1000.0) as i64).unwrap_or(now_ms());
    let start_ms = params
        .start
        .map(|t| (t * 1000.0) as i64)
        .unwrap_or(end_ms - promql::types::DEFAULT_LOOKBACK_MS);

    let all_series = fetch_series_for_query(&state, start_ms, end_ms).await;

    let filtered: Vec<&promql::Series> = if params.matchers.is_empty() {
        all_series.iter().collect()
    } else {
        all_series
            .iter()
            .filter(|s| {
                params
                    .matchers
                    .iter()
                    .any(|m| match parse_series_matcher(m) {
                        Some(matchers) => promql::label::matches_all(&matchers, &s.labels),
                        None => false,
                    })
            })
            .collect()
    };

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, s) in filtered.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        promql::types::write_labels_json(&mut out, &s.labels);
    }
    out.push_str("]}");

    (
        StatusCode::OK,
        rate_limit_headers,
        [("content-type", "application/json")],
        out,
    )
        .into_response()
}
