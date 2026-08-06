// SPDX-License-Identifier: BUSL-1.1

//! GET `/obsv/api/v1/labels` and GET `/obsv/api/v1/label/:name/values`.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::admission::{admit, identity_database};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::LabelsParams;
use crate::control::server::http::routes::promql::helpers::{fetch_series_for_query, now_ms};

/// GET `/obsv/api/v1/labels` — list all label names.
pub async fn label_names(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    Query(params): Query<LabelsParams>,
) -> Response {
    let rate_limit_headers = match admit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
        "promql_labels",
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

    let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &all_series {
        for k in s.labels.keys() {
            names.insert(k.clone());
        }
    }

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, n);
        out.push('"');
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

/// GET `/obsv/api/v1/label/:name/values` — list values for a label.
pub async fn label_values(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<LabelsParams>,
) -> Response {
    let rate_limit_headers = match admit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
        "promql_label_values",
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

    let mut values: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &all_series {
        if let Some(v) = s.labels.get(&name) {
            values.insert(v.clone());
        }
    }

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, v);
        out.push('"');
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
