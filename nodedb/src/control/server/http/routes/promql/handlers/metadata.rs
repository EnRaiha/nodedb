// SPDX-License-Identifier: BUSL-1.1

//! GET `/obsv/api/v1/metadata` — metric metadata for Grafana metric browser.

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::helpers::{begin_request, prom_error_from};
use crate::control::server::http::routes::promql::source::{
    NODE_METRICS, ReadScope, node_metrics_visible, readable_metric_collections,
};

pub async fn metadata(
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
        "promql_metadata",
    ) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let rl = request.rate_limit_headers;
    let scope = ReadScope::new(&state.shared, &identity.0, request.database_id);

    // Stored metrics carry no type or help text: remote write and OTLP do
    // not record them with the samples.
    let mut entries: Vec<(String, &str, &str)> = match readable_metric_collections(&scope) {
        Ok(metrics) => metrics
            .into_iter()
            .map(|m| (m.name, "unknown", ""))
            .collect(),
        Err(e) => return (rl, prom_error_from(&e)).into_response(),
    };
    if node_metrics_visible(&identity.0) {
        entries.extend(
            NODE_METRICS
                .iter()
                .map(|(name, metric_type, help)| ((*name).to_owned(), *metric_type, *help)),
        );
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries.dedup_by(|a, b| a.0 == b.0);

    let mut out = String::from(r#"{"status":"success","data":{"#);
    for (i, (name, metric_type, help)) in entries.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, name);
        out.push_str(r#"":[{"type":""#);
        out.push_str(metric_type);
        out.push_str(r#"","help":""#);
        promql::types::json_escape(&mut out, help);
        out.push_str(r#"","unit":""}]"#);
    }
    out.push_str("}}");
    (
        StatusCode::OK,
        rl,
        [("content-type", "application/json")],
        out,
    )
        .into_response()
}
