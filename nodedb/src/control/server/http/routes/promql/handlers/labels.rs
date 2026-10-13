// SPDX-License-Identifier: BUSL-1.1

//! GET/POST `/obsv/api/v1/labels` and GET `/obsv/api/v1/label/:name/values`.

use std::collections::BTreeSet;

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::helpers::{
    PromRequest, begin_request, now_ms, parse_series_matcher, prom_error_from,
};
use crate::control::server::http::routes::promql::source::{
    NODE_METRICS, ReadScope, SampleBudget, SelectorRead, load_series, node_metrics_visible,
    readable_metric_collections,
};

use super::series::time_window;

/// GET `/obsv/api/v1/labels` — list all label names.
pub async fn label_names(
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
        "promql_labels",
    ) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let scope = ReadScope::new(&state.shared, &identity.0, request.database_id);
    match series_in_window(&state, &scope, &request).await {
        Ok(series) => {
            let names: BTreeSet<String> = series
                .iter()
                .flat_map(|s| s.labels.keys().cloned())
                .collect();
            string_list(request.rate_limit_headers, &names)
        }
        Err(e) => (request.rate_limit_headers, prom_error_from(&e)).into_response(),
    }
}

/// GET `/obsv/api/v1/label/:name/values` — list values for a label.
pub async fn label_values(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    Path(name): Path<String>,
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
        "promql_label_values",
    ) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let scope = ReadScope::new(&state.shared, &identity.0, request.database_id);

    // Metric names come from the catalog: Grafana's metric browser asks for
    // them constantly, and listing them needs no scan.
    if name == "__name__" && request.params.all("match[]").is_empty() {
        return match readable_metric_collections(&scope) {
            Ok(metrics) => {
                let mut names: BTreeSet<String> = metrics.into_iter().map(|m| m.name).collect();
                if node_metrics_visible(&identity.0) {
                    names.extend(NODE_METRICS.iter().map(|(n, _, _)| (*n).to_owned()));
                }
                string_list(request.rate_limit_headers, &names)
            }
            Err(e) => (request.rate_limit_headers, prom_error_from(&e)).into_response(),
        };
    }

    match series_in_window(&state, &scope, &request).await {
        Ok(series) => {
            let values: BTreeSet<String> = series
                .iter()
                .filter_map(|s| s.labels.get(&name).cloned())
                .collect();
            string_list(request.rate_limit_headers, &values)
        }
        Err(e) => (request.rate_limit_headers, prom_error_from(&e)).into_response(),
    }
}

/// The series of the request's window: those its `match[]` values select,
/// or every readable metric when it has none.
async fn series_in_window(
    state: &AppState,
    scope: &ReadScope<'_>,
    request: &PromRequest,
) -> crate::Result<Vec<promql::Series>> {
    let tuning = &state.shared.tuning.promql;
    let now = now_ms();
    let (start_ms, end_ms) = time_window(&request.params, now, tuning.lookback_delta_ms)?;
    let matchers = request.params.all("match[]");
    let reads: Vec<SelectorRead> = if matchers.is_empty() {
        let mut reads: Vec<SelectorRead> = readable_metric_collections(scope)?
            .into_iter()
            .map(|metric| SelectorRead {
                name: Some(metric.name),
                matchers: Vec::new(),
                min_ts: start_ms,
                max_ts: end_ms,
            })
            .collect();
        if node_metrics_visible(scope.identity) {
            reads.extend(NODE_METRICS.iter().map(|(n, _, _)| SelectorRead {
                name: Some((*n).to_owned()),
                matchers: Vec::new(),
                min_ts: start_ms,
                max_ts: end_ms,
            }));
        }
        reads
    } else {
        matchers
            .into_iter()
            .map(|m| parse_series_matcher(m, start_ms, end_ms))
            .collect::<Result<_, _>>()?
    };
    let mut budget = SampleBudget::new(tuning.max_samples);
    load_series(scope, &reads, &mut budget, now).await
}

fn string_list(rate_limit_headers: HeaderMap, items: &BTreeSet<String>) -> Response {
    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        promql::types::json_escape(&mut out, item);
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
