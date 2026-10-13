// SPDX-License-Identifier: BUSL-1.1

//! Prometheus remote read HTTP handler.
//!
//! - POST `/obsv/api/v1/read` — accept snappy-compressed protobuf `ReadRequest`,
//!   answer a snappy-compressed protobuf `ReadResponse`
//!
//! Each query returns the stored series its matchers select, with the
//! samples in `[start, end]`. Remote read applies no lookback.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::control::promql::remote_proto::{
    self, Label, MatchType, QueryResult, ReadRequest, ReadResponse, Sample, TimeSeries,
};
use crate::control::promql::{self, PromqlError};
use crate::control::server::http::admission::admit_without_rate_limit;
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use super::helpers::{now_ms, prom_error_from};
use super::remote::decompress_body;
use super::source::{ReadScope, SampleBudget, SelectorRead, load_series};

/// POST `/obsv/api/v1/read` — Prometheus remote read endpoint.
pub async fn remote_read(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // The `X-NodeDB-Database` header selects the database the request reads.
    let database_id = match crate::control::server::http::routes::query::resolve_database_id(
        &headers,
        &crate::control::server::http::routes::query::DatabaseQueryParam::default(),
        &state,
    ) {
        Ok(id) => id,
        Err(error) => return error.into_response(),
    };
    // Same door as remote write and `/metrics`: this is the observability
    // surface a Prometheus/Grafana deployment polls, not per-query traffic.
    // Runs before the body is decompressed or decoded.
    if let Err(error) = admit_without_rate_limit(&state, &identity.0, database_id, peer.as_str()) {
        return error.into_response();
    }

    let decompressed = match decompress_body(&headers, &body) {
        Ok(d) => d,
        Err(response) => return *response,
    };
    let read_req = match ReadRequest::decode(&decompressed[..]) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("protobuf decode error: {e}"),
            )
                .into_response();
        }
    };

    let scope = ReadScope::new(&state.shared, &identity.0, database_id);
    // One budget for the whole request, across all of its queries.
    let mut budget = SampleBudget::new(state.shared.tuning.promql.max_samples);
    let now = now_ms();
    let mut results = Vec::with_capacity(read_req.queries.len());
    for query in &read_req.queries {
        match execute_read_query(&scope, query, &mut budget, now).await {
            Ok(timeseries) => results.push(QueryResult { timeseries }),
            Err(e) => return prom_error_from(&e).into_response(),
        }
    }

    let response = ReadResponse { results };
    let mut response_buf = Vec::new();
    if let Err(e) = response.encode(&mut response_buf) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("protobuf encode error: {e}"),
        )
            .into_response();
    }

    let compressed = match snap::raw::Encoder::new().compress_vec(&response_buf) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "snappy compression failed for remote read response");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "compression error".to_string(),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        [
            ("content-type", "application/x-protobuf"),
            ("content-encoding", "snappy"),
        ],
        compressed,
    )
        .into_response()
}

/// Run one remote read query against the stored series.
async fn execute_read_query(
    scope: &ReadScope<'_>,
    query: &remote_proto::Query,
    budget: &mut SampleBudget,
    now: i64,
) -> crate::Result<Vec<TimeSeries>> {
    let start_ms = query.start_timestamp_ms;
    let end_ms = query.end_timestamp_ms;
    let read = SelectorRead {
        name: None,
        matchers: convert_matchers(&query.matchers)?,
        min_ts: start_ms,
        max_ts: end_ms,
    };
    let series = load_series(scope, std::slice::from_ref(&read), budget, now).await?;

    Ok(series
        .into_iter()
        .map(|s| TimeSeries {
            labels: s
                .labels
                .into_iter()
                .map(|(name, value)| Label { name, value })
                .collect(),
            samples: s
                .samples
                .iter()
                .filter(|sample| sample.timestamp_ms >= start_ms && sample.timestamp_ms <= end_ms)
                .map(|sample| Sample {
                    value: sample.value,
                    timestamp: sample.timestamp_ms,
                })
                .collect(),
            exemplars: vec![],
        })
        .filter(|ts| !ts.samples.is_empty())
        .collect())
}

/// Protobuf matchers → PromQL matchers. An unknown match type or a regex
/// that does not compile is an error.
fn convert_matchers(
    matchers: &[remote_proto::LabelMatcher],
) -> Result<Vec<promql::LabelMatcher>, PromqlError> {
    matchers
        .iter()
        .map(|m| {
            let op = match MatchType::try_from(m.match_type) {
                Ok(MatchType::Eq) => promql::LabelMatchOp::Equal,
                Ok(MatchType::Neq) => promql::LabelMatchOp::NotEqual,
                Ok(MatchType::Re) => promql::LabelMatchOp::RegexMatch,
                Ok(MatchType::Nre) => promql::LabelMatchOp::RegexNotMatch,
                Err(_) => {
                    return Err(PromqlError::UnknownMatchType {
                        match_type: m.match_type,
                    });
                }
            };
            promql::LabelMatcher::new(m.name.clone(), op, m.value.clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher(match_type: i32, value: &str) -> remote_proto::LabelMatcher {
        remote_proto::LabelMatcher {
            match_type,
            name: "job".into(),
            value: value.into(),
        }
    }

    #[test]
    fn unknown_match_type_is_an_error() {
        let err = convert_matchers(&[matcher(9, "api")]).unwrap_err();
        assert!(matches!(
            err,
            PromqlError::UnknownMatchType { match_type: 9 }
        ));
    }

    #[test]
    fn invalid_regex_is_an_error() {
        assert!(convert_matchers(&[matcher(MatchType::Re as i32, "(")]).is_err());
    }

    #[test]
    fn known_match_types_convert() {
        let m = convert_matchers(&[
            matcher(MatchType::Eq as i32, "a"),
            matcher(MatchType::Neq as i32, "a"),
            matcher(MatchType::Re as i32, "a.*"),
            matcher(MatchType::Nre as i32, "a.*"),
        ])
        .unwrap();
        assert_eq!(m.len(), 4);
    }
}
