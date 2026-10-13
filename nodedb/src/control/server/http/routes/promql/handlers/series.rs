// SPDX-License-Identifier: BUSL-1.1

//! GET/POST `/obsv/api/v1/series` — find series by label matchers.

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::control::promql::{self, PromqlError};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

use crate::control::server::http::routes::promql::helpers::{
    begin_request, now_ms, parse_series_matcher, prom_error_from, promql_error,
};
use crate::control::server::http::routes::promql::params::PromParams;
use crate::control::server::http::routes::promql::source::{
    ReadScope, SampleBudget, SelectorRead, load_series,
};

pub async fn series_query(
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
        "promql_series",
    ) {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let rl = request.rate_limit_headers;
    let now = now_ms();
    let tuning = &state.shared.tuning.promql;

    let reads = match series_reads(&request.params, now, tuning.lookback_delta_ms) {
        Ok(reads) => reads,
        Err(e) => return (rl, promql_error(&e)).into_response(),
    };
    let scope = ReadScope::new(&state.shared, &identity.0, request.database_id);
    let mut budget = SampleBudget::new(tuning.max_samples);
    let series = match load_series(&scope, &reads, &mut budget, now).await {
        Ok(series) => series,
        Err(e) => return (rl, prom_error_from(&e)).into_response(),
    };

    let mut out = String::from(r#"{"status":"success","data":["#);
    for (i, s) in series.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        promql::types::write_labels_json(&mut out, &s.labels);
    }
    out.push_str("]}");

    (
        StatusCode::OK,
        rl,
        [("content-type", "application/json")],
        out,
    )
        .into_response()
}

/// One read per `match[]` value. Prometheus requires at least one.
fn series_reads(
    params: &PromParams,
    now: i64,
    lookback_ms: i64,
) -> Result<Vec<SelectorRead>, PromqlError> {
    let (start_ms, end_ms) = time_window(params, now, lookback_ms)?;
    let matchers = params.all("match[]");
    if matchers.is_empty() {
        return Err(PromqlError::BadParameter {
            param: "match[]".to_owned(),
            detail: "no match[] parameter provided".to_owned(),
        });
    }
    matchers
        .into_iter()
        .map(|m| parse_series_matcher(m, start_ms, end_ms))
        .collect()
}

/// The `[start, end]` window of a metadata endpoint. `end` defaults to now
/// and `start` to one lookback before `end`.
pub(crate) fn time_window(
    params: &PromParams,
    now: i64,
    lookback_ms: i64,
) -> Result<(i64, i64), PromqlError> {
    let end_ms = params.time_ms("end")?.unwrap_or(now);
    let start_ms = params
        .time_ms("start")?
        .unwrap_or(end_ms.saturating_sub(lookback_ms));
    if end_ms < start_ms {
        return Err(PromqlError::BadParameter {
            param: "end".to_owned(),
            detail: "end timestamp must not be before start time".to_owned(),
        });
    }
    Ok((start_ms, end_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(query: &str) -> PromParams {
        PromParams::from_request(Some(query), &HeaderMap::new(), &[]).unwrap()
    }

    #[test]
    fn match_is_required() {
        assert!(series_reads(&params("start=0&end=1"), 0, 1).is_err());
    }

    #[test]
    fn every_match_becomes_a_read() {
        let reads = series_reads(&params("match[]=up&match[]=down&start=0&end=1"), 0, 1).unwrap();
        assert_eq!(reads.len(), 2);
        assert_eq!((reads[0].min_ts, reads[0].max_ts), (0, 1000));
    }

    #[test]
    fn unparseable_match_is_an_error() {
        assert!(series_reads(&params("match[]=sum(up)"), 0, 1).is_err());
    }
}
