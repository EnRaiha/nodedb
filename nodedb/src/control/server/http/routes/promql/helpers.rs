// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for PromQL HTTP handlers.

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::control::promql::{self, PromqlError};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::http::admission::admit;
use crate::control::server::http::auth::AppState;
use crate::control::server::http::routes::query::resolve_database_id;
use crate::types::DatabaseId;

use super::params::PromParams;
use super::source::SelectorRead;

pub type PromResponse = (StatusCode, [(&'static str, &'static str); 1], String);

pub fn prom_success(value: promql::Value) -> PromResponse {
    let result = promql::PromResult::success(value);
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        result.to_json(),
    )
}

pub fn prom_error(err_type: &str, message: &str) -> PromResponse {
    let status = match err_type {
        "bad_data" => StatusCode::BAD_REQUEST,
        "execution" => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    prom_error_status(status, err_type, message)
}

/// A Prometheus error envelope with an explicit HTTP status.
pub fn prom_error_status(status: StatusCode, err_type: &str, message: &str) -> PromResponse {
    let result = promql::PromResult::error(err_type, message.to_string());
    (
        status,
        [("content-type", "application/json")],
        result.to_json(),
    )
}

/// The Prometheus error envelope for a PromQL error.
pub fn promql_error(err: &PromqlError) -> PromResponse {
    if err.is_bad_data() {
        prom_error("bad_data", &err.to_string())
    } else {
        prom_error("execution", &err.to_string())
    }
}

/// The Prometheus error envelope for any error a PromQL read returns. A
/// PromQL error keeps its Prometheus class. Every other error keeps the
/// HTTP status the gateway maps it to, such as 403 for a denied read.
pub fn prom_error_from(err: &crate::Error) -> PromResponse {
    match err {
        crate::Error::Promql(inner) => promql_error(inner),
        other => {
            let (code, message) = crate::control::gateway::GatewayErrorMap::to_http(other);
            let status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            prom_error_status(status, "execution", &message)
        }
    }
}

/// A range-query step: a PromQL duration or float seconds, positive.
pub fn parse_step(s: &str) -> Result<i64, PromqlError> {
    let ms = match promql::ast::Duration::parse(s) {
        Some(d) => Some(d.ms()),
        None => s
            .parse::<f64>()
            .ok()
            .filter(|secs| secs.is_finite())
            .map(|secs| (secs * 1000.0).round() as i64),
    };
    ms.filter(|ms| *ms > 0)
        .ok_or_else(|| PromqlError::BadParameter {
            param: "step".to_owned(),
            detail: format!("cannot parse {s:?} as a positive duration"),
        })
}

/// Parse a `match[]` value. It must be a single vector selector.
pub fn parse_series_matcher(
    input: &str,
    min_ts: i64,
    max_ts: i64,
) -> Result<SelectorRead, PromqlError> {
    let tokens = promql::lexer::tokenize(input)?;
    match promql::parse(&tokens)? {
        promql::ast::Expr::VectorSelector { name, matchers, .. } => Ok(SelectorRead {
            name,
            matchers,
            min_ts,
            max_ts,
        }),
        _ => Err(PromqlError::BadParameter {
            param: "match[]".to_owned(),
            detail: format!("{input:?} is not a vector selector"),
        }),
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// A PromQL request past parameter decoding, database resolution and the
/// admission gate.
pub struct PromRequest {
    pub params: PromParams,
    pub database_id: DatabaseId,
    /// `X-RateLimit-*` headers every response of the request carries.
    pub rate_limit_headers: HeaderMap,
}

/// Decode parameters, resolve the request's database (`X-NodeDB-Database`
/// header, then `database` parameter, then the default database) and admit
/// the request against that database.
pub fn begin_request(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &Bytes,
    operation: &str,
) -> Result<PromRequest, Box<Response>> {
    let params = PromParams::from_request(raw_query, headers, body)
        .map_err(|e| Box::new(promql_error(&e).into_response()))?;
    let database_id = resolve_database_id(headers, &params.database(), state)
        .map_err(|e| Box::new(e.into_response()))?;
    let rate_limit_headers = admit(state, identity, database_id, peer_addr, operation)
        .map_err(|e| Box::new(e.into_response()))?;
    Ok(PromRequest {
        params,
        database_id,
        rate_limit_headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_accepts_durations_and_seconds() {
        assert_eq!(parse_step("15s").unwrap(), 15_000);
        assert_eq!(parse_step("1m").unwrap(), 60_000);
        assert_eq!(parse_step("0.5").unwrap(), 500);
    }

    #[test]
    fn bad_step_is_an_error() {
        for raw in ["", "abc", "0", "-5", "NaN"] {
            assert!(parse_step(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn series_matcher_must_be_a_selector() {
        let read = parse_series_matcher(r#"up{job="api"}"#, 1, 2).unwrap();
        assert_eq!(read.name.as_deref(), Some("up"));
        assert_eq!(read.matchers.len(), 1);
        assert!(parse_series_matcher("sum(up)", 1, 2).is_err());
        assert!(parse_series_matcher(r#"up{job=~"("}"#, 1, 2).is_err());
    }
}
