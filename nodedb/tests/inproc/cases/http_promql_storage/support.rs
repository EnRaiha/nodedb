// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers: remote write and remote read over the wire, PromQL JSON
//! requests, API keys scoped to a database.

use nodedb::control::promql::remote_proto::{
    Label, LabelMatcher, MatchType, Query, ReadRequest, ReadResponse, Sample, TimeSeries,
    WriteRequest,
};
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};
use nodedb_test_support::pgwire_harness::TestServer;
use prost::Message;

/// Tenant of the harness superuser and of every key these tests mint.
pub const TENANT: u64 = 1;

/// A whole second, ten minutes in the past: inside every default lookback
/// and retention, and clear of the current minute.
pub fn base_ms() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64;
    (now / 1000 - 600) * 1000
}

pub fn base_url(srv: &TestServer) -> String {
    format!("http://127.0.0.1:{}/v1/obsv/api/v1", srv.http_port)
}

/// One series: `labels` includes `__name__`; `samples` are `(ms, value)`.
pub fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
    TimeSeries {
        labels: labels
            .iter()
            .map(|(name, value)| Label {
                name: (*name).into(),
                value: (*value).into(),
            })
            .collect(),
        samples: samples
            .iter()
            .map(|&(timestamp, value)| Sample { value, timestamp })
            .collect(),
        exemplars: vec![],
    }
}

/// Request context: an optional bearer token and an optional database name.
#[derive(Clone, Copy, Default)]
pub struct As<'a> {
    pub token: Option<&'a str>,
    pub database: Option<&'a str>,
}

fn apply(builder: reqwest::RequestBuilder, ctx: As<'_>) -> reqwest::RequestBuilder {
    let builder = match ctx.token {
        Some(token) => builder.header("Authorization", format!("Bearer {token}")),
        None => builder,
    };
    match ctx.database {
        Some(database) => builder.header("X-NodeDB-Database", database),
        None => builder,
    }
}

/// POST a snappy-compressed `WriteRequest`. Returns the status and body.
pub async fn remote_write(
    srv: &TestServer,
    ctx: As<'_>,
    timeseries: Vec<TimeSeries>,
) -> (reqwest::StatusCode, String) {
    let mut buf = Vec::new();
    WriteRequest { timeseries }
        .encode(&mut buf)
        .expect("encode write request");
    let body = snap::raw::Encoder::new()
        .compress_vec(&buf)
        .expect("snappy compress");
    let response = apply(
        reqwest::Client::new()
            .post(format!("{}/write", base_url(srv)))
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .body(body),
        ctx,
    )
    .send()
    .await
    .expect("POST remote write");
    let status = response.status();
    (status, response.text().await.expect("remote write body"))
}

/// A remote-read matcher.
pub fn matcher(match_type: MatchType, name: &str, value: &str) -> LabelMatcher {
    LabelMatcher {
        match_type: match_type as i32,
        name: name.into(),
        value: value.into(),
    }
}

/// POST one remote-read query. `Ok` is the decoded response, `Err` the
/// status and body of a refusal.
pub async fn remote_read(
    srv: &TestServer,
    ctx: As<'_>,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<LabelMatcher>,
) -> Result<ReadResponse, (reqwest::StatusCode, String)> {
    let mut buf = Vec::new();
    ReadRequest {
        queries: vec![Query {
            start_timestamp_ms: start_ms,
            end_timestamp_ms: end_ms,
            matchers,
        }],
    }
    .encode(&mut buf)
    .expect("encode read request");
    let body = snap::raw::Encoder::new()
        .compress_vec(&buf)
        .expect("snappy compress");
    let response = apply(
        reqwest::Client::new()
            .post(format!("{}/read", base_url(srv)))
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .body(body),
        ctx,
    )
    .send()
    .await
    .expect("POST remote read");
    let status = response.status();
    let bytes = response.bytes().await.expect("remote read body");
    if !status.is_success() {
        return Err((status, String::from_utf8_lossy(&bytes).into_owned()));
    }
    let raw = snap::raw::Decoder::new()
        .decompress_vec(&bytes)
        .expect("snappy decompress");
    Ok(ReadResponse::decode(&raw[..]).expect("decode read response"))
}

/// GET `{base}/{path}` with `query` pairs. Returns the status and the JSON.
pub async fn get_json(
    srv: &TestServer,
    ctx: As<'_>,
    path: &str,
    query: &[(&str, String)],
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = apply(
        reqwest::Client::new()
            .get(format!("{}/{path}", base_url(srv)))
            .query(query),
        ctx,
    )
    .send()
    .await
    .expect("GET promql endpoint");
    let status = response.status();
    let text = response.text().await.expect("promql body");
    let json = sonic_rs::from_str(&text).unwrap_or_else(|e| panic!("{path} JSON {e}: {text}"));
    (status, json)
}

/// POST `{base}/{path}` with a form-encoded body.
pub async fn post_form(
    srv: &TestServer,
    ctx: As<'_>,
    path: &str,
    form: &[(&str, String)],
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = apply(
        reqwest::Client::new()
            .post(format!("{}/{path}", base_url(srv)))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form_body(form)),
        ctx,
    )
    .send()
    .await
    .expect("POST promql endpoint");
    let status = response.status();
    let text = response.text().await.expect("promql body");
    let json = sonic_rs::from_str(&text).unwrap_or_else(|e| panic!("{path} JSON {e}: {text}"));
    (status, json)
}

/// Unix seconds of `ms`, as a PromQL time parameter.
pub fn secs(ms: i64) -> String {
    format!("{}", ms as f64 / 1000.0)
}

/// The `data.result` array of a query response.
pub fn result(json: &serde_json::Value) -> &Vec<serde_json::Value> {
    json.pointer("/data/result")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("no data.result in {json}"))
}

/// The value of an instant-vector element.
pub fn instant_value(element: &serde_json::Value) -> f64 {
    element
        .pointer("/value/1")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no value in {element}"))
}

/// The values of a range-vector element, in order.
pub fn range_values(element: &serde_json::Value) -> Vec<f64> {
    element
        .pointer("/values")
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("no values in {element}"))
        .iter()
        .map(|pair| {
            pair.pointer("/1")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("bad point {pair}"))
        })
        .collect()
}

/// The id of the database named `name`.
pub fn database_id(shared: &SharedState, name: &str) -> DatabaseId {
    shared
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .expect("read database catalog")
        .unwrap_or_else(|| panic!("database {name} exists"))
}

/// An API key for a new service account with `roles` in `databases`.
pub fn api_key(
    shared: &SharedState,
    username: &str,
    roles: Vec<Role>,
    databases: Vec<DatabaseId>,
) -> String {
    let user_id = shared
        .credentials
        .create_service_account(username, TenantId::new(TENANT), roles, databases.clone())
        .expect("create service account");
    shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username,
                user_id,
                tenant_id: TenantId::new(TENANT),
                expires_secs: 0,
                scope: vec![],
                accessible_databases: databases,
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create API key")
}

/// `application/x-www-form-urlencoded` body of `form`.
fn form_body(form: &[(&str, String)]) -> String {
    form.iter()
        .map(|(key, value)| format!("{}={}", form_escape(key), form_escape(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encode every byte outside the unreserved set.
fn form_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}
