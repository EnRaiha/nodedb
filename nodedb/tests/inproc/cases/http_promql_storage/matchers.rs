// SPDX-License-Identifier: BUSL-1.1

//! Label matchers over stored series: the ones pushed into the scan and the
//! ones applied after it give the same answer Prometheus gives.

use std::collections::BTreeSet;

use nodedb_test_support::pgwire_harness::TestServer;

use super::support::{As, base_ms, get_json, remote_write, result, secs, series};

/// Write `jobs_up` for jobs `api`, `web` and `batch` (`batch` without an
/// `env` label) and `jobs_down` for `api`. Returns the sample time.
async fn seed_jobs(srv: &TestServer) -> i64 {
    let t0 = base_ms();
    let (status, body) = remote_write(
        srv,
        As::default(),
        vec![
            series(
                &[("__name__", "jobs_up"), ("job", "api"), ("env", "prod")],
                &[(t0, 1.0)],
            ),
            series(
                &[("__name__", "jobs_up"), ("job", "web"), ("env", "prod")],
                &[(t0, 2.0)],
            ),
            series(&[("__name__", "jobs_up"), ("job", "batch")], &[(t0, 3.0)]),
            series(&[("__name__", "jobs_down"), ("job", "api")], &[(t0, 4.0)]),
        ],
    )
    .await;
    assert_eq!(
        status,
        reqwest::StatusCode::NO_CONTENT,
        "remote write: {body}"
    );
    t0
}

/// The `job` labels of the instant query `query` at `t0`.
async fn jobs(srv: &TestServer, query: &str, t0: i64) -> BTreeSet<String> {
    let (status, json) = get_json(
        srv,
        As::default(),
        "query",
        &[("query", query.to_owned()), ("time", secs(t0))],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{query}: {json}");
    result(&json)
        .iter()
        .map(|e| {
            let name = e.pointer("/metric/__name__").and_then(|v| v.as_str());
            let job = e.pointer("/metric/job").and_then(|v| v.as_str());
            format!("{}:{}", name.unwrap_or(""), job.unwrap_or(""))
        })
        .collect()
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn label_matchers_push_down_and_post_filter() {
    let srv = TestServer::start().await;
    let t0 = seed_jobs(&srv).await;

    assert_eq!(
        jobs(&srv, r#"jobs_up{job!="api"}"#, t0).await,
        set(&["jobs_up:batch", "jobs_up:web"])
    );
    assert_eq!(
        jobs(&srv, r#"jobs_up{job=~"api|web"}"#, t0).await,
        set(&["jobs_up:api", "jobs_up:web"])
    );
    assert_eq!(
        jobs(&srv, r#"jobs_up{job=~"a.*"}"#, t0).await,
        set(&["jobs_up:api"])
    );
    assert_eq!(
        jobs(&srv, r#"jobs_up{job!~"a.*"}"#, t0).await,
        set(&["jobs_up:batch", "jobs_up:web"])
    );
    assert_eq!(
        jobs(&srv, r#"jobs_up{env=""}"#, t0).await,
        set(&["jobs_up:batch"])
    );
    assert_eq!(
        jobs(&srv, r#"{__name__=~"jobs_.*", job="api"}"#, t0).await,
        set(&["jobs_down:api", "jobs_up:api"])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unselective_selector_is_refused() {
    let srv = TestServer::start().await;
    let t0 = seed_jobs(&srv).await;
    let (status, json) = get_json(
        &srv,
        As::default(),
        "query",
        &[("query", r#"{job=~".*"}"#.to_owned()), ("time", secs(t0))],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["errorType"], "bad_data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_regex_is_refused() {
    let srv = TestServer::start().await;
    let (status, json) = get_json(
        &srv,
        As::default(),
        "query",
        &[("query", r#"jobs_up{job=~"("}"#.to_owned())],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(json["errorType"], "bad_data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn series_requires_match() {
    let srv = TestServer::start().await;
    let (status, json) = get_json(&srv, As::default(), "series", &[]).await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{json}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn series_and_labels_list_stored_series() {
    let srv = TestServer::start().await;
    let t0 = seed_jobs(&srv).await;
    let window = [("start", secs(t0 - 1_000)), ("end", secs(t0 + 1_000))];

    let mut query = vec![("match[]", r#"jobs_up{env="prod"}"#.to_owned())];
    query.extend(window.iter().cloned());
    let (status, json) = get_json(&srv, As::default(), "series", &query).await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    assert_eq!(json["data"].as_array().map(Vec::len), Some(2), "{json}");

    let (status, json) = get_json(&srv, As::default(), "label/job/values", window.as_ref()).await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    assert_eq!(
        json["data"],
        serde_json::json!(["api", "batch", "web"]),
        "{json}"
    );

    let (status, json) = get_json(&srv, As::default(), "labels", window.as_ref()).await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    let names: Vec<&str> = json["data"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    for expected in ["__name__", "env", "job"] {
        assert!(names.contains(&expected), "{expected} in {json}");
    }

    let (status, json) = get_json(&srv, As::default(), "label/__name__/values", &[]).await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    let metrics: Vec<&str> = json["data"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(
        metrics.contains(&"jobs_up") && metrics.contains(&"jobs_down"),
        "{json}"
    );
}
