// SPDX-License-Identifier: BUSL-1.1

//! Remote-written samples read back through remote read and PromQL.

use nodedb::control::promql::remote_proto::MatchType;
use nodedb_test_support::pgwire_harness::TestServer;

use super::support::{
    As, base_ms, get_json, instant_value, matcher, post_form, range_values, remote_read,
    remote_write, result, secs, series,
};

/// Write `http_requests_total` for jobs `api` (1, 2, 3 at t0, t0+15s,
/// t0+30s) and `web` (7 at t0). Returns t0.
async fn seed_requests(srv: &TestServer) -> i64 {
    let t0 = base_ms();
    let (status, body) = remote_write(
        srv,
        As::default(),
        vec![
            series(
                &[
                    ("__name__", "http_requests_total"),
                    ("job", "api"),
                    ("instance", "a"),
                ],
                &[(t0, 1.0), (t0 + 15_000, 2.0), (t0 + 30_000, 3.0)],
            ),
            series(
                &[("__name__", "http_requests_total"), ("job", "web")],
                &[(t0, 7.0)],
            ),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_write_then_remote_read_round_trip() {
    let srv = TestServer::start().await;
    let t0 = seed_requests(&srv).await;

    let response = remote_read(
        &srv,
        As::default(),
        t0,
        t0 + 30_000,
        vec![
            matcher(MatchType::Eq, "__name__", "http_requests_total"),
            matcher(MatchType::Eq, "job", "api"),
        ],
    )
    .await
    .unwrap_or_else(|e| panic!("remote read refused: {e:?}"));
    assert_eq!(response.results.len(), 1);
    let timeseries = &response.results[0].timeseries;
    assert_eq!(timeseries.len(), 1, "{timeseries:?}");
    let labels: Vec<(&str, &str)> = timeseries[0]
        .labels
        .iter()
        .map(|l| (l.name.as_str(), l.value.as_str()))
        .collect();
    assert_eq!(
        labels,
        vec![
            ("__name__", "http_requests_total"),
            ("instance", "a"),
            ("job", "api"),
        ]
    );
    let samples: Vec<(i64, f64)> = timeseries[0]
        .samples
        .iter()
        .map(|s| (s.timestamp, s.value))
        .collect();
    assert_eq!(
        samples,
        vec![(t0, 1.0), (t0 + 15_000, 2.0), (t0 + 30_000, 3.0)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instant_query_reads_written_samples() {
    let srv = TestServer::start().await;
    let t0 = seed_requests(&srv).await;
    let at = secs(t0 + 30_000);

    let (status, json) = get_json(
        &srv,
        As::default(),
        "query",
        &[
            ("query", r#"http_requests_total{job="api"}"#.to_owned()),
            ("time", at.clone()),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    let vector = result(&json);
    assert_eq!(vector.len(), 1, "{json}");
    assert_eq!(instant_value(&vector[0]), 3.0);

    let (status, json) = get_json(
        &srv,
        As::default(),
        "query",
        &[
            ("query", "sum(http_requests_total)".to_owned()),
            ("time", at),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    assert_eq!(instant_value(&result(&json)[0]), 10.0, "{json}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn instant_query_post_form_body() {
    let srv = TestServer::start().await;
    let t0 = seed_requests(&srv).await;

    let (status, json) = post_form(
        &srv,
        As::default(),
        "query",
        &[
            ("query", r#"http_requests_total{job="api"}"#.to_owned()),
            ("time", secs(t0 + 30_000)),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    assert_eq!(instant_value(&result(&json)[0]), 3.0, "{json}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_query_steps_over_written_samples() {
    let srv = TestServer::start().await;
    let t0 = seed_requests(&srv).await;

    let (status, json) = get_json(
        &srv,
        As::default(),
        "query_range",
        &[
            ("query", r#"http_requests_total{job="api"}"#.to_owned()),
            ("start", secs(t0)),
            ("end", secs(t0 + 30_000)),
            ("step", "15s".to_owned()),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    let matrix = result(&json);
    assert_eq!(matrix.len(), 1, "{json}");
    assert_eq!(range_values(&matrix[0]), vec![1.0, 2.0, 3.0]);

    let (status, json) = get_json(
        &srv,
        As::default(),
        "query",
        &[
            (
                "query",
                r#"rate(http_requests_total{job="api"}[1m])"#.to_owned(),
            ),
            ("time", secs(t0 + 30_000)),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    let rate = instant_value(&result(&json)[0]);
    assert!((rate - 2.0 / 30.0).abs() < 1e-9, "rate {rate}: {json}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subquery_over_written_samples() {
    let srv = TestServer::start().await;
    let t0 = seed_requests(&srv).await;
    let at = secs(t0 + 30_000);

    // Steps at t0+1s ..= t0+30s see the api series step from 1 to 2 to 3.
    for (query, expected) in [
        (
            r#"max_over_time(http_requests_total{job="api"}[30s:1s])"#,
            3.0,
        ),
        (
            r#"min_over_time(http_requests_total{job="api"}[30s:1s])"#,
            1.0,
        ),
        // Shifted back 10s: steps at t0-9s ..= t0+20s; the latest is 2.
        (
            r#"max_over_time(http_requests_total{job="api"}[30s:1s] offset 10s)"#,
            2.0,
        ),
    ] {
        let (status, json) = get_json(
            &srv,
            As::default(),
            "query",
            &[("query", query.to_owned()), ("time", at.clone())],
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "{query}: {json}");
        let vector = result(&json);
        assert_eq!(vector.len(), 1, "{query}: {json}");
        assert_eq!(instant_value(&vector[0]), expected, "{query}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_write_registers_metric_collection() {
    let srv = TestServer::start().await;
    seed_requests(&srv).await;

    let rows = srv
        .query_text("SELECT COUNT(*) FROM http_requests_total")
        .await
        .expect("SQL resolves the remote-written metric");
    assert_eq!(rows, vec!["4".to_owned()]);

    // The registration DDL keeps the column names the read path expects.
    let stored = srv
        .shared
        .credentials
        .catalog()
        .get_collection(nodedb::types::DatabaseId::DEFAULT, 1, "http_requests_total")
        .expect("read catalog")
        .expect("metric collection registered");
    let nodedb_types::CollectionType::Columnar(nodedb_types::ColumnarProfile::Timeseries {
        time_key,
        ..
    }) = &stored.collection_type
    else {
        panic!("not a timeseries collection: {:?}", stored.collection_type);
    };
    assert_eq!(time_key, "timestamp");
    let names: Vec<&str> = stored.fields.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"timestamp") && names.contains(&"value"),
        "declared columns: {:?}",
        stored.fields
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_filters_remote_written_metric_by_label() {
    let srv = TestServer::start().await;
    seed_requests(&srv).await;

    // The label columns reach the catalog through the asynchronous field
    // projection merge, so the query is retried until it resolves.
    let sql = "SELECT value FROM http_requests_total WHERE job = 'api'";
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let rows = loop {
        match srv.query_text(sql).await {
            Ok(rows) if rows.len() == 3 => break rows,
            other if std::time::Instant::now() >= deadline => {
                panic!("{sql} never returned the 3 api rows: {other:?}")
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    };
    let mut values: Vec<f64> = rows
        .iter()
        .map(|v| v.parse().unwrap_or_else(|_| panic!("numeric value: {v}")))
        .collect();
    values.sort_by(f64::total_cmp);
    assert_eq!(values, vec![1.0, 2.0, 3.0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn label_value_with_space_and_comma_round_trips() {
    let srv = TestServer::start().await;
    let t0 = base_ms();
    let path = "/a b,c=d";
    let (status, body) = remote_write(
        &srv,
        As::default(),
        vec![series(
            &[("__name__", "page_views"), ("path", path)],
            &[(t0, 5.0)],
        )],
    )
    .await;
    assert_eq!(
        status,
        reqwest::StatusCode::NO_CONTENT,
        "remote write: {body}"
    );

    let response = remote_read(
        &srv,
        As::default(),
        t0,
        t0,
        vec![matcher(MatchType::Eq, "__name__", "page_views")],
    )
    .await
    .unwrap_or_else(|e| panic!("remote read refused: {e:?}"));
    let timeseries = &response.results[0].timeseries;
    assert_eq!(timeseries.len(), 1, "{timeseries:?}");
    let stored = timeseries[0]
        .labels
        .iter()
        .find(|l| l.name == "path")
        .map(|l| l.value.as_str());
    assert_eq!(stored, Some(path));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nan_sample_does_not_reject_series() {
    let srv = TestServer::start().await;
    let t0 = base_ms();
    let (status, body) = remote_write(
        &srv,
        As::default(),
        vec![series(
            &[("__name__", "stale_gauge")],
            &[(t0, 1.0), (t0 + 1_000, f64::NAN), (t0 + 2_000, 3.0)],
        )],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(body, r#"{"accepted":2,"rejected":1}"#);

    let response = remote_read(
        &srv,
        As::default(),
        t0,
        t0 + 2_000,
        vec![matcher(MatchType::Eq, "__name__", "stale_gauge")],
    )
    .await
    .unwrap_or_else(|e| panic!("remote read refused: {e:?}"));
    let values: Vec<f64> = response.results[0].timeseries[0]
        .samples
        .iter()
        .map(|s| s.value)
        .collect();
    assert_eq!(values, vec![1.0, 3.0]);
}
