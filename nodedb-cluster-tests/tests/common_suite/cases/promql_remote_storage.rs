// SPDX-License-Identifier: BUSL-1.1

//! PromQL reads over remote-written data on a 3-node cluster.
//!
//! Remote write on one node registers each metric collection through the
//! replicated catalog and stores the samples on the metric's owning data
//! group. Remote read and `/query` on every node then return the same
//! series: discovery reads each node's replicated catalog, and every scan
//! is routed to the group that owns the metric.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb::control::promql::remote_proto::{
    Label, LabelMatcher, MatchType, Query, ReadRequest, ReadResponse, Sample, TimeSeries,
    WriteRequest,
};
use prost::Message;

use common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};

/// An HTTP listener serving `node`'s state in trust mode.
async fn start_http(node: &TestClusterNode) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http listener");
    let addr = listener.local_addr().expect("http local addr");
    let shared = Arc::clone(&node.shared);
    let (bus, _) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    let handle = tokio::spawn(async move {
        let _ = nodedb::control::server::http::server::run_with_listener(
            listener,
            shared,
            AuthMode::Trust,
            None,
            bus,
        )
        .await;
    });
    (format!("http://{addr}/v1/obsv/api/v1"), handle)
}

fn base_ms() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64;
    (now / 1000 - 600) * 1000
}

fn series(metric: &str, job: &str, samples: &[(i64, f64)]) -> TimeSeries {
    TimeSeries {
        labels: vec![
            Label {
                name: "__name__".into(),
                value: metric.into(),
            },
            Label {
                name: "job".into(),
                value: job.into(),
            },
        ],
        samples: samples
            .iter()
            .map(|&(timestamp, value)| Sample { value, timestamp })
            .collect(),
        exemplars: vec![],
    }
}

async fn remote_write(base: &str, timeseries: Vec<TimeSeries>) -> (reqwest::StatusCode, String) {
    let mut buf = Vec::new();
    WriteRequest { timeseries }
        .encode(&mut buf)
        .expect("encode write request");
    let body = snap::raw::Encoder::new()
        .compress_vec(&buf)
        .expect("snappy compress");
    let response = reqwest::Client::new()
        .post(format!("{base}/write"))
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .expect("POST remote write");
    let status = response.status();
    (status, response.text().await.expect("remote write body"))
}

/// `(metric, job, values)` of every series a remote read returns, sorted.
async fn remote_read(base: &str, start_ms: i64, end_ms: i64) -> Option<Vec<(String, Vec<f64>)>> {
    let mut buf = Vec::new();
    ReadRequest {
        queries: vec![Query {
            start_timestamp_ms: start_ms,
            end_timestamp_ms: end_ms,
            matchers: vec![LabelMatcher {
                match_type: MatchType::Re as i32,
                name: "__name__".into(),
                value: "promql_cluster_m1|promql_cluster_m2".into(),
            }],
        }],
    }
    .encode(&mut buf)
    .expect("encode read request");
    let body = snap::raw::Encoder::new()
        .compress_vec(&buf)
        .expect("snappy compress");
    let response = reqwest::Client::new()
        .post(format!("{base}/read"))
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let bytes = response.bytes().await.ok()?;
    let raw = snap::raw::Decoder::new().decompress_vec(&bytes).ok()?;
    let decoded = ReadResponse::decode(&raw[..]).ok()?;
    let mut out: Vec<(String, Vec<f64>)> = decoded
        .results
        .first()?
        .timeseries
        .iter()
        .map(|ts| {
            let label = |name: &str| {
                ts.labels
                    .iter()
                    .find(|l| l.name == name)
                    .map(|l| l.value.clone())
                    .unwrap_or_default()
            };
            (
                format!("{}:{}", label("__name__"), label("job")),
                ts.samples.iter().map(|s| s.value).collect(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Some(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_write_on_one_node_reads_back_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    let mut endpoints = Vec::new();
    for node in &cluster.nodes {
        endpoints.push(start_http(node).await);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let t0 = base_ms();
    let (status, body) = remote_write(
        &endpoints[0].0,
        vec![
            series("promql_cluster_m1", "api", &[(t0, 1.0), (t0 + 15_000, 2.0)]),
            series("promql_cluster_m2", "web", &[(t0, 5.0)]),
        ],
    )
    .await;
    assert_eq!(
        status,
        reqwest::StatusCode::NO_CONTENT,
        "remote write: {body}"
    );

    let expected = vec![
        ("promql_cluster_m1:api".to_owned(), vec![1.0, 2.0]),
        ("promql_cluster_m2:web".to_owned(), vec![5.0]),
    ];
    for (index, (base, _)) in endpoints.iter().enumerate() {
        let base = base.clone();
        let want = expected.clone();
        wait_for_async(
            &format!("node {index} reads both metrics back"),
            Duration::from_secs(15),
            Duration::from_millis(100),
            || {
                let base = base.clone();
                let want = want.clone();
                async move { remote_read(&base, t0, t0 + 15_000).await == Some(want) }
            },
        )
        .await;

        let response = reqwest::Client::new()
            .get(format!("{base}/query"))
            .query(&[
                ("query", "sum(promql_cluster_m1)".to_owned()),
                ("time", format!("{}", (t0 + 15_000) as f64 / 1000.0)),
            ])
            .send()
            .await
            .expect("GET /query");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let text = response.text().await.expect("query body");
        let json: serde_json::Value =
            sonic_rs::from_str(&text).unwrap_or_else(|e| panic!("query JSON {e}: {text}"));
        let value = json
            .pointer("/data/result/0/value/1")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("node {index}: no value in {json}"));
        assert_eq!(value, "2", "node {index}: {json}");
    }

    for (_, handle) in endpoints {
        handle.abort();
    }
    cluster.shutdown().await;
}
