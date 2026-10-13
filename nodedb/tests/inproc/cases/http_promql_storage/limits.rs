// SPDX-License-Identifier: BUSL-1.1

//! Limits and malformed requests fail loudly; nothing is truncated.

use nodedb::control::promql::remote_proto::{LabelMatcher, MatchType};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::config::tuning::PromqlTuning;

use super::support::{As, base_ms, get_json, matcher, remote_read, remote_write, secs, series};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn too_many_samples_fails_loudly() {
    let srv = TestServer::start_with_promql_tuning(PromqlTuning {
        max_samples: 2,
        ..PromqlTuning::default()
    })
    .await;
    let t0 = base_ms();
    let (status, body) = remote_write(
        &srv,
        As::default(),
        vec![series(
            &[("__name__", "busy")],
            &[(t0, 1.0), (t0 + 1_000, 2.0), (t0 + 2_000, 3.0)],
        )],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT, "{body}");

    let (status, json) = get_json(
        &srv,
        As::default(),
        "query_range",
        &[
            ("query", "busy".to_owned()),
            ("start", secs(t0)),
            ("end", secs(t0 + 2_000)),
            ("step", "1s".to_owned()),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY, "{json}");
    assert_eq!(json["errorType"], "execution");
    let message = json["error"].as_str().unwrap_or_default();
    assert!(message.contains("too many samples"), "{json}");

    let refused = remote_read(
        &srv,
        As::default(),
        t0,
        t0 + 2_000,
        vec![matcher(MatchType::Eq, "__name__", "busy")],
    )
    .await;
    let (status, _) = refused.expect_err("remote read over the budget is refused");
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    // Two samples fit the budget exactly.
    let response = remote_read(
        &srv,
        As::default(),
        t0,
        t0 + 1_000,
        vec![matcher(MatchType::Eq, "__name__", "busy")],
    )
    .await
    .unwrap_or_else(|e| panic!("remote read within the budget: {e:?}"));
    assert_eq!(response.results[0].timeseries[0].samples.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_query_resolution_is_capped() {
    let srv = TestServer::start().await;
    let (status, json) = get_json(
        &srv,
        As::default(),
        "query_range",
        &[
            ("query", "up".to_owned()),
            ("start", "0".to_owned()),
            ("end", "100000".to_owned()),
            ("step", "1s".to_owned()),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{json}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_step_is_refused() {
    let srv = TestServer::start().await;
    let (status, json) = get_json(
        &srv,
        As::default(),
        "query_range",
        &[
            ("query", "up".to_owned()),
            ("start", "0".to_owned()),
            ("end", "60".to_owned()),
            ("step", "soon".to_owned()),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{json}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_read_unknown_match_type_is_refused() {
    let srv = TestServer::start().await;
    let refused = remote_read(
        &srv,
        As::default(),
        0,
        1_000,
        vec![LabelMatcher {
            match_type: 9,
            name: "__name__".into(),
            value: "up".into(),
        }],
    )
    .await;
    let (status, body) = refused.expect_err("unknown match type is refused");
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{body}");
}
