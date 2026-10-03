// SPDX-License-Identifier: BUSL-1.1

//! The WebSocket upgrade and the plain RPC methods.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb::config::auth::AuthMode;
use tokio_tungstenite::tungstenite::Message;

use crate::cases::http_support::start_http;

// ─── Upgrade rejected under Password mode ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_upgrade_refused_without_bearer_token() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(
        result.is_err(),
        "WS upgrade must be refused under Password mode with no bearer token"
    );
}

// ─── Upgrade succeeds under Trust mode ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_upgrade_succeeds_under_trust_mode() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let result = tokio_tungstenite::connect_async(&url).await;
    assert!(
        result.is_ok(),
        "WS upgrade must succeed under Trust mode; error: {:?}",
        result.unwrap_err()
    );
}

// ─── ping → pong ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_ping_returns_pong_result() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    let req = serde_json::json!({
        "id": 1,
        "method": "ping"
    })
    .to_string();
    ws.send(Message::Text(req.into())).await.expect("send ping");

    let msg = tokio::time::timeout(Duration::from_millis(500), ws.next())
        .await
        .expect("timeout waiting for pong")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text, got {other:?}"),
    };

    let value: serde_json::Value =
        serde_json::from_str(&text).expect("pong response must be valid JSON");
    assert_eq!(value["id"], 1, "response id must match request id");
    assert_eq!(
        value["result"], "pong",
        "ping response must have result='pong'"
    );
}

// ─── query method returns result field ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_query_method_returns_result_field() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    let req = serde_json::json!({
        "id": 42,
        "method": "query",
        "params": {"sql": "SHOW USERS"}
    })
    .to_string();
    ws.send(Message::Text(req.into()))
        .await
        .expect("send query");

    let msg = tokio::time::timeout(Duration::from_millis(1000), ws.next())
        .await
        .expect("timeout waiting for query response")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text frame, got {other:?}"),
    };

    let value: serde_json::Value =
        serde_json::from_str(&text).expect("query response must be valid JSON");
    assert_eq!(value["id"], 42, "response id must match request id");
    // Either `result` (success) or `error` (failure) must be present — never neither.
    assert!(
        value.get("result").is_some() || value.get("error").is_some(),
        "WS query response must have 'result' or 'error' field; got: {value}"
    );
}

// ─── unknown method returns error field ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_unknown_method_returns_error() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    let req = serde_json::json!({
        "id": 99,
        "method": "nonexistent_method_xyz"
    })
    .to_string();
    ws.send(Message::Text(req.into()))
        .await
        .expect("send unknown method");

    let msg = tokio::time::timeout(Duration::from_millis(500), ws.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text frame, got {other:?}"),
    };

    let value: serde_json::Value = serde_json::from_str(&text).expect("must be valid JSON");
    assert_eq!(value["id"], 99, "response id must match request id");
    assert!(
        value.get("error").is_some(),
        "unknown method must produce an error response; got: {value}"
    );
}

// ─── malformed JSON frame ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_malformed_json_returns_error_not_crash() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect");

    ws.send(Message::Text("{not valid json".into()))
        .await
        .expect("send malformed");

    let msg = tokio::time::timeout(Duration::from_millis(500), ws.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("ws error");

    let text = match msg {
        Message::Text(t) => t.to_string(),
        other => panic!("expected Text, got {other:?}"),
    };
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("error response must be valid JSON");
    assert!(
        value.get("error").is_some(),
        "malformed JSON frame must return an error response, not crash; got: {value}"
    );
}
