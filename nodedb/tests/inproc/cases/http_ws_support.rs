// SPDX-License-Identifier: BUSL-1.1

//! Steps the WebSocket RPC cases share: connections, JSON frame reads, and
//! the resume auth exchange.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Message, http};

use super::http_support::TestServer;

pub(super) type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const WS_READ_TIMEOUT: Duration = Duration::from_millis(500);

pub(super) async fn connect_ws(srv: &TestServer) -> WsStream {
    let url = format!("ws://{}/v1/ws", srv.local_addr);
    tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS connect")
        .0
}

pub(super) async fn connect_ws_in_database(srv: &TestServer, database: &str) -> WsStream {
    let mut request = format!("ws://{}/v1/ws", srv.local_addr)
        .into_client_request()
        .expect("WebSocket request");
    request.headers_mut().insert(
        http::HeaderName::from_static("x-nodedb-database"),
        http::HeaderValue::from_str(database).expect("database header"),
    );
    tokio_tungstenite::connect_async(request)
        .await
        .expect("database-scoped WS connect")
        .0
}

pub(super) async fn next_ws_json(ws: &mut WsStream, context: &str) -> serde_json::Value {
    let message = tokio::time::timeout(WS_READ_TIMEOUT, ws.next())
        .await
        .unwrap_or_else(|_| panic!("timeout waiting for {context}"))
        .unwrap_or_else(|| panic!("WS stream ended while waiting for {context}"))
        .unwrap_or_else(|error| panic!("WS error while waiting for {context}: {error}"));
    let Message::Text(text) = message else {
        panic!("expected Text frame while waiting for {context}, got {message:?}");
    };
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("invalid JSON while waiting for {context}: {error}"))
}

/// The next JSON frame, however long it takes. The caller bounds the wait:
/// a write's change arrives once the apply loop settles its entry.
pub(super) async fn next_ws_json_patient(ws: &mut WsStream) -> serde_json::Value {
    let message = ws
        .next()
        .await
        .expect("WS stream ended while waiting for a frame")
        .unwrap_or_else(|error| panic!("WS error while waiting for a frame: {error}"));
    let Message::Text(text) = message else {
        panic!("expected a Text frame, got {message:?}");
    };
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("invalid JSON frame: {error}"))
}

pub(super) async fn read_auth_exchange(
    ws: &mut WsStream,
    auth_id: u64,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    let mut notifications = Vec::new();
    for _ in 0..3 {
        let message = next_ws_json(ws, "auth exchange").await;
        if message["id"] == auth_id {
            return (message, notifications);
        }
        assert_eq!(
            message["method"], "change",
            "auth exchange may contain only change notifications before its response: {message}"
        );
        notifications.push(message);
    }
    panic!("auth response {auth_id} was not received within the bounded auth exchange");
}

pub(super) async fn send_auth(ws: &mut WsStream, id: u64, session_id: &str, cursor: Option<&str>) {
    let mut params = serde_json::json!({"session_id": session_id});
    if let Some(cursor) = cursor {
        params["cursor"] = serde_json::Value::String(cursor.to_owned());
    }
    ws.send(Message::Text(
        serde_json::json!({"id": id, "method": "auth", "params": params})
            .to_string()
            .into(),
    ))
    .await
    .expect("send auth");
}

pub(super) async fn assert_no_ws_message(ws: &mut WsStream, context: &str) {
    match tokio::time::timeout(Duration::from_millis(150), ws.next()).await {
        Err(_) => {}
        Ok(Some(Ok(message))) => panic!("unexpected WS frame after {context}: {message:?}"),
        Ok(Some(Err(error))) => panic!("WS error after {context}: {error}"),
        Ok(None) => panic!("WS stream ended unexpectedly after {context}"),
    }
}

pub(super) fn assert_error_contains(response: &serde_json::Value, expected: &str) {
    let error = response["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected error response, got: {response}"));
    assert!(
        error.contains(expected),
        "expected error containing {expected:?}, got {error:?}"
    );
}
