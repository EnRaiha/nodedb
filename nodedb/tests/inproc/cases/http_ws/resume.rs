// SPDX-License-Identifier: BUSL-1.1

//! Resume auth replays the selected database's events in publication order
//! behind opaque cursors, and rejects bad cursors.

use std::time::Duration;

use futures::SinkExt;
use nodedb::config::auth::AuthMode;
use nodedb::types::{DatabaseId, TenantId};
use tokio_tungstenite::tungstenite::Message;

use crate::cases::http_support::{
    await_orders_published, create_orders, insert_orders, start_http,
};
use crate::cases::http_ws_support::{
    assert_error_contains, assert_no_ws_message, connect_ws, connect_ws_in_database, next_ws_json,
    next_ws_json_patient, read_auth_exchange, send_auth,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_replay_isolates_events_by_selected_database() {
    let srv = start_http(AuthMode::Trust).await;
    create_orders(&srv).await;
    insert_orders(&srv, &["default-database-order"]).await;
    srv.node
        .exec("CREATE DATABASE ws_second_database")
        .await
        .expect("create the second database");
    let second_database = srv
        .node
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name("ws_second_database")
        .expect("read the catalog")
        .expect("the second database exists");
    srv.node
        .exec("USE DATABASE ws_second_database")
        .await
        .expect("use the second database");
    create_orders(&srv).await;
    insert_orders(&srv, &["second-database-order"]).await;
    await_orders_published(&srv, TenantId::new(1), DatabaseId::DEFAULT, 1).await;
    await_orders_published(&srv, TenantId::new(1), second_database, 1).await;

    let mut ws = connect_ws_in_database(&srv, "ws_second_database").await;
    send_auth(&mut ws, 1, "database-scoped-resume", None).await;
    let (response, notifications) = read_auth_exchange(&mut ws, 1).await;

    assert_eq!(response["result"]["replayed"], 1);
    assert_eq!(notifications.len(), 1, "only the selected database replays");
    assert_eq!(
        notifications[0]["params"]["document_id"],
        "second-database-order"
    );
    assert_eq!(
        notifications[0]["params"]["database_id"],
        second_database.as_u64(),
        "the replay notification must carry the selected database identity"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_replays_in_publication_order_with_opaque_cursors() {
    let srv = start_http(AuthMode::Trust).await;
    create_orders(&srv).await;
    insert_orders(&srv, &["published-first", "published-second"]).await;
    await_orders_published(&srv, TenantId::new(1), DatabaseId::DEFAULT, 2).await;

    let mut ws = connect_ws(&srv).await;
    send_auth(&mut ws, 1, "resume-publication-order", None).await;
    let (response, notifications) = read_auth_exchange(&mut ws, 1).await;

    assert_eq!(response["result"]["session_id"], "resume-publication-order");
    assert_eq!(response["result"]["replayed"], 2);
    let snapshot_cursor = response["result"]["snapshot_cursor"]
        .as_str()
        .expect("auth response must include snapshot_cursor");
    assert!(
        snapshot_cursor.starts_with("v2:"),
        "snapshot_cursor must be an opaque versioned cursor: {response}"
    );
    assert_eq!(notifications.len(), 2, "both pre-auth events must replay");
    assert!(
        notifications
            .iter()
            .all(|notification| notification["params"]["wal_lsn"].is_u64()),
        "every replayed change carries its WAL LSN: {notifications:?}"
    );
    assert_eq!(notifications[0]["params"]["document_id"], "published-first");
    assert_eq!(
        notifications[1]["params"]["document_id"],
        "published-second"
    );
    let first_cursor = notifications[0]["params"]["cursor"]
        .as_str()
        .expect("first replay must include an opaque cursor");
    let second_cursor = notifications[1]["params"]["cursor"]
        .as_str()
        .expect("second replay must include an opaque cursor");
    assert!(first_cursor.starts_with("v2:"));
    assert!(second_cursor.starts_with("v2:"));
    assert_ne!(
        first_cursor, second_cursor,
        "each publication must expose a distinct opaque cursor"
    );
    assert_no_ws_message(&mut ws, "the complete initial replay").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_cursor_controls_reconnect_progress_and_handoffs_to_live_events() {
    let srv = start_http(AuthMode::Trust).await;
    create_orders(&srv).await;
    insert_orders(&srv, &["first-replayed-event", "second-replayed-event"]).await;
    await_orders_published(&srv, TenantId::new(1), DatabaseId::DEFAULT, 2).await;
    let session_id = "client-controlled-resume-session";

    let mut first_connection = connect_ws(&srv).await;
    send_auth(&mut first_connection, 1, session_id, None).await;
    let (_first_response, first_notifications) = read_auth_exchange(&mut first_connection, 1).await;
    assert_eq!(first_notifications.len(), 2);
    let first_cursor = first_notifications[0]["params"]["cursor"]
        .as_str()
        .expect("first delivered event must include cursor")
        .to_owned();
    drop(first_connection);

    let mut resumed_connection = connect_ws(&srv).await;
    send_auth(&mut resumed_connection, 2, session_id, Some(&first_cursor)).await;
    let (response, notifications) = read_auth_exchange(&mut resumed_connection, 2).await;
    assert_eq!(response["result"]["session_id"], session_id);
    assert_eq!(response["result"]["replayed"], 1);
    let snapshot_cursor = response["result"]["snapshot_cursor"]
        .as_str()
        .expect("reconnect auth response must include snapshot_cursor")
        .to_owned();
    assert_eq!(
        notifications.len(),
        1,
        "the supplied client cursor must suppress only the first publication"
    );
    assert_eq!(
        notifications[0]["params"]["document_id"],
        "second-replayed-event"
    );
    assert_no_ws_message(&mut resumed_connection, "cursor-limited reconnect replay").await;

    // A write after the auth snapshot must flow through the live handoff.
    insert_orders(&srv, &["published-after-auth-snapshot"]).await;
    let live_notification = tokio::time::timeout(
        Duration::from_secs(5),
        next_ws_json_patient(&mut resumed_connection),
    )
    .await
    .expect("timeout waiting for the post-snapshot live change");
    assert_eq!(live_notification["method"], "change");
    assert!(live_notification["params"]["wal_lsn"].is_u64());
    assert_eq!(
        live_notification["params"]["document_id"],
        "published-after-auth-snapshot"
    );
    let live_cursor = live_notification["params"]["cursor"]
        .as_str()
        .expect("post-snapshot change must include cursor");
    assert!(live_cursor.starts_with("v2:"));
    assert_ne!(
        live_cursor, snapshot_cursor,
        "a post-snapshot publication must receive a later opaque cursor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_auth_rejects_legacy_and_malformed_cursors_and_second_auth() {
    let srv = start_http(AuthMode::Trust).await;
    let mut ws = connect_ws(&srv).await;

    ws.send(Message::Text(
        serde_json::json!({
            "id": 1,
            "method": "auth",
            "params": {"session_id": "cursor-validation", "last_lsn": 101}
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send legacy last_lsn auth");
    assert_error_contains(
        &next_ws_json(&mut ws, "legacy last_lsn rejection").await,
        "last_lsn is no longer supported",
    );

    send_auth(&mut ws, 2, "cursor-validation", Some("not-a-valid-cursor")).await;
    assert_error_contains(
        &next_ws_json(&mut ws, "malformed cursor rejection").await,
        "cursor must be a valid opaque change cursor",
    );

    send_auth(&mut ws, 3, "cursor-validation", None).await;
    let (accepted, notifications) = read_auth_exchange(&mut ws, 3).await;
    assert!(
        notifications.is_empty(),
        "empty stream must not replay changes"
    );
    assert!(
        accepted.get("result").is_some(),
        "first valid auth must succeed"
    );

    send_auth(&mut ws, 4, "cursor-validation", None).await;
    assert_error_contains(
        &next_ws_json(&mut ws, "second auth rejection").await,
        "resume auth is permitted only once per connection",
    );
}
