// SPDX-License-Identifier: BUSL-1.1

//! Opaque cursors page and resume CDC poll and SSE streams without a repeat
//! or a gap.

use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb::control::security::identity::Role;

use crate::cases::http_support::{
    commit_orders, create_api_key, create_orders, publish_orders, start_http,
};

fn opaque_cursor(body: &serde_json::Value) -> String {
    let cursor = body["next_cursor"]["cursor"]
        .as_str()
        .expect("CDC page must return next_cursor.cursor as a string");
    assert!(
        !cursor.is_empty(),
        "CDC next_cursor.cursor must be a nonempty opaque token: {body}"
    );
    cursor.to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_paginates_same_timestamp_with_opaque_cursor() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(&srv.node.shared, "cdc_cursor_reader", vec![Role::ReadOnly]);
    create_orders(&srv).await;
    commit_orders(
        &srv,
        &["cursor-order-1", "cursor-order-2", "cursor-order-3"],
        3,
    )
    .await;

    let client = reqwest::Client::new();
    let first_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("first CDC poll page");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: serde_json::Value = first_response
        .json()
        .await
        .expect("parse first CDC poll page");
    let first_changes = first_body["changes"]
        .as_array()
        .expect("first changes array");
    assert_eq!(first_changes.len(), 1, "limit=1 must produce one change");
    assert_eq!(
        first_changes[0]["document_id"], "cursor-order-1",
        "first page must contain the first event"
    );
    assert_eq!(
        first_body["has_more"], true,
        "a one-item page before additional matching events must report has_more"
    );
    let cursor = opaque_cursor(&first_body);

    let second_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("second CDC poll page");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response
        .json()
        .await
        .expect("parse second CDC poll page");
    let second_changes = second_body["changes"]
        .as_array()
        .expect("second changes array");
    assert_eq!(
        second_changes.len(),
        1,
        "second limit=1 page must have one change"
    );
    assert_eq!(
        second_changes[0]["document_id"], "cursor-order-2",
        "the opaque cursor must advance to the next same-millisecond event instead of replaying the first"
    );
    assert_ne!(
        second_changes[0]["document_id"], "cursor-order-1",
        "the cursor page must not replay the first event"
    );
}

/// A cursor taken before a write resumes at that write's event: the cursor
/// names a feed position, so a later publication is never skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_opaque_cursor_resumes_at_an_event_published_after_it() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.node.shared,
        "cdc_later_publication_reader",
        vec![Role::ReadOnly],
    );
    create_orders(&srv).await;
    publish_orders(&srv, &["published-first"], 1).await;

    let client = reqwest::Client::new();
    let first_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("first CDC page before the later write");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: serde_json::Value = first_response.json().await.expect("parse first page");
    assert_eq!(
        first_body["changes"][0]["document_id"], "published-first",
        "the first page must return the event that was published first"
    );
    let first_cursor = opaque_cursor(&first_body);

    publish_orders(&srv, &["published-after-the-cursor"], 2).await;

    let second_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", first_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("second CDC page after the later write");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response.json().await.expect("parse second page");
    assert_eq!(
        second_body["changes"][0]["document_id"], "published-after-the-cursor",
        "the cursor must not omit an event published after it was taken"
    );
    let second_cursor = opaque_cursor(&second_body);

    let final_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", second_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("final CDC page");
    assert_eq!(final_response.status(), reqwest::StatusCode::OK);
    let final_body: serde_json::Value = final_response.json().await.expect("parse final page");
    assert_eq!(
        final_body["changes"]
            .as_array()
            .expect("final changes array")
            .len(),
        0,
        "opaque cursor pagination must neither repeat nor omit either event"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_opaque_cursor_paginates_events_sharing_one_lsn() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.node.shared,
        "cdc_duplicate_lsn_reader",
        vec![Role::ReadOnly],
    );
    create_orders(&srv).await;
    commit_orders(&srv, &["shared-lsn-first", "shared-lsn-second"], 2).await;

    let client = reqwest::Client::new();
    let first_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("first shared-LSN CDC page");
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_body: serde_json::Value = first_response.json().await.expect("parse first page");
    assert_eq!(first_body["changes"][0]["document_id"], "shared-lsn-first");
    let first_cursor = opaque_cursor(&first_body);

    let second_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", first_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("second shared-LSN CDC page");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response.json().await.expect("parse second page");
    assert_eq!(
        second_body["changes"][0]["document_id"],
        "shared-lsn-second"
    );
    assert_ne!(
        second_body["changes"][0]["document_id"], first_body["changes"][0]["document_id"],
        "the cursor must not repeat the first event when both events share an LSN"
    );
    let second_cursor = opaque_cursor(&second_body);

    let final_response = client
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", second_cursor.as_str()), ("limit", "1")])
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("final shared-LSN CDC page");
    assert_eq!(final_response.status(), reqwest::StatusCode::OK);
    let final_body: serde_json::Value = final_response.json().await.expect("parse final page");
    assert!(
        final_body["changes"]
            .as_array()
            .expect("final changes array")
            .is_empty(),
        "each event sharing an LSN must be returned exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_rejects_malformed_opaque_cursor() {
    let srv = start_http(AuthMode::Trust).await;
    let response = reqwest::Client::new()
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .query(&[("cursor", "not-a-valid-opaque-cursor")])
        .send()
        .await
        .expect("poll CDC with malformed cursor");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "malformed opaque CDC cursors must be rejected"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_rejects_legacy_since_lsn_continuation() {
    let srv = start_http(AuthMode::Trust).await;
    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_lsn=603",
            srv.local_addr
        ))
        .send()
        .await
        .expect("poll CDC with legacy since_lsn continuation");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "legacy scalar since_lsn continuations must be rejected in favor of opaque cursors"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_limit_zero_clamps_to_a_nonempty_page() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.node.shared,
        "cdc_zero_limit_reader",
        vec![Role::ReadOnly],
    );
    create_orders(&srv).await;
    publish_orders(&srv, &["zero-limit-order"], 1).await;

    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0&limit=0",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("CDC poll with limit=0");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("parse CDC poll response");
    let changes = body["changes"].as_array().expect("CDC changes array");
    assert_eq!(
        changes.len(),
        1,
        "limit=0 must clamp to a positive page size for the one-event fixture: {body}"
    );
    assert!(
        !changes.is_empty() || body["has_more"].as_bool() != Some(true),
        "CDC poll must not report an empty page with has_more=true for limit=0: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_last_event_id_replays_only_events_after_the_cursor() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.node.shared,
        "cdc_sse_cursor_reader",
        vec![Role::ReadOnly],
    );
    create_orders(&srv).await;
    publish_orders(&srv, &["sse-cursor-first", "sse-cursor-second"], 2).await;

    let client = reqwest::Client::new();
    let poll_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0&limit=1",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("poll first CDC event to obtain its opaque cursor");
    assert_eq!(poll_response.status(), reqwest::StatusCode::OK);
    let poll_body: serde_json::Value = poll_response
        .json()
        .await
        .expect("parse first CDC poll page");
    let poll_changes = poll_body["changes"]
        .as_array()
        .expect("first CDC poll changes array");
    assert_eq!(poll_changes.len(), 1, "limit=1 must return one event");
    assert_eq!(poll_changes[0]["document_id"], "sse-cursor-first");
    let cursor = opaque_cursor(&poll_body);

    let mut response = client
        .get(format!("http://{}/v1/cdc/orders", srv.local_addr))
        .header("Authorization", format!("Bearer {token}"))
        .header("Last-Event-ID", cursor)
        .send()
        .await
        .expect("CDC SSE request with Last-Event-ID");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let chunk = tokio::time::timeout(Duration::from_millis(300), response.chunk())
        .await
        .expect("timed out waiting for first replayed SSE event")
        .expect("SSE response body error")
        .expect("SSE stream ended before replaying an event");
    let event = std::str::from_utf8(&chunk).expect("SSE event must be UTF-8");
    assert!(
        event.contains("sse-cursor-second"),
        "Last-Event-ID replay must start after the cursor: {event}"
    );
    assert!(
        !event.contains("sse-cursor-first"),
        "Last-Event-ID replay must not include the cursor event: {event}"
    );
}
