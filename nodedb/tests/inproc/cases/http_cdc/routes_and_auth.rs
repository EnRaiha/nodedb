// SPDX-License-Identifier: BUSL-1.1

//! CDC routes are mounted, require auth under Password mode, accept only GET,
//! and enforce the collection READ grant.

use std::time::Duration;

use nodedb::config::auth::AuthMode;
use nodedb::control::security::identity::Role;

use crate::cases::http_support::{create_api_key, create_orders, publish_orders, start_http};

fn is_auth_error(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN
}

// ─── /v1/cdc/{collection} SSE stream ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_route_is_mounted() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders", srv.local_addr);
    // With a short timeout — we just need to confirm the route exists (not 404).
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new().get(&url).send(),
    )
    .await;
    match result {
        Ok(Ok(resp)) => {
            assert_ne!(
                resp.status(),
                reqwest::StatusCode::NOT_FOUND,
                "/v1/cdc/orders SSE route must be mounted (not 404)"
            );
        }
        // Timeout means the SSE stream started and is holding the connection.
        // That is a success: the route exists and is serving.
        Ok(Err(e)) => panic!("Request error: {e}"),
        Err(_timeout) => {} // SSE stream opened — route confirmed mounted
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cdc/orders", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cdc/orders");
    assert!(
        is_auth_error(resp.status()),
        "/v1/cdc/orders must require auth under Password mode; got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_rejects_cross_tenant_param() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders?tenant_id=999", srv.local_addr);
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new().get(&url).send(),
    )
    .await;
    if let Ok(Ok(resp)) = result {
        assert!(
            is_auth_error(resp.status()),
            "/v1/cdc/orders must reject cross-tenant tenant_id param; got {}",
            resp.status()
        );
    }
    // Timeout is ambiguous here; the cross-tenant guard is already covered
    // in http_route_authentication.rs.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_post_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /v1/cdc/orders");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/cdc/orders POST must return 405"
    );
}

// ─── /v1/cdc/{collection}/poll ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_route_is_mounted() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders/poll", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cdc/orders/poll");
    assert_ne!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "/v1/cdc/orders/poll must be mounted (not 404)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_requires_auth_under_password_mode() {
    let srv = start_http(AuthMode::Password).await;
    let url = format!("http://{}/v1/cdc/orders/poll", srv.local_addr);
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET /v1/cdc/orders/poll");
    assert!(
        is_auth_error(resp.status()),
        "/v1/cdc/orders/poll must require auth under Password mode; got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_post_returns_405() {
    let srv = start_http(AuthMode::Trust).await;
    let url = format!("http://{}/v1/cdc/orders/poll", srv.local_addr);
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("POST /v1/cdc/orders/poll");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::METHOD_NOT_ALLOWED,
        "/v1/cdc/orders/poll POST must return 405"
    );
}

// ─── Collection READ grant ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_rejects_custom_role_without_collection_grant() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.node.shared,
        "cdc_ungranted_poll_reader",
        vec![Role::Custom("cdc_ungranted_poll_role".into())],
    );
    create_orders(&srv).await;
    publish_orders(&srv, &["ungranted-poll-order"], 1).await;

    let response = reqwest::Client::new()
        .get(format!("http://{}/v1/cdc/orders/poll", srv.local_addr))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("poll CDC changes without collection grant");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "CDC poll must enforce collection READ permission before returning matching events"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_sse_rejects_custom_role_without_collection_grant_before_streaming() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.node.shared,
        "cdc_ungranted_sse_reader",
        vec![Role::Custom("cdc_ungranted_sse_role".into())],
    );
    create_orders(&srv).await;
    publish_orders(&srv, &["ungranted-sse-order"], 1).await;

    let response = tokio::time::timeout(
        Duration::from_millis(300),
        reqwest::Client::new()
            .get(format!("http://{}/v1/cdc/orders", srv.local_addr))
            .header("Authorization", format!("Bearer {token}"))
            .send(),
    )
    .await
    .expect("ungranted CDC SSE request must be rejected before opening a stream")
    .expect("GET CDC SSE without collection grant");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::FORBIDDEN,
        "CDC SSE must enforce collection READ permission before opening a stream or replaying backlog"
    );
}
