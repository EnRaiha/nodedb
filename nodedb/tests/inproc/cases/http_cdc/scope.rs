// SPDX-License-Identifier: BUSL-1.1

//! CDC poll results stay inside the selected database and the caller's
//! tenant.

use nodedb::config::auth::AuthMode;
use nodedb::control::security::identity::Role;
use nodedb::types::{DatabaseId, TenantId};

use crate::cases::http_support::{
    await_orders_published, create_api_key, create_orders, insert_orders, publish_orders,
    start_http,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_isolates_events_by_selected_database_and_prefers_header() {
    let srv = start_http(AuthMode::Trust).await;
    create_orders(&srv).await;
    publish_orders(&srv, &["default-database-order"], 1).await;
    srv.node
        .exec("CREATE DATABASE cdc_second_database")
        .await
        .expect("create the second database");
    let second_database = srv
        .node
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name("cdc_second_database")
        .expect("read the catalog")
        .expect("the second database exists");
    srv.node
        .exec("USE DATABASE cdc_second_database")
        .await
        .expect("use the second database");
    create_orders(&srv).await;
    insert_orders(&srv, &["second-database-order"]).await;
    await_orders_published(&srv, TenantId::new(1), second_database, 1).await;

    let client = reqwest::Client::new();
    let default_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0",
            srv.local_addr
        ))
        .send()
        .await
        .expect("poll default CDC database");
    let default_status = default_response.status();
    let default_text = default_response
        .text()
        .await
        .expect("read default CDC response");
    assert_eq!(
        default_status,
        reqwest::StatusCode::OK,
        "unexpected default CDC response: {default_text}"
    );
    let default_body: serde_json::Value =
        serde_json::from_str(&default_text).expect("parse default CDC response");
    assert_eq!(
        default_body["changes"]
            .as_array()
            .expect("default changes array")
            .iter()
            .map(|change| change["document_id"].as_str().expect("document id"))
            .collect::<Vec<_>>(),
        vec!["default-database-order"],
        "the default database poll must not expose the second database event: {default_body}"
    );

    let second_response = client
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0&database=does_not_exist",
            srv.local_addr
        ))
        .header("X-NodeDB-Database", "cdc_second_database")
        .send()
        .await
        .expect("poll second CDC database");
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = second_response
        .json()
        .await
        .expect("parse second CDC response");
    assert_eq!(
        second_body["changes"]
            .as_array()
            .expect("second changes array")
            .iter()
            .map(|change| change["document_id"].as_str().expect("document id"))
            .collect::<Vec<_>>(),
        vec!["second-database-order"],
        "the database header must override the query parameter and isolate CDC events: {second_body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_poll_excludes_matching_events_from_other_tenants() {
    let srv = start_http(AuthMode::Password).await;
    let token = create_api_key(
        &srv.node.shared,
        "cdc_tenant_one_reader",
        vec![Role::ReadOnly],
    );

    create_orders(&srv).await;
    publish_orders(&srv, &["tenant-one-order"], 1).await;
    srv.node
        .exec("CREATE TENANT cdc_other_tenant ID 2")
        .await
        .expect("create tenant 2");
    srv.node
        .exec("SET TENANT = 2")
        .await
        .expect("act as tenant 2");
    create_orders(&srv).await;
    insert_orders(&srv, &["tenant-two-order"]).await;
    srv.node
        .exec("RESET TENANT")
        .await
        .expect("act as tenant 1");
    await_orders_published(&srv, TenantId::new(2), DatabaseId::DEFAULT, 1).await;

    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/v1/cdc/orders/poll?since_ms=0",
            srv.local_addr
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("poll CDC changes as tenant-one reader");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.expect("parse CDC poll response");
    let changes = body["changes"].as_array().expect("CDC changes array");
    let document_ids: Vec<_> = changes
        .iter()
        .map(|change| change["document_id"].as_str().expect("document id"))
        .collect();

    assert!(
        document_ids.contains(&"tenant-one-order"),
        "the authorized tenant's event must remain visible: {body}"
    );
    assert_eq!(
        document_ids,
        vec!["tenant-one-order"],
        "CDC poll must return only the authorized tenant's event, not another tenant's event with the same collection and timestamp: {body}"
    );
}
