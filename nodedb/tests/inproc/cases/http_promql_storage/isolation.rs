// SPDX-License-Identifier: BUSL-1.1

//! Database scoping, collection permissions, row-level security and the
//! node counters on the PromQL read endpoints.

use nodedb::control::security::identity::{Permission, Role};
use nodedb::control::security::permission::collection_target;
use nodedb::types::{DatabaseId, TenantId};
use nodedb_test_support::pgwire_harness::TestServer;

use super::support::{
    As, TENANT, api_key, base_ms, database_id, get_json, instant_value, remote_write, result, secs,
    series,
};

async fn write_up(srv: &TestServer, database: &str, value: f64, t0: i64) {
    let (status, body) = remote_write(
        srv,
        As {
            token: None,
            database: Some(database),
        },
        vec![series(
            &[("__name__", "up"), ("job", "api")],
            &[(t0, value)],
        )],
    )
    .await;
    assert_eq!(
        status,
        reqwest::StatusCode::NO_CONTENT,
        "write {database}: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn databases_are_isolated() {
    let srv = TestServer::start().await;
    srv.exec("CREATE DATABASE promql_db_a")
        .await
        .expect("create db a");
    srv.exec("CREATE DATABASE promql_db_b")
        .await
        .expect("create db b");
    let t0 = base_ms();
    write_up(&srv, "promql_db_a", 1.0, t0).await;
    write_up(&srv, "promql_db_b", 2.0, t0).await;
    let (status, body) = remote_write(
        &srv,
        As {
            token: None,
            database: Some("promql_db_a"),
        },
        vec![series(&[("__name__", "only_in_a")], &[(t0, 9.0)])],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT, "{body}");

    for (database, expected) in [("promql_db_a", 1.0), ("promql_db_b", 2.0)] {
        let (status, json) = get_json(
            &srv,
            As {
                token: None,
                database: Some(database),
            },
            "query",
            &[("query", "up".to_owned()), ("time", secs(t0))],
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK, "{database}: {json}");
        let vector = result(&json);
        assert_eq!(vector.len(), 1, "{database}: {json}");
        assert_eq!(instant_value(&vector[0]), expected, "{database}");
    }

    let (status, json) = get_json(
        &srv,
        As {
            token: None,
            database: Some("promql_db_b"),
        },
        "label/__name__/values",
        &[],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    let names: Vec<&str> = json["data"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(names.contains(&"up"), "{json}");
    assert!(
        !names.contains(&"only_in_a"),
        "db a's metric leaked into db b: {json}"
    );

    let db_a = database_id(&srv.shared, "promql_db_a");
    let token = api_key(
        &srv.shared,
        "promql_db_a_reader",
        vec![Role::DatabaseReader(db_a)],
        vec![db_a],
    );
    let (status, json) = get_json(
        &srv,
        As {
            token: Some(&token),
            database: Some("promql_db_b"),
        },
        "query",
        &[("query", "up".to_owned()), ("time", secs(t0))],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::FORBIDDEN, "{json}");

    let (status, json) = get_json(
        &srv,
        As {
            token: Some(&token),
            database: Some("promql_db_a"),
        },
        "query",
        &[("query", "up".to_owned()), ("time", secs(t0))],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    assert_eq!(instant_value(&result(&json)[0]), 1.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_permission_and_rls_apply() {
    let srv = TestServer::start().await;
    let t0 = base_ms();
    let (status, body) = remote_write(
        &srv,
        As::default(),
        vec![
            series(&[("__name__", "guarded"), ("job", "api")], &[(t0, 1.0)]),
            series(&[("__name__", "guarded"), ("job", "web")], &[(t0, 2.0)]),
        ],
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT, "{body}");

    let username = "promql_guarded_reader";
    let token = api_key(
        &srv.shared,
        username,
        vec![Role::Custom("promql_guarded_role".into())],
        vec![DatabaseId::DEFAULT],
    );
    let query = [("query", "guarded".to_owned()), ("time", secs(t0))];
    let ctx = As {
        token: Some(&token),
        database: None,
    };

    let (status, json) = get_json(&srv, ctx, "query", &query).await;
    assert_eq!(
        status,
        reqwest::StatusCode::FORBIDDEN,
        "no grant yet: {json}"
    );

    srv.shared
        .permissions
        .grant(
            &collection_target(DatabaseId::DEFAULT, TenantId::new(TENANT), "guarded"),
            &format!("user:{username}"),
            Permission::Read,
            "nodedb",
            Some(srv.shared.credentials.catalog()),
        )
        .expect("grant read on guarded");
    srv.exec("CREATE RLS POLICY guarded_api_only ON guarded FOR READ USING (job = 'api')")
        .await
        .expect("create read policy");

    let (status, json) = get_json(&srv, ctx, "query", &query).await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    let vector = result(&json);
    assert_eq!(vector.len(), 1, "the policy admits job=api only: {json}");
    assert_eq!(vector[0]["metric"]["job"], "api");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_counters_need_monitor() {
    let srv = TestServer::start().await;
    let query = [("query", "nodedb_queries_total".to_owned())];

    let reader = api_key(
        &srv.shared,
        "promql_plain_reader",
        vec![Role::ReadOnly],
        vec![DatabaseId::DEFAULT],
    );
    let (status, json) = get_json(
        &srv,
        As {
            token: Some(&reader),
            database: None,
        },
        "query",
        &query,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    assert!(result(&json).is_empty(), "counters leaked: {json}");

    let monitor = api_key(
        &srv.shared,
        "promql_monitor",
        vec![Role::Monitor],
        vec![DatabaseId::DEFAULT],
    );
    let (status, json) = get_json(
        &srv,
        As {
            token: Some(&monitor),
            database: None,
        },
        "query",
        &query,
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{json}");
    assert_eq!(result(&json).len(), 1, "{json}");
}
