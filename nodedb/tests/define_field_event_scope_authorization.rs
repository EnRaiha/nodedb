// SPDX-License-Identifier: BUSL-1.1

//! DEFINE FIELD / DEFINE EVENT must authorize ALTER and mutate only the
//! collection in the selected database.

mod common;

use common::pgwire_harness::TestServer;
use nodedb::types::DatabaseId;

fn assert_insufficient_privilege(error: tokio_postgres::Error) {
    assert_eq!(
        error
            .as_db_error()
            .expect("server must return a SQLSTATE")
            .code()
            .code(),
        "42501",
        "DEFINE FIELD/EVENT must require ALTER permission"
    );
}

#[tokio::test]
async fn define_field_and_event_require_alter_and_use_selected_database() {
    const COLLECTION: &str = "field_event_scoped";
    const DATABASE: &str = "field_event_non_default";
    const ROLE: &str = "field_event_denied_role";
    const USER: &str = "field_event_denied_user";

    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("create default collection");
    server
        .exec(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create non-default database");
    server
        .exec(&format!("USE DATABASE {DATABASE}"))
        .await
        .expect("select non-default database");
    server
        .exec(&format!("CREATE COLLECTION {COLLECTION}"))
        .await
        .expect("create non-default collection");
    server
        .exec("USE DATABASE default")
        .await
        .expect("return to default database");
    server
        .exec(&format!("CREATE ROLE {ROLE}"))
        .await
        .expect("create custom role");
    server
        .exec(&format!("CREATE USER {USER} WITH PASSWORD 'x' ROLE {ROLE}"))
        .await
        .expect("create custom-role user");
    server
        .exec(&format!("GRANT ALL ON DATABASE {DATABASE} TO {USER}"))
        .await
        .expect("grant non-default database access without collection ALTER");

    let (default_client, _default_connection) = server
        .connect_as(USER, "x")
        .await
        .expect("connect custom-role user to default database");
    assert_insufficient_privilege(
        default_client
            .simple_query(&format!(
                "DEFINE FIELD denied_default ON {COLLECTION} TYPE text"
            ))
            .await
            .expect_err("custom role without ALTER must be denied in default database"),
    );
    assert_insufficient_privilege(
        default_client
            .simple_query(&format!(
                "DEFINE EVENT denied_default ON {COLLECTION} WHEN true THEN SELECT 1"
            ))
            .await
            .expect_err("custom role without ALTER must not define default event"),
    );

    let (non_default_client, _non_default_connection) = server
        .connect_as_database(USER, "x", DATABASE)
        .await
        .expect("connect custom-role user to granted non-default database");
    assert_insufficient_privilege(
        non_default_client
            .simple_query(&format!(
                "DEFINE FIELD denied_non_default ON {COLLECTION} TYPE text"
            ))
            .await
            .expect_err("custom role without ALTER must be denied in non-default database"),
    );
    assert_insufficient_privilege(
        non_default_client
            .simple_query(&format!(
                "DEFINE EVENT denied_non_default ON {COLLECTION} WHEN true THEN SELECT 1"
            ))
            .await
            .expect_err("custom role without ALTER must not define non-default event"),
    );

    let catalog = server.shared.credentials.catalog();
    let database_id = catalog
        .get_database_id_by_name(DATABASE)
        .expect("look up non-default database")
        .expect("created database descriptor");
    for id in [DatabaseId::DEFAULT, database_id] {
        let collection = catalog
            .get_collection(id, 1, COLLECTION)
            .expect("read collection catalog entry")
            .expect("created collection catalog entry");
        assert!(
            collection.field_defs.is_empty() && collection.event_defs.is_empty(),
            "denied DEFINE FIELD/EVENT must not mutate database {}: {collection:?}",
            id.as_u64()
        );
    }

    server
        .exec(&format!("GRANT ALTER ON {COLLECTION} TO {ROLE}"))
        .await
        .expect("grant ALTER to custom role");
    non_default_client
        .simple_query(&format!(
            "DEFINE FIELD authorized_field ON {COLLECTION} TYPE text"
        ))
        .await
        .expect("authorized non-default DEFINE FIELD");
    non_default_client
        .simple_query(&format!(
            "DEFINE EVENT authorized_event ON {COLLECTION} WHEN true THEN SELECT 1"
        ))
        .await
        .expect("authorized non-default DEFINE EVENT");

    let default_collection = catalog
        .get_collection(DatabaseId::DEFAULT, 1, COLLECTION)
        .expect("read default collection")
        .expect("default collection exists");
    assert!(
        default_collection.field_defs.is_empty() && default_collection.event_defs.is_empty(),
        "authorized non-default definitions must not modify same-named default collection: {default_collection:?}"
    );

    let non_default_collection = catalog
        .get_collection(database_id, 1, COLLECTION)
        .expect("read non-default collection")
        .expect("non-default collection exists");
    assert!(
        non_default_collection
            .field_defs
            .iter()
            .any(|field| field.name == "authorized_field"),
        "authorized DEFINE FIELD must modify the selected non-default collection"
    );
    assert!(
        non_default_collection
            .event_defs
            .iter()
            .any(|event| event.name == "authorized_event"),
        "authorized DEFINE EVENT must modify the selected non-default collection"
    );
}
