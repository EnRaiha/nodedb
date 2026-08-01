// SPDX-License-Identifier: BUSL-1.1

//! Authorization parity for native-protocol SQL execution.
//!
//! Native materialized and lazy SQL paths must apply the same collection-
//! permission gates as pgwire before dispatch.

mod common;

use common::native_harness::{
    NativeTestServer, do_handshake, send_api_key_auth, send_request, send_sql,
};
use nodedb::control::security::apikey::CreateKeyParams;
use nodedb::control::security::identity::Role;
use nodedb::control::state::SharedState;
use nodedb::types::{DatabaseId, TenantId};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, OpCode};

fn create_api_key(shared: &SharedState, username: &str, roles: Vec<Role>) -> String {
    create_api_key_with_databases(shared, username, roles, vec![DatabaseId::DEFAULT], None)
}

fn create_api_key_with_databases(
    shared: &SharedState,
    username: &str,
    roles: Vec<Role>,
    accessible_databases: Vec<DatabaseId>,
    default_database: Option<DatabaseId>,
) -> String {
    let user_id = if username == "nodedb" {
        shared
            .credentials
            .get_user(username)
            .expect("harness superuser")
            .user_id
    } else {
        shared
            .credentials
            .create_service_account(
                username,
                TenantId::new(1),
                roles,
                accessible_databases.clone(),
            )
            .expect("create native service account")
    };

    if let Some(default_database) = default_database {
        let stored = shared
            .credentials
            .prepare_set_default_database(username, default_database.as_u64())
            .expect("set service account default database");
        shared
            .credentials
            .catalog()
            .put_user(&stored)
            .expect("persist service account default database");
        shared.credentials.install_replicated_user(&stored, None);
    }

    shared
        .api_keys
        .create_key(
            CreateKeyParams {
                username,
                user_id,
                tenant_id: TenantId::new(1),
                expires_secs: 0,
                scope: vec![],
                accessible_databases,
            },
            Some(shared.credentials.catalog()),
        )
        .expect("create native API key")
}

async fn authenticated_stream(server: &NativeTestServer, token: String) -> tokio::net::TcpStream {
    let (mut stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_api_key_auth(&mut stream, 1, token).await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native API key auth");
    stream
}

async fn authenticated_stream_in_database(
    server: &NativeTestServer,
    token: String,
    database: &str,
) -> tokio::net::TcpStream {
    let (mut stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::ApiKey { token }),
            database: Some(database.into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native API key auth");
    stream
}

async fn password_authenticated_stream(
    server: &NativeTestServer,
    username: &str,
    password: &str,
    database: Option<&str>,
) -> tokio::net::TcpStream {
    let (mut stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Password {
                username: username.into(),
                password: password.into(),
            }),
            database: database.map(str::to_owned),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        auth.status,
        ResponseStatus::Ok,
        "native password auth: {auth:?}"
    );
    stream
}

async fn seed_private_collection(server: &NativeTestServer, collection: &str) {
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(server, admin_token).await;
    let create = send_sql(&mut admin, 2, &format!("CREATE COLLECTION {collection}")).await;
    assert_eq!(
        create.status,
        ResponseStatus::Ok,
        "create private collection"
    );
    let insert = send_sql(
        &mut admin,
        3,
        &format!("INSERT INTO {collection} {{ id: 'hidden', value: 17 }}"),
    )
    .await;
    assert_eq!(insert.status, ResponseStatus::Ok, "seed private collection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_materialized_sql_rejects_collection_without_permission() {
    let server = NativeTestServer::start_authenticated().await;
    seed_private_collection(&server, "native_materialized_private").await;
    let token = create_api_key(
        &server.shared,
        "native_materialized_reader",
        vec![Role::Custom("native_materialized_role".into())],
    );
    let mut stream = authenticated_stream(&server, token).await;

    let response = send_sql(
        &mut stream,
        2,
        "SELECT * FROM native_materialized_private ORDER BY id",
    )
    .await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native materialized SQL must enforce PermissionStore before dispatch"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native materialized denial must report insufficient privilege: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_rejects_system_audit_log_for_custom_role() {
    let server = NativeTestServer::start_authenticated().await;
    let token = create_api_key(
        &server.shared,
        "native_audit_log_denied",
        vec![
            Role::Custom("native_audit_log_denied_role".into()),
            Role::ReadOnly,
        ],
    );
    let mut stream = authenticated_stream(&server, token).await;

    let response = send_sql(&mut stream, 2, "SELECT * FROM _system.audit_log").await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native SQL must deny system audit-log access to a custom role"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native audit-log denial must report insufficient privilege: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_auth_rejects_database_outside_api_key_scope_before_query() {
    let server = NativeTestServer::start_authenticated().await;
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token).await;
    let create = send_sql(
        &mut admin,
        2,
        "CREATE DATABASE native_scoped_other_database",
    )
    .await;
    assert_eq!(
        create.status,
        ResponseStatus::Ok,
        "create inaccessible database"
    );
    drop(admin);

    let token = create_api_key(
        &server.shared,
        "native_default_only_database_reader",
        vec![Role::ReadOnly],
    );
    let (mut stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let response = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::ApiKey { token }),
            database: Some("native_scoped_other_database".into()),
            ..Default::default()
        },
    )
    .await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native Auth must reject an existing database outside the API key scope before any query"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native database-selection denial must report insufficient privilege: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_password_auth_uses_persisted_default_and_live_database_grants() {
    const USERNAME: &str = "native_password_database_reader";
    const PASSWORD: &str = "NativePassword!42";
    const DATABASE_A: &str = "native_password_database_a";
    const DATABASE_B: &str = "native_password_database_b";
    const DATABASE_C: &str = "native_password_database_c";

    let server = NativeTestServer::start_authenticated().await;
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token.clone()).await;
    for (seq, database) in [(2, DATABASE_A), (3, DATABASE_B), (4, DATABASE_C)] {
        let create = send_sql(&mut admin, seq, &format!("CREATE DATABASE {database}")).await;
        assert_eq!(
            create.status,
            ResponseStatus::Ok,
            "create database {database}"
        );
    }
    drop(admin);

    let catalog = server.shared.credentials.catalog();
    let db_a = catalog
        .get_database_id_by_name(DATABASE_A)
        .expect("look up database A")
        .expect("created database A descriptor");
    let db_b = catalog
        .get_database_id_by_name(DATABASE_B)
        .expect("look up database B")
        .expect("created database B descriptor");

    let database_admin_token = create_api_key_with_databases(
        &server.shared,
        "nodedb",
        vec![Role::Superuser],
        vec![db_a, db_b],
        None,
    );
    let mut admin_a =
        authenticated_stream_in_database(&server, database_admin_token.clone(), DATABASE_A).await;
    let create_a = send_sql(&mut admin_a, 2, "CREATE COLLECTION native_password_rows_a").await;
    assert_eq!(
        create_a.status,
        ResponseStatus::Ok,
        "create collection in database A"
    );
    let insert_a = send_sql(
        &mut admin_a,
        3,
        "INSERT INTO native_password_rows_a { id: 'database-a' }",
    )
    .await;
    assert_eq!(insert_a.status, ResponseStatus::Ok, "seed database A row");
    drop(admin_a);

    let mut admin_b =
        authenticated_stream_in_database(&server, database_admin_token, DATABASE_B).await;
    let create_b = send_sql(&mut admin_b, 2, "CREATE COLLECTION native_password_rows_b").await;
    assert_eq!(
        create_b.status,
        ResponseStatus::Ok,
        "create collection in database B"
    );
    let insert_b = send_sql(
        &mut admin_b,
        3,
        "INSERT INTO native_password_rows_b { id: 'database-b' }",
    )
    .await;
    assert_eq!(insert_b.status, ResponseStatus::Ok, "seed database B row");
    drop(admin_b);

    let user_id = server
        .shared
        .credentials
        .create_user(USERNAME, PASSWORD, TenantId::new(1), vec![Role::ReadOnly])
        .expect("create password user");
    let stored = server
        .shared
        .credentials
        .prepare_set_default_database(USERNAME, db_a.as_u64())
        .expect("set password user default database");
    catalog
        .put_user(&stored)
        .expect("persist password user default database");
    server
        .shared
        .credentials
        .install_replicated_user(&stored, None);
    catalog
        .put_database_grant(db_a, user_id, "CONNECT")
        .expect("grant database A");
    catalog
        .put_database_grant(db_b, user_id, "CONNECT")
        .expect("grant database B");

    let mut default_database =
        password_authenticated_stream(&server, USERNAME, PASSWORD, None).await;
    let read_a = send_sql(
        &mut default_database,
        2,
        "SELECT * FROM native_password_rows_a WHERE id = 'database-a'",
    )
    .await;
    assert_eq!(
        read_a.status,
        ResponseStatus::Ok,
        "omitted database binds persisted default A"
    );
    assert_eq!(
        read_a.rows.as_ref().map(Vec::len),
        Some(1),
        "read database A row"
    );
    drop(default_database);

    let mut selected_database =
        password_authenticated_stream(&server, USERNAME, PASSWORD, Some(DATABASE_B)).await;
    let read_b = send_sql(
        &mut selected_database,
        2,
        "SELECT * FROM native_password_rows_b WHERE id = 'database-b'",
    )
    .await;
    assert_eq!(
        read_b.status,
        ResponseStatus::Ok,
        "explicit database binds granted B"
    );
    assert_eq!(
        read_b.rows.as_ref().map(Vec::len),
        Some(1),
        "read database B row"
    );
    drop(selected_database);

    let (mut denied_stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let denied = send_request(
        &mut denied_stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Password {
                username: USERNAME.into(),
                password: PASSWORD.into(),
            }),
            database: Some(DATABASE_C.into()),
            ..Default::default()
        },
    )
    .await;
    drop(denied_stream);
    server.shutdown().await;

    assert_eq!(denied.status, ResponseStatus::Error);
    assert_eq!(
        denied.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "an explicit ungranted database must be denied during password Auth: {denied:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_automatic_trust_binds_configured_principal_default_database() {
    const DATABASE: &str = "native_automatic_trust_default_database";

    let server = NativeTestServer::start().await;
    let (mut setup, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let create_database = send_sql(&mut setup, 1, &format!("CREATE DATABASE {DATABASE}")).await;
    assert_eq!(
        create_database.status,
        ResponseStatus::Ok,
        "automatic trust must return the original operation response"
    );
    drop(setup);

    let catalog = server.shared.credentials.catalog();
    let database_id = catalog
        .get_database_id_by_name(DATABASE)
        .expect("look up trust default database")
        .expect("created trust default database descriptor");
    let stored = server
        .shared
        .credentials
        .prepare_set_default_database("nodedb", database_id.as_u64())
        .expect("set configured trust principal default database");
    catalog
        .put_user(&stored)
        .expect("persist configured trust principal default database");
    server
        .shared
        .credentials
        .install_replicated_user(&stored, None);

    let (mut writer, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let create_collection = send_sql(
        &mut writer,
        1,
        "CREATE COLLECTION native_trust_default_rows",
    )
    .await;
    assert_eq!(
        create_collection.status,
        ResponseStatus::Ok,
        "automatic trust must bind the configured principal default database"
    );
    let insert = send_sql(
        &mut writer,
        2,
        "INSERT INTO native_trust_default_rows { id: 'trust-default' }",
    )
    .await;
    assert_eq!(insert.status, ResponseStatus::Ok, "seed trust default row");
    drop(writer);

    let (mut reader, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let read = send_sql(
        &mut reader,
        1,
        "SELECT * FROM native_trust_default_rows WHERE id = 'trust-default'",
    )
    .await;
    drop(reader);
    server.shutdown().await;

    assert_eq!(
        read.status,
        ResponseStatus::Ok,
        "read configured trust default database"
    );
    assert_eq!(
        read.rows.as_ref().map(Vec::len),
        Some(1),
        "automatic trust must persist the selected database for SQL"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_auth_rejects_deleted_identity_default_database() {
    let server = NativeTestServer::start_authenticated().await;
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token).await;
    let create = send_sql(&mut admin, 2, "CREATE DATABASE native_deleted_default_db").await;
    assert_eq!(create.status, ResponseStatus::Ok, "create default database");
    drop(admin);

    let db_id = server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name("native_deleted_default_db")
        .expect("look up default database")
        .expect("created database descriptor");
    let token = create_api_key_with_databases(
        &server.shared,
        "native_deleted_default_reader",
        vec![Role::ReadOnly],
        vec![db_id],
        Some(db_id),
    );
    server
        .shared
        .credentials
        .catalog()
        .delete_database(db_id)
        .expect("delete database descriptor");

    let (mut stream, _) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let response = send_api_key_auth(&mut stream, 1, token).await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(response.status, ResponseStatus::Error);
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("3D000"),
        "a missing identity default database must reject Auth before session setup: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_discard_all_preserves_authenticated_database() {
    let server = NativeTestServer::start_authenticated().await;
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token).await;
    let create = send_sql(&mut admin, 2, "CREATE DATABASE native_discard_database").await;
    assert_eq!(
        create.status,
        ResponseStatus::Ok,
        "create selected database"
    );
    drop(admin);

    let db_id = server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name("native_discard_database")
        .expect("look up selected database")
        .expect("created database descriptor");
    let token = create_api_key_with_databases(
        &server.shared,
        "native_discard_database_admin",
        vec![Role::Superuser],
        vec![db_id],
        None,
    );
    let mut stream =
        authenticated_stream_in_database(&server, token, "native_discard_database").await;
    let create = send_sql(&mut stream, 2, "CREATE COLLECTION native_discard_rows").await;
    assert_eq!(
        create.status,
        ResponseStatus::Ok,
        "create collection in selected database"
    );
    let discard = send_sql(&mut stream, 3, "DISCARD ALL").await;
    assert_eq!(discard.status, ResponseStatus::Ok, "discard session state");
    let insert = send_sql(
        &mut stream,
        4,
        "INSERT INTO native_discard_rows { id: 'persisted-db-binding' }",
    )
    .await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(
        insert.status,
        ResponseStatus::Ok,
        "DISCARD ALL must retain the selected database for subsequent SQL: {insert:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_discard_all_preserves_selected_database_auth_context_for_rls() {
    let server = NativeTestServer::start_authenticated().await;
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token).await;

    let create_a = send_sql(&mut admin, 2, "CREATE DATABASE native_auth_context_a").await;
    assert_eq!(
        create_a.status,
        ResponseStatus::Ok,
        "create default database A"
    );
    let create_b = send_sql(&mut admin, 3, "CREATE DATABASE native_auth_context_b").await;
    assert_eq!(
        create_b.status,
        ResponseStatus::Ok,
        "create selected database B"
    );

    let catalog = server.shared.credentials.catalog();
    let db_a = catalog
        .get_database_id_by_name("native_auth_context_a")
        .expect("look up default database A")
        .expect("created database A descriptor");
    let db_b = catalog
        .get_database_id_by_name("native_auth_context_b")
        .expect("look up selected database B")
        .expect("created database B descriptor");
    drop(admin);

    let admin_token = create_api_key_with_databases(
        &server.shared,
        "native_auth_context_database_admin",
        vec![Role::Superuser],
        vec![db_b],
        None,
    );
    let mut admin =
        authenticated_stream_in_database(&server, admin_token, "native_auth_context_b").await;
    let create = send_sql(&mut admin, 2, "CREATE COLLECTION native_auth_context_rows").await;
    assert_eq!(
        create.status,
        ResponseStatus::Ok,
        "create collection in database B"
    );
    let insert = send_sql(
        &mut admin,
        3,
        &format!(
            "INSERT INTO native_auth_context_rows {{ id: 'selected-b', shard: {} }}",
            db_b.as_u64()
        ),
    )
    .await;
    assert_eq!(insert.status, ResponseStatus::Ok, "insert database B row");
    let policy = send_sql(
        &mut admin,
        4,
        "CREATE RLS POLICY native_auth_context_selected_db ON native_auth_context_rows FOR READ \
         USING (shard = $auth.database_id)",
    )
    .await;
    assert_eq!(
        policy.status,
        ResponseStatus::Ok,
        "create selected database RLS policy: {policy:?}"
    );
    drop(admin);

    let token = create_api_key_with_databases(
        &server.shared,
        "native_auth_context_reader",
        vec![Role::ReadOnly],
        vec![db_a, db_b],
        Some(db_a),
    );
    let mut restricted =
        authenticated_stream_in_database(&server, token, "native_auth_context_b").await;

    let before_discard = send_sql(
        &mut restricted,
        2,
        "SELECT * FROM native_auth_context_rows WHERE id = 'selected-b'",
    )
    .await;
    assert_eq!(
        before_discard.status,
        ResponseStatus::Ok,
        "read selected database row"
    );
    assert_eq!(
        before_discard.rows.as_ref().map(Vec::len).unwrap_or(0),
        1,
        "RLS must see the B row using the explicitly selected database: {before_discard:?}"
    );

    let discard = send_sql(&mut restricted, 3, "DISCARD ALL").await;
    assert_eq!(discard.status, ResponseStatus::Ok, "discard session state");
    let after_discard = send_sql(
        &mut restricted,
        4,
        "SELECT * FROM native_auth_context_rows WHERE id = 'selected-b'",
    )
    .await;
    drop(restricted);
    server.shutdown().await;

    assert_eq!(
        after_discard.status,
        ResponseStatus::Ok,
        "DISCARD ALL must retain the selected database for native SQL: {after_discard:?}"
    );
    assert_eq!(
        after_discard.rows.as_ref().map(Vec::len).unwrap_or(0),
        1,
        "DISCARD ALL must preserve the selected database AuthContext for RLS: {after_discard:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_materialized_sql_rejects_write_without_permission_or_mutation() {
    let server = NativeTestServer::start_authenticated().await;
    let admin_token = create_api_key(&server.shared, "nodedb", vec![Role::Superuser]);
    let mut admin = authenticated_stream(&server, admin_token).await;
    let create = send_sql(&mut admin, 2, "CREATE COLLECTION native_denied_writes").await;
    assert_eq!(create.status, ResponseStatus::Ok, "create write target");

    let token = create_api_key(
        &server.shared,
        "native_ungranted_writer",
        vec![Role::Custom("native_ungranted_writer_role".into())],
    );
    let mut restricted = authenticated_stream(&server, token).await;
    let response = send_sql(
        &mut restricted,
        2,
        "INSERT INTO native_denied_writes { id: 'forbidden', value: 23 }",
    )
    .await;
    let observed = send_sql(
        &mut admin,
        3,
        "SELECT id FROM native_denied_writes ORDER BY id",
    )
    .await;
    drop(restricted);
    drop(admin);
    server.shutdown().await;

    assert!(
        observed.rows.as_ref().is_none_or(Vec::is_empty)
            && observed.rows_affected.unwrap_or_default() == 0,
        "an unauthorized native write must not mutate the collection: {observed:?}"
    );
    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native writes require explicit PermissionStore authorization before dispatch"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native write denial must report insufficient privilege: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_explain_rejects_collection_before_plan_metadata() {
    let server = NativeTestServer::start_authenticated().await;
    seed_private_collection(&server, "native_explain_private").await;
    let token = create_api_key(
        &server.shared,
        "native_explain_reader",
        vec![Role::Custom("native_explain_role".into())],
    );
    let mut stream = authenticated_stream(&server, token).await;

    let response = send_sql(
        &mut stream,
        2,
        "EXPLAIN SELECT * FROM native_explain_private",
    )
    .await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(response.status, ResponseStatus::Error);
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native EXPLAIN must authorize before exposing plan metadata: {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_lazy_sql_rejects_collection_without_permission_before_stream_open() {
    let server = NativeTestServer::start_authenticated().await;
    seed_private_collection(&server, "native_lazy_private").await;
    let token = create_api_key(
        &server.shared,
        "native_lazy_reader",
        vec![Role::Custom("native_lazy_role".into())],
    );
    let mut stream = authenticated_stream(&server, token).await;

    let response = send_sql(&mut stream, 2, "SELECT * FROM native_lazy_private").await;
    drop(stream);
    server.shutdown().await;

    assert_eq!(
        response.status,
        ResponseStatus::Error,
        "native lazy SQL must reject access before opening the result stream"
    );
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("42501"),
        "native lazy denial must report insufficient privilege: {response:?}"
    );
}
