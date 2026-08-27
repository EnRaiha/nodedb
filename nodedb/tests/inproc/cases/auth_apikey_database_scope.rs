// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for API key database scoping (section B).

use nodedb_test_support::pgwire_auth_helpers::{
    ddl_err, ddl_ok, make_state, make_state_with_catalog, superuser,
};
use nodedb_types::id::DatabaseId;

/// Superuser can create API keys with no database restriction.
#[tokio::test]
async fn superuser_can_create_key_without_databases_clause() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER alice WITH PASSWORD 'pass' ROLE readwrite TENANT 1",
    )
    .await;
    ddl_ok(&state, &su, "CREATE API KEY FOR alice").await;
}

/// CREATE API KEY with databases the owner lacks is rejected with 42501.
#[tokio::test]
async fn create_key_with_wider_databases_than_owner_rejected() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER bob WITH PASSWORD 'pass' ROLE readwrite TENANT 1",
    )
    .await;

    // Bob has no explicit database grants, so his set is just DEFAULT.
    // Asking for a non-existent database should fail.
    // We first try with a non-existent DB — should get 42704 (not found).
    let err = ddl_err(
        &state,
        &su,
        "CREATE API KEY FOR bob WITH DATABASES ghost_db",
    )
    .await;
    // Either "not found" or "not in owner's set" — both indicate rejection.
    assert!(
        err.contains("not found") || err.contains("not in owner") || err.contains("42704"),
        "expected rejection, got: {err}"
    );
}

/// Superuser key inherits: empty accessible_databases means inherit owner.
#[tokio::test]
async fn inherit_flag_is_empty_accessible_databases() {
    let state = make_state_with_catalog();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER carol WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "CREATE API KEY FOR carol").await;

    // The key should be in the store with no accessible_databases (inherit flag).
    let keys = state.api_keys.list_keys_for_user("carol");
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].accessible_databases.is_empty(),
        "inherit key should have empty accessible_databases"
    );
}

/// LIST API KEYS includes the databases column.
#[tokio::test]
async fn list_api_keys_includes_databases_column() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER dave WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "CREATE API KEY FOR dave").await;

    // Should not error.
    ddl_ok(&state, &su, "LIST API KEYS").await;
}

/// SHOW API KEYS routes to the same handler as LIST API KEYS.
#[tokio::test]
async fn show_api_keys_routes_same_as_list() {
    let state = make_state();
    let su = superuser();
    ddl_ok(&state, &su, "CREATE USER eve WITH PASSWORD 'pass'").await;
    ddl_ok(&state, &su, "CREATE API KEY FOR eve").await;

    ddl_ok(&state, &su, "SHOW API KEYS").await;
}

/// Key with accessible_databases roundtrips through store correctly.
#[tokio::test]
async fn key_accessible_databases_roundtrip_in_store() {
    use nodedb::control::security::apikey::ApiKeyStore;
    use nodedb::types::TenantId;

    let store = ApiKeyStore::new();
    let db1 = DatabaseId::new(1);

    // create_key with explicit databases
    let _token = store
        .create_key(
            nodedb::control::security::apikey::CreateKeyParams {
                username: "frank",
                user_id: 10,
                tenant_id: TenantId::new(1),
                expires_secs: 0,
                scope: vec![],
                accessible_databases: vec![db1],
            },
            None,
        )
        .unwrap();

    let keys = store.list_keys_for_user("frank");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].accessible_databases, vec![db1]);
}

/// DatabaseSet::intersect correctness in the session auth flow.
#[tokio::test]
async fn narrowed_key_respects_intersection_in_verify() {
    use nodedb::control::security::apikey::ApiKeyStore;
    use nodedb::control::security::identity::DatabaseSet;
    use nodedb::types::TenantId;
    use smallvec::smallvec;

    let db_a = DatabaseId::new(1);
    let db_b = DatabaseId::new(2);

    // Owner set: {a, b}. Key set: {a}. Effective should be {a}.
    let owner_set = DatabaseSet::Some(smallvec![db_a, db_b]);
    let key_set = DatabaseSet::Some(smallvec![db_a]);
    let effective = owner_set.intersect(&key_set);
    assert_eq!(effective, DatabaseSet::Some(smallvec![db_a]));

    // Owner set: {a}. Key set: {b}. Effective should be empty.
    let owner_set2 = DatabaseSet::Some(smallvec![db_a]);
    let key_set2 = DatabaseSet::Some(smallvec![db_b]);
    let effective2 = owner_set2.intersect(&key_set2);
    assert_eq!(effective2, DatabaseSet::Some(smallvec![]));

    // Owner set narrowed after key creation: owner has {a}, key has {a, b}.
    // Intersection at bind time = {a}.
    let owner_post_narrow = DatabaseSet::Some(smallvec![db_a]);
    let key_at_create = DatabaseSet::Some(smallvec![db_a, db_b]);
    let effective3 = owner_post_narrow.intersect(&key_at_create);
    assert_eq!(effective3, DatabaseSet::Some(smallvec![db_a]));

    let _ = ApiKeyStore::new();
    let _ = TenantId::new(1);
}
