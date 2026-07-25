// SPDX-License-Identifier: BUSL-1.1

//! Shared fixtures for `pgwire_auth_*` integration tests.
//!
//! Each split test file needs: a minimal `SharedState`, two canonical
//! identities (superuser + readonly), and two DDL runners (expect ok /
//! expect err). Keeping them here avoids copy-paste drift across files.

#![allow(dead_code)]

use std::sync::Arc;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, DatabaseSet, Role};
use nodedb::control::server::pgwire::ddl_encode;
use nodedb::control::server::shared::ddl;
use nodedb::control::server::shared::session::DetachedTxnScope;
use nodedb::control::state::SharedState;
use nodedb::types::TenantId;
use nodedb::wal::WalManager;

/// Create a minimal `SharedState` (no Data Plane needed for DDL tests).
pub fn make_state() -> Arc<SharedState> {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    SharedState::new(dispatcher, wal).expect("build shared state")
}

/// Create a `SharedState` whose `CredentialStore` is backed by a real redb
/// catalog and the built-in `default` database is bootstrapped. Use this for
/// DDL tests that resolve database names (e.g. `FOR DATABASE default`).
pub fn make_state_with_catalog() -> Arc<SharedState> {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let catalog_path = dir.path().join("system.redb");
    let credentials = Arc::new(
        nodedb::control::security::credential::store::CredentialStore::open(&catalog_path).unwrap(),
    );
    let _ = credentials.catalog().bootstrap_default_database();
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    SharedState::new_with_credentials(dispatcher, wal, credentials).expect("build shared state")
    // `dir` drops here. On Linux, file handles held by `wal` and the redb
    // catalog keep both files readable for the test's lifetime even after
    // the directory entry is removed (open-then-unlink semantics).
}

/// Superuser identity for DDL tests.
pub fn superuser() -> AuthenticatedIdentity {
    let store = nodedb::control::security::credential::store::CredentialStore::new()
        .expect("create test credential store");
    store
        .bootstrap_superuser("nodedb", "test-only-password")
        .expect("bootstrap test superuser");
    store
        .to_identity("nodedb", AuthMethod::Trust)
        .expect("build catalog-authenticated test superuser")
}

/// Readonly identity for permission tests.
pub fn readonly_user() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        99,
        "viewer",
        TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::ReadOnly],
        None,
        DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT,]),
    )
}

/// Run DDL, expect success.
pub async fn ddl_ok(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) {
    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        state,
        identity,
        sql,
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .map(ddl_encode::ddl_results_to_pgwire);
    assert!(result.is_some(), "DDL not recognized: {sql}");
    result
        .unwrap()
        .unwrap_or_else(|e| panic!("DDL failed: {sql}: {e}"));
}

/// Run DDL, expect error; return the error string for assertions.
pub async fn ddl_err(state: &SharedState, identity: &AuthenticatedIdentity, sql: &str) -> String {
    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        state,
        identity,
        sql,
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .map(ddl_encode::ddl_results_to_pgwire);
    assert!(result.is_some(), "DDL not recognized: {sql}");
    let err = result.unwrap().unwrap_err();
    err.to_string()
}

/// Run DDL and return the result without panicking on either branch. Useful
/// when the gate test only cares whether the privilege check fired (look for
/// "42501" in the error string) and the underlying handler may legitimately
/// succeed or fail with a non-privilege error depending on cluster state.
pub async fn try_ddl(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<(), String> {
    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        state,
        identity,
        sql,
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .map(ddl_encode::ddl_results_to_pgwire);
    let result = result.expect("DDL not recognized");
    result.map(|_| ()).map_err(|e| e.to_string())
}

/// Run DDL as a readonly identity and assert it is denied with `permission denied`.
pub async fn assert_readonly_denied(state: &SharedState, sql: &str) {
    let viewer = readonly_user();
    let err = ddl_err(state, &viewer, sql).await;
    assert!(err.contains("permission denied"), "{err}");
}

/// Cluster-admin identity (no implicit RLS bypass, no cross-DB data access).
pub fn cluster_admin_user() -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        100,
        "cluster_admin",
        nodedb::types::TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::ClusterAdmin],
        None,
        DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT,]),
    )
}

/// Database-owner identity for `db_id`.
pub fn database_owner_user(db_id: nodedb_types::id::DatabaseId) -> AuthenticatedIdentity {
    AuthenticatedIdentity::new_regular(
        101,
        "db_owner",
        nodedb::types::TenantId::new(1),
        AuthMethod::Trust,
        vec![Role::DatabaseOwner(db_id)],
        None,
        DatabaseSet::Some(smallvec::smallvec![db_id]),
    )
}

/// Assert that the audit log contains at least one entry with `event` and `db_id`.
pub fn assert_audit_has(
    state: &SharedState,
    event: nodedb::control::security::audit::AuditEvent,
    db_id: Option<nodedb_types::id::DatabaseId>,
) {
    let log = state.audit.lock().unwrap_or_else(|p| p.into_inner());
    assert!(
        log.all()
            .iter()
            .any(|e| e.event == event && e.database_id == db_id),
        "expected audit event {event:?} for db {db_id:?}, got {:?}",
        log.all()
            .iter()
            .map(|e| (e.event.clone(), e.database_id))
            .collect::<Vec<_>>()
    );
}
