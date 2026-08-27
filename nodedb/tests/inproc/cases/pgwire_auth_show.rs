// SPDX-License-Identifier: BUSL-1.1

//! SHOW commands (SHOW SESSION, SHOW GRANTS) over the DDL path.

use nodedb::control::server::shared::ddl;
use nodedb::control::server::shared::ddl::result::DdlResult;
use nodedb::control::server::shared::session::DetachedTxnScope;
use nodedb_test_support::pgwire_auth_helpers::{ddl_ok, make_state, superuser};

#[tokio::test]
async fn show_session() {
    let state = make_state();
    let su = superuser();
    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        &state,
        &su,
        "SHOW SESSION",
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .unwrap()
    .unwrap();

    match &result[0] {
        DdlResult::Rows(_) => {}
        other => panic!("expected Rows response, got: {other:?}"),
    }
}

#[tokio::test]
async fn show_grants() {
    let state = make_state();
    let su = superuser();
    ddl_ok(
        &state,
        &su,
        "CREATE USER judy WITH PASSWORD 'pass' ROLE readwrite",
    )
    .await;
    ddl_ok(&state, &su, "GRANT ROLE monitor TO judy").await;

    let scope = DetachedTxnScope::new();
    let result = ddl::dispatch(
        &state,
        &su,
        "SHOW GRANTS FOR judy",
        nodedb_types::id::DatabaseId::DEFAULT,
        &scope.ctx(),
    )
    .await
    .unwrap()
    .unwrap();
    match &result[0] {
        DdlResult::Rows(_) => {}
        other => panic!("expected Rows response, got: {other:?}"),
    }
}
