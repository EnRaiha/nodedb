// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION` against a soft-deleted (dropped, `is_active == false`)
//! collection must be refused, for every `ALTER COLLECTION` sub-handler.
//!
//! `catalog.get_collection` does not filter on `is_active`
//! (`control/security/catalog/collections.rs`), so a handler that loads the
//! row with a bare `.ok_or_else(...)` on `None` still gets `Some(coll)` back
//! for a dropped collection and happily mutates and re-persists it. Two
//! sibling ALTER handlers already guard against this —
//! `alter/add_column.rs` matches `Ok(Some(coll)) if coll.is_active` and
//! `alter/strict_schema.rs::load_strict_collection` does
//! `.filter(|c| c.is_active)` — both surfacing SQLSTATE `42P01` /
//! "does not exist" on a dropped name, the same shape `sql_drop_collection`
//! pins for `SELECT` against a dropped collection. The six handlers below
//! (`enforcement.rs` x4, `materialized_sum.rs`, `ownership.rs`) load the row
//! the same unguarded way and must be brought onto the same contract.
//!
//! Each test creates a collection meeting that handler's own preconditions,
//! `DROP`s it (soft-delete, the default), then re-issues the same ALTER and
//! asserts it fails with `42P01` — the SQLSTATE the two existing guards
//! already return for "collection missing or inactive". On the pre-fix tree
//! every one of these currently SUCCEEDS instead.

use crate::harness::TestServer;

/// `ALTER COLLECTION ... SET RETENTION` on a dropped collection must be
/// refused, not silently re-persist a resurrected row with the new
/// retention period.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_set_retention_on_dropped_collection_is_refused() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION alter_drop_retention (id TEXT PRIMARY KEY, v TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create collection");
    srv.exec("DROP COLLECTION alter_drop_retention")
        .await
        .expect("drop collection");

    srv.expect_error(
        "ALTER COLLECTION alter_drop_retention SET RETENTION = '30d'",
        "42P01",
    )
    .await;
}

/// `ALTER COLLECTION ... SET LEGAL_HOLD` on a dropped collection must be
/// refused, not attach a legal hold to a name nobody can query any more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_set_legal_hold_on_dropped_collection_is_refused() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION alter_drop_legal_hold (id TEXT PRIMARY KEY, v TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create collection");
    srv.exec("DROP COLLECTION alter_drop_legal_hold")
        .await
        .expect("drop collection");

    srv.expect_error(
        "ALTER COLLECTION alter_drop_legal_hold SET LEGAL_HOLD = TRUE TAG 'litigation'",
        "42P01",
    )
    .await;
}

/// `ALTER COLLECTION ... SET APPEND_ONLY` on a dropped collection must be
/// refused, not flip an enforcement flag on a row nothing can write to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_set_append_only_on_dropped_collection_is_refused() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION alter_drop_append_only (id TEXT PRIMARY KEY, v TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create collection");
    srv.exec("DROP COLLECTION alter_drop_append_only")
        .await
        .expect("drop collection");

    srv.expect_error(
        "ALTER COLLECTION alter_drop_append_only SET APPEND_ONLY",
        "42P01",
    )
    .await;
}

/// `ALTER COLLECTION ... SET LAST_VALUE_CACHE` on a dropped collection must
/// be refused. The collection must be a live timeseries collection for the
/// handler's own `is_timeseries()` gate to pass, so the pre-fix tree's bug
/// (accepting the ALTER anyway) isn't masked by an unrelated `42809`
/// "not a timeseries collection" rejection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_set_last_value_cache_on_dropped_collection_is_refused() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION alter_drop_lvc \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, sensor TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await
    .expect("create timeseries collection");
    srv.exec("DROP COLLECTION alter_drop_lvc")
        .await
        .expect("drop collection");

    srv.expect_error(
        "ALTER COLLECTION alter_drop_lvc SET LAST_VALUE_CACHE = TRUE",
        "42P01",
    )
    .await;
}

/// `ALTER COLLECTION ... ADD COLUMN ... MATERIALIZED_SUM` on a dropped
/// target collection must be refused, not bind a running total to a name
/// nobody can query any more. The target must be `document_strict` so the
/// handler's own column-declaration step (`declare_target_column`) would
/// otherwise succeed and mask the missing `is_active` guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_add_materialized_sum_on_dropped_target_is_refused() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION alter_drop_sum_target (id TEXT PRIMARY KEY, owner TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create target collection");
    srv.exec("DROP COLLECTION alter_drop_sum_target")
        .await
        .expect("drop target collection");

    srv.expect_error(
        "ALTER COLLECTION alter_drop_sum_target ADD COLUMN balance TEXT \
         MATERIALIZED_SUM SOURCE alter_drop_sum_source \
         ON alter_drop_sum_source.account_id = alter_drop_sum_target.id \
         VALUE alter_drop_sum_source.amount",
        "42P01",
    )
    .await;
}

/// `ALTER COLLECTION ... OWNER TO` on a dropped collection must be refused,
/// not transfer ownership of a name nobody can query any more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_owner_to_on_dropped_collection_is_refused() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE COLLECTION alter_drop_owner (id TEXT PRIMARY KEY, v TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create collection");
    srv.exec("DROP COLLECTION alter_drop_owner")
        .await
        .expect("drop collection");

    srv.expect_error("ALTER COLLECTION alter_drop_owner OWNER TO nodedb", "42P01")
        .await;
}
