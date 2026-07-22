// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: a plain (soft) `DROP COLLECTION` must hide a graph
//! collection's edges and stats from reads, even though it does not
//! physically reclaim storage.
//!
//! Before the fix, `DROP COLLECTION c` (without `PURGE`) only flipped the
//! catalog `is_active` flag — `MATCH ... IN c` and tenant-wide
//! `SHOW GRAPH STATS` never consulted `is_active`, so soft-dropped
//! collections kept showing up in graph reads until a hard purge. This
//! mirrors the fix applied to `graph_ops::stats::show_graph_stats` for the
//! single-collection case, extended to the tenant-wide aggregate and to
//! `MATCH ... IN c`.

mod common;

use common::pgwire_harness::TestServer;

/// `MATCH ... IN 'c'` and tenant-wide `SHOW GRAPH STATS` must stop seeing a
/// collection's edges the moment it is soft-dropped, and must see them again
/// once it is `UNDROP`-ed (soft-delete is reversible, not physical erasure).
#[tokio::test]
async fn soft_drop_hides_match_and_tenant_stats_until_undrop() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_soft_drop").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_soft_drop' FROM 'a' TO 'b' TYPE 'knows'")
        .await
        .unwrap();

    // Sanity: both MATCH and tenant-wide stats see the edge pre-drop.
    {
        let rows = srv
            .query_text_joined("MATCH (x)-[:knows]->(y) IN 'g_soft_drop' RETURN x, y")
            .await
            .expect("MATCH should succeed pre-drop");
        assert!(!rows.is_empty(), "expected edge visible pre-drop: {rows:?}");
    }
    {
        let rows = srv.query_rows("SHOW GRAPH STATS").await.unwrap();
        assert!(
            rows.iter().any(|r| r[0] == "g_soft_drop"),
            "expected g_soft_drop in tenant-wide stats pre-drop: {rows:?}"
        );
    }

    // Plain (soft) DROP — no PURGE.
    srv.exec("DROP COLLECTION g_soft_drop").await.unwrap();

    // MATCH targeting the deactivated collection must now fail the same way
    // a base-engine SELECT against a soft-dropped collection does: 42P01.
    srv.expect_error(
        "MATCH (x)-[:knows]->(y) IN 'g_soft_drop' RETURN x, y",
        "42P01",
    )
    .await;

    // Tenant-wide SHOW GRAPH STATS must no longer include the collection's
    // counters.
    {
        let rows = srv.query_rows("SHOW GRAPH STATS").await.unwrap();
        assert!(
            !rows.iter().any(|r| r[0] == "g_soft_drop"),
            "g_soft_drop must be hidden from tenant-wide stats post-drop: {rows:?}"
        );
    }

    // UNDROP restores visibility — proving the data was hidden, not erased.
    srv.exec("UNDROP COLLECTION g_soft_drop").await.unwrap();

    {
        let rows = srv
            .query_text_joined("MATCH (x)-[:knows]->(y) IN 'g_soft_drop' RETURN x, y")
            .await
            .expect("MATCH should succeed post-undrop");
        assert!(
            !rows.is_empty(),
            "expected edge visible again post-undrop: {rows:?}"
        );
    }
    {
        let rows = srv.query_rows("SHOW GRAPH STATS").await.unwrap();
        assert!(
            rows.iter().any(|r| r[0] == "g_soft_drop"),
            "expected g_soft_drop back in tenant-wide stats post-undrop: {rows:?}"
        );
    }
}

/// Single-collection `SHOW GRAPH STATS 'c'` already gated on `is_active`
/// before this fix — this test pins that existing behavior stays intact
/// alongside the new tenant-wide + MATCH gating.
#[tokio::test]
async fn soft_drop_named_collection_stats_still_deactivated() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_named_drop").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_named_drop' FROM 'a' TO 'b' TYPE 'k'")
        .await
        .unwrap();

    srv.exec("DROP COLLECTION g_named_drop").await.unwrap();

    srv.expect_error("SHOW GRAPH STATS 'g_named_drop'", "42P01")
        .await;
}

/// `DROP ... PURGE` must still fully remove edges and stats — the hard-purge
/// path is untouched by this fix and must keep working exactly as before.
///
/// Multi-threaded runtime: the single-node purge path reclaims storage inline
/// via `block_in_place`, which panics on the current-thread runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_purge_removes_edges_and_stats() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION g_hard_purge").await.unwrap();
    srv.exec("GRAPH INSERT EDGE IN 'g_hard_purge' FROM 'a' TO 'b' TYPE 'k'")
        .await
        .unwrap();

    srv.exec("DROP COLLECTION g_hard_purge PURGE")
        .await
        .unwrap();

    // Purged collections are gone outright — not found, not deactivated.
    srv.expect_error("SHOW GRAPH STATS 'g_hard_purge'", "42P01")
        .await;

    let rows = srv.query_rows("SHOW GRAPH STATS").await.unwrap();
    assert!(
        !rows.iter().any(|r| r[0] == "g_hard_purge"),
        "purged collection must not reappear in tenant-wide stats: {rows:?}"
    );
}
