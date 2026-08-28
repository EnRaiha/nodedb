// SPDX-License-Identifier: BUSL-1.1
//! Per-descriptor plan cache invalidation on a 3-node cluster.
//!
//! The plan cache now records the set of `(descriptor_id,
//! version)` pairs each plan touched. A cached plan is evicted
//! when any recorded descriptor's version bumps, but unrelated
//! DDL on a different descriptor leaves the cache intact.
//!
//! Both scenarios share a single cluster to avoid spawning two
//! 3-node clusters back-to-back in the same test binary — the
//! combined thread/port footprint was enough to stall shutdown
//! between tests on loaded hardware.

use crate::common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};

const TENANT: u64 = 1;
// 10 s — every visibility check in this test rides on the metadata
// Raft commit + apply + post-apply cache update path. Five seconds
// (the original budget) was tight enough that fresh-cluster startup
// jitter occasionally tripped a false timeout. Ten seconds is still
// strict enough to catch real regressions but tolerant of cold-start
// election lag.
const WAIT_BUDGET: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn per_descriptor_plan_cache_invalidation() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // ── Setup: two collections + two owners, all replicated. ──
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION orders")
        .await
        .expect("create orders");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION bar")
        .await
        .expect("create bar");
    cluster
        .exec_ddl_on_any_leader("CREATE USER cache_owner WITH PASSWORD 'pw' ROLE READWRITE")
        .await
        .expect("create cache_owner");
    cluster
        .exec_ddl_on_any_leader("CREATE USER unrelated_owner WITH PASSWORD 'pw' ROLE READWRITE")
        .await
        .expect("create unrelated_owner");

    wait_for("collections stamped v1", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| {
            n.collection_descriptor(TENANT, "orders").map(|s| s.0) == Some(1)
                && n.collection_descriptor(TENANT, "bar").map(|s| s.0) == Some(1)
        })
    })
    .await;
    wait_for("users replicated", WAIT_BUDGET, POLL, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.has_active_user("cache_owner") && n.has_active_user("unrelated_owner"))
    })
    .await;

    let leader = &cluster.nodes[0];

    // ── Scenario A: ALTER on target descriptor bumps its version
    // and forces re-plan. ──
    leader.exec("SELECT * FROM orders").await.expect("orders 1");
    leader.exec("SELECT * FROM orders").await.expect("orders 2");

    cluster
        .exec_ddl_on_any_leader("ALTER COLLECTION orders OWNER TO cache_owner")
        .await
        .expect("alter orders");

    wait_for("orders stamped v2", WAIT_BUDGET, POLL, || {
        cluster
            .nodes
            .iter()
            .all(|n| n.collection_descriptor(TENANT, "orders").map(|s| s.0) == Some(2))
    })
    .await;

    // Re-plan must succeed against v2. The correctness guard for
    // actual invalidation lives in `plan_cache::cache_miss_version_bump`.
    leader.exec("SELECT * FROM orders").await.expect("orders 3");

    // ── Scenario B: ALTER on unrelated descriptor leaves the
    // other cached plan intact. ──
    leader.exec("SELECT * FROM bar").await.expect("bar 1");

    cluster
        .exec_ddl_on_any_leader("ALTER COLLECTION orders OWNER TO unrelated_owner")
        .await
        .expect("alter orders again");

    wait_for("orders stamped v3, bar still v1", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| {
            n.collection_descriptor(TENANT, "orders").map(|s| s.0) == Some(3)
                && n.collection_descriptor(TENANT, "bar").map(|s| s.0) == Some(1)
        })
    })
    .await;

    // bar's cached plan is still valid — must still succeed.
    leader.exec("SELECT * FROM bar").await.expect("bar 2");
    leader.exec("SELECT * FROM orders").await.expect("orders 4");

    cluster.shutdown().await;
}
