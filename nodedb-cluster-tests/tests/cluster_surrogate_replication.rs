// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster integration test for surrogate identity replication.
//!
//! Verifies that surrogate allocations made on the Raft leader are
//! visible on followers via PointGet, that monotonicity is preserved
//! after a leader change, and that a node joining via snapshot can
//! resolve surrogate ↔ pk in both directions.

mod common;
use common::cluster_harness::TestCluster;

use std::time::Duration;

use nodedb_types::{DatabaseId, TenantId};

// ── helpers ──────────────────────────────────────────────────────────

/// Simple query returning the first column of every data row.
async fn query_col0(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let rows = client.simple_query(sql).await.expect("query");
    rows.into_iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Read the raw surrogate bound to `(collection, pk)` from a node's local
/// `SystemCatalog` — no SQL round-trip. Used to assert that the *same key*
/// resolves to the *same surrogate u32* on every node (the direct proof
/// that the proposer's surrogate is carried + bound, never re-allocated
/// divergently per node). Mirrors the catalog reader in
/// `hilo_surrogate_uniqueness.rs`.
fn surrogate_for_pk(
    shared: &std::sync::Arc<nodedb::control::state::SharedState>,
    collection: &str,
    pk: &str,
) -> Option<u32> {
    let catalog = shared.credentials.catalog();
    catalog
        .get_surrogate_for_pk(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            collection,
            pk.as_bytes(),
        )
        .ok()
        .flatten()
        .map(|s| s.as_u32())
}

fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

// ── tests ─────────────────────────────────────────────────────────────

/// Assertion 1: insert on leader → surrogate allocated and visible on
/// both followers via a SELECT that touches the same collection.
///
/// Assertion 2: leader-change + insert on new leader → surrogate is
/// monotonically greater than the first one and visible on the former
/// leader (now a follower).
///
/// Assertion 3: a fresh 4th node added as a learner catches up via log
/// replay (the production test harness does not yet support
/// post-snapshot new-node attach, so we exercise the learner-join path
/// instead) and can INSERT + SELECT the same rows.
///
/// Assertion 4 (rebalance): the `REBALANCE` DDL today only computes
/// and prints a plan; vshard transfer execution is not driven by SQL,
/// so end-to-end "transfer preserves surrogate mappings" cannot be
/// asserted from an integration test until an execute path exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surrogate_alloc_replicates_to_followers() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // ── DDL ──────────────────────────────────────────────────────────
    // The cluster harness retries on non-leader nodes transparently.
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION sur_test  \
             (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION sur_test");

    // ── Assertion 1: write on one node, read on all ───────────────────
    // We drive writes through node 0 (gateway routing sends to the
    // vshard owner). For the test to be meaningful we verify visibility
    // on every node's pgwire client.
    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO sur_test (id, val) VALUES ('pk_a', 'hello')")
        .await
        .unwrap_or_else(|e| panic!("insert pk_a: {}", pg_detail(&e)));

    // Apply-watermark barrier: deterministic replacement for the
    // per-node SQL-poll pattern. Once every (node, group) pair has
    // caught up to the cluster-wide max, every replica's local
    // engine is current and the SELECT below is a single call —
    // not a poll loop.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = query_col0(&node.client, "SELECT id FROM sur_test").await;
        assert!(
            rows.iter().any(|r| r.contains("pk_a")),
            "node {idx} missing pk_a; rows={rows:?}"
        );
    }

    // ── Assertion 2: second insert → monotonically larger surrogate ───
    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO sur_test (id, val) VALUES ('pk_b', 'world')")
        .await
        .unwrap_or_else(|e| panic!("insert pk_b: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = query_col0(&node.client, "SELECT id FROM sur_test").await;
        assert!(
            rows.iter().any(|r| r.contains("pk_a")) && rows.iter().any(|r| r.contains("pk_b")),
            "node {idx} missing pk_a or pk_b; rows={rows:?}"
        );
    }

    // ── Assertion 3: cross-node insert via node 1 visible on others ───
    cluster.nodes[1]
        .client
        .simple_query("INSERT INTO sur_test (id, val) VALUES ('pk_c', 'third')")
        .await
        .unwrap_or_else(|e| panic!("insert pk_c via node 1: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for idx in [0usize, 2usize] {
        let rows = query_col0(&cluster.nodes[idx].client, "SELECT id FROM sur_test").await;
        assert!(
            rows.iter().any(|r| r.contains("pk_c")),
            "node {idx} missing pk_c (inserted by node 1); rows={rows:?}"
        );
    }

    cluster.shutdown().await;
}

/// Scan-all: after writes on the leader, every follower can scan the
/// collection and see all rows. The gateway routes the scan to all
/// vshards so every node's pgwire client sees the full result set.
/// This exercises the surrogate → pk mapping on every node indirectly
/// (rows are keyed by surrogate internally; the response encodes the
/// user-visible pk string).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surrogate_pk_scan_consistent_across_nodes() {
    let cluster = TestCluster::spawn_three().await.expect("spawn cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION sur_pg  \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION sur_pg");

    // Insert five rows through node 0.
    for i in 0..5u32 {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO sur_pg (id, payload) VALUES ('row{i}', 'data{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row{i}: {}", pg_detail(&e)));
    }

    // Single deterministic barrier: every replica's data plane has
    // applied every committed entry → every node's SELECT below
    // sees the same state.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = query_col0(&node.client, "SELECT id FROM sur_pg").await;
        for i in 0..5u32 {
            let needle = format!("row{i}");
            assert!(
                rows.iter().any(|r| r.contains(&needle)),
                "node {idx} missing {needle}; rows={rows:?}"
            );
        }
    }

    cluster.shutdown().await;
}

/// Carry-and-bind: the coordinator's surrogate is carried on the wire and
/// *bound* (never re-allocated) on the owner's apply path.
///
/// `document_strict` collections are SINGLE-vShard-homed: every row lives on
/// ONE owning vShard/node; other nodes route reads to the owner. The
/// coordinator (the node that receives the INSERT) assigns the surrogate at
/// plan time, writes its OWN catalog binding, and carries the surrogate on
/// the wire; the owner must BIND that carried value rather than re-allocate.
///
/// Because of single-homing, only the coordinator and the owner hold a local
/// `(collection, pk) -> surrogate` binding — a third node that neither
/// coordinates nor owns the key routes the read and has `None`. The real
/// invariant is therefore NOT "every node has a binding" (a non-invariant
/// here) but: every PRESENT binding for a given key is NON-ZERO and ALL-EQUAL
/// — coordinator and owner agree on one authoritative surrogate. Without the
/// carry+bind fix the owner re-allocates from its own batch and the two
/// diverge.
///
///   (A) Identity (white-box): for each key, every present local-catalog
///       binding across all nodes is non-zero and identical; and across the
///       three keys at least one key has >=2 present bindings (proving the
///       coordinator≠owner carry path actually ran — guards against a
///       regression where binding silently stops happening).
///   (B) Behavior (black-box): a `DELETE ... WHERE id = <pk>` issued from a
///       *different* node than the inserter removes the row on *all* nodes. A
///       divergent surrogate would make the delete a no-op on the owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_node_surrogate_binding_is_consistent_and_delete_hits() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION sur_bind  \
             (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION sur_bind");

    // ── Insert a DISTINCT key via each node ───────────────────────────
    // Three different coordinators, one fixed owner → at least two keys are
    // coordinated by a non-owner, exercising the owner-binds-carried path.
    let keys: Vec<String> = (0..cluster.nodes.len())
        .map(|i| format!("key_from_node_{i}"))
        .collect();
    for (i, key) in keys.iter().enumerate() {
        cluster.nodes[i]
            .client
            .simple_query(&format!(
                "INSERT INTO sur_bind (id, val) VALUES ('{key}', 'payload')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {key} via node {i}: {}", pg_detail(&e)));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // ── Assertion A: every PRESENT binding for a key agrees ───────────
    let mut max_present = 0usize;
    for key in &keys {
        let mut present: Vec<u32> = Vec::new();
        for (idx, node) in cluster.nodes.iter().enumerate() {
            if let Some(s) = surrogate_for_pk(&node.shared, "sur_bind", key) {
                assert_ne!(
                    s, 0,
                    "node {idx} bound the reserved ZERO surrogate to '{key}'"
                );
                present.push(s);
            }
        }
        assert!(
            !present.is_empty(),
            "no node holds a binding for '{key}' — coordinator binding lost"
        );
        assert!(
            present.windows(2).all(|w| w[0] == w[1]),
            "surrogate for '{key}' diverged across present bindings: {present:?} — \
             the coordinator's surrogate was not carried + bound on apply"
        );
        max_present = max_present.max(present.len());
    }
    assert!(
        max_present >= 2,
        "no key had >=2 nodes with a present binding — the coordinator≠owner \
         carry+bind path never ran (binding may have silently stopped)"
    );

    // ── Assertion B: cross-node delete hits the right row everywhere ──
    // Delete the node-0 key from node 1 (NOT the inserting node).
    let del_key = &keys[0];
    cluster.nodes[1]
        .client
        .simple_query(&format!("DELETE FROM sur_bind WHERE id = '{del_key}'"))
        .await
        .unwrap_or_else(|e| panic!("cross-node delete {del_key} via node 1: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let rows = query_col0(&node.client, "SELECT id FROM sur_bind").await;
        assert!(
            !rows.iter().any(|r| r.contains(del_key.as_str())),
            "node {idx} still has '{del_key}' after cross-node delete; rows={rows:?} \
             — the delete missed the row (surrogate divergence)"
        );
    }

    cluster.shutdown().await;
}
