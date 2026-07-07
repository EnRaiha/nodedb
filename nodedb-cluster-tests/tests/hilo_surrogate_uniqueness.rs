// SPDX-License-Identifier: BUSL-1.1
//! Cluster integration test: S4a-0 — HiLo surrogate allocator global uniqueness.
//!
//! ## Invariant proved
//!
//! In cluster mode, each node carves disjoint batches from a deterministic
//! global watermark `G` advanced exclusively via
//! `MetadataEntry::SurrogateReserve` through the metadata Raft group. This
//! test proves:
//!
//!   (i)  Every surrogate assigned cluster-wide is globally unique — no value
//!        is handed to two different primary keys on any node combination.
//!   (ii) No surrogate is `Surrogate::ZERO` (the reserved sentinel).
//!   (iii) The two nodes' surrogate sets are fully disjoint — no shared value
//!         exists between ranges reserved by different nodes.
//!
//! ## Scale
//!
//! Each node reserves one ~4096-wide batch from the global counter on first use.
//! Node 0 gets `[1, 4097)` and node 1 gets `[4097, 8193)` — entirely
//! non-overlapping. ~100 inserts per node is therefore more than sufficient to
//! prove the cross-node disjointness invariant; we do NOT need to fill or cross
//! an entire batch. Batch-boundary crossing (exhausting a reservation and
//! triggering a second `reserve_from_global` Raft write) is already covered by
//! the `reserve_from_global` unit tests in `registry.rs` — there is no need to
//! reproduce that cost here.
//!
//! ## What is NOT tested here (deferred to later units)
//!
//! Same-key unification across nodes — that is S4a-1 / a later unit. Here
//! every key is globally distinct; we do NOT re-insert the same key on a
//! second node and do NOT check that two nodes bind it to the same surrogate.

mod common;
use common::cluster_harness::{TestCluster, wait::wait_for};

use std::collections::HashSet;
use std::time::Duration;

use nodedb_types::{DatabaseId, TenantId};

// ── constants ─────────────────────────────────────────────────────────────────

/// Number of distinct keys inserted per node.
///
/// Each node carves one ~4096-wide batch from the global Raft counter on first
/// use, so even a handful of inserts exercises the full cross-node uniqueness
/// path. ~100 keeps total wall-clock well within the cluster-group harness
/// timeout while still meaningfully populating both nodes' catalogs.
const KEYS_PER_NODE: u32 = 100;

/// Name of the test collection.
const COLLECTION: &str = "hilo_uniq_col";

// ── helpers ───────────────────────────────────────────────────────────────────

/// Drain the surrogate catalog on `node` for `collection` and return the full
/// set of `(pk_string, surrogate_u32)` pairs. Uses the `SystemCatalog` API
/// directly — no SQL round-trip needed.
fn read_catalog_surrogates(
    shared: &std::sync::Arc<nodedb::control::state::SharedState>,
    collection: &str,
) -> Vec<(String, u32)> {
    let catalog = shared.credentials.catalog();
    catalog
        .scan_surrogates_for_collection(DatabaseId::DEFAULT, TenantId::new(1), collection)
        .unwrap_or_default()
        .into_iter()
        .map(|(pk_bytes, surrogate)| {
            let pk = String::from_utf8(pk_bytes).unwrap_or_else(|_| "<binary>".into());
            (pk, surrogate.as_u32())
        })
        .collect()
}

/// Insert `count` rows with keys `<prefix>_0` … `<prefix>_{count-1}` through
/// `client`, retrying each INSERT for up to 15 s (the data-group leader may
/// be mid-election on a freshly-formed cluster). Panics if any insert never
/// succeeds within the deadline.
async fn insert_batch(client: &tokio_postgres::Client, collection: &str, prefix: &str, count: u32) {
    for i in 0..count {
        let pk = format!("{prefix}_{i}");
        let sql = format!("INSERT INTO {collection} (id, val) VALUES ('{pk}', 'v{i}')");
        wait_for(
            &format!("INSERT {pk}"),
            Duration::from_secs(15),
            Duration::from_millis(100),
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(client.simple_query(&sql))
                })
                .is_ok()
            },
        )
        .await;
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// S4a-0: cross-node surrogate uniqueness and disjointness.
///
/// Proves in a single cluster bring-up (minimising expensive spawns) that:
///
///   (A) Every surrogate assigned across both nodes is globally unique —
///       merge both sets, sort, dedup, assert count equals total inserts.
///   (B) No `Surrogate::ZERO` was issued.
///   (C) The two nodes' surrogate sets are disjoint — HashSet intersection
///       is empty.
///
/// Batch-boundary crossing (triggering a second `reserve_from_global` Raft
/// round) is already covered by `reserve_from_global` unit tests in
/// `registry.rs`. This test needs only ~100 inserts per node to confirm the
/// cross-node range-disjointness the HiLo design guarantees.
///
/// Steps:
///   1. Spin up a 3-node cluster.
///   2. Create a `document_strict` collection (each INSERT triggers a surrogate
///      allocation through `SurrogateAssigner::assign`).
///   3. Insert `KEYS_PER_NODE` distinct keys via node 0 (prefix `n0`).
///   4. Insert `KEYS_PER_NODE` distinct keys via node 1 (prefix `n1`).
///   5. Wait for both nodes' catalogs to reach the expected row count.
///   6. Read the complete surrogate catalog from both nodes.
///   7. Assert global uniqueness (A), no-zero (B), and disjointness (C).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hilo_surrogate_globally_unique_and_disjoint_across_nodes() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    // ── Step 1: DDL ───────────────────────────────────────────────────────────
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");

    // ── Step 2: Insert distinct keys on node 0 ────────────────────────────────
    insert_batch(&cluster.nodes[0].client, COLLECTION, "n0", KEYS_PER_NODE).await;

    // ── Step 3: Insert distinct keys on node 1 ────────────────────────────────
    insert_batch(&cluster.nodes[1].client, COLLECTION, "n1", KEYS_PER_NODE).await;

    // ── Step 4: Wait for catalogs to reflect both batches ─────────────────────
    // The catalog is written on the inserting node synchronously (before the
    // INSERT response is returned), so both nodes should already have their own
    // entries. We wait up to 10 s for each node's count to reach the expected
    // value to absorb any scheduling jitter.
    for (idx, node) in cluster.nodes[..2].iter().enumerate() {
        let node_prefix = if idx == 0 { "n0" } else { "n1" };
        wait_for(
            &format!("node {idx} catalog has {KEYS_PER_NODE} surrogates for prefix {node_prefix}"),
            Duration::from_secs(10),
            Duration::from_millis(100),
            || {
                let bindings = read_catalog_surrogates(&node.shared, COLLECTION);
                let prefix_count = bindings
                    .iter()
                    .filter(|(pk, _)| pk.starts_with(node_prefix))
                    .count();
                prefix_count >= KEYS_PER_NODE as usize
            },
        )
        .await;
    }

    // ── Step 5: Collect all surrogates from both nodes ────────────────────────
    let node0_bindings = read_catalog_surrogates(&cluster.nodes[0].shared, COLLECTION);
    let node1_bindings = read_catalog_surrogates(&cluster.nodes[1].shared, COLLECTION);

    // Each node only has its own prefix keys in its local catalog. The surrogate
    // assigner writes a catalog row on the node that received the INSERT; binding
    // replication is WAL-replay-based, not an immediate Raft write. So node 0
    // holds all `n0_*` bindings and node 1 holds all `n1_*` bindings; we union
    // them to form the cluster-wide view.
    let node0_set: HashSet<u32> = node0_bindings
        .iter()
        .filter(|(pk, _)| pk.starts_with("n0"))
        .map(|(_, s)| *s)
        .collect();

    let node1_set: HashSet<u32> = node1_bindings
        .iter()
        .filter(|(pk, _)| pk.starts_with("n1"))
        .map(|(_, s)| *s)
        .collect();

    // Sanity: each node should have exactly KEYS_PER_NODE bindings.
    assert_eq!(
        node0_set.len(),
        KEYS_PER_NODE as usize,
        "node 0 catalog should have exactly {KEYS_PER_NODE} n0-prefixed bindings; got {}",
        node0_set.len()
    );
    assert_eq!(
        node1_set.len(),
        KEYS_PER_NODE as usize,
        "node 1 catalog should have exactly {KEYS_PER_NODE} n1-prefixed bindings; got {}",
        node1_set.len()
    );

    // ── Assertion (A): global uniqueness ─────────────────────────────────────
    // If any two keys (across both nodes) share a surrogate, the old per-node
    // `fetch_add` allocator is present and the HiLo reserve path is broken.
    let mut all_surrogates: Vec<u32> = node0_set.iter().chain(node1_set.iter()).copied().collect();
    let total_seen = all_surrogates.len();
    all_surrogates.sort_unstable();
    let mut deduped = all_surrogates.clone();
    deduped.dedup();
    let unique_count = deduped.len();

    let duplicates: Vec<u32> = all_surrogates
        .windows(2)
        .filter(|w| w[0] == w[1])
        .map(|w| w[0])
        .collect::<HashSet<u32>>()
        .into_iter()
        .take(10)
        .collect();

    assert_eq!(
        unique_count, total_seen,
        "S4a-0 FAILED: cluster-wide surrogate collision detected. \
         {total_seen} assignments but only {unique_count} unique values. \
         Duplicate surrogates (first 10): {duplicates:?}"
    );

    let total_expected = (KEYS_PER_NODE * 2) as usize;
    assert_eq!(
        unique_count, total_expected,
        "S4a-0: expected {total_expected} unique surrogates cluster-wide, found {unique_count}"
    );

    // ── Assertion (B): no Surrogate::ZERO ────────────────────────────────────
    assert!(
        deduped.iter().all(|&s| s != 0),
        "S4a-0 FAILED: Surrogate::ZERO (0) was issued to a real key. \
         Surrogate::ZERO is reserved and must never be handed to user data."
    );

    // ── Assertion (C): disjoint ranges ───────────────────────────────────────
    // Node 0 and node 1 each reserve a distinct ~4096-wide slice from the
    // global Raft counter, so their assigned surrogates must never overlap.
    let intersection: HashSet<u32> = node0_set.intersection(&node1_set).copied().collect();
    assert!(
        intersection.is_empty(),
        "S4a-0 FAILED: node 0 and node 1 surrogate ranges overlap. \
         Colliding surrogates (first 10): {:?}. \
         This means the HiLo Raft-reserve path is not being used and nodes \
         are allocating from independent per-node counters.",
        intersection.iter().take(10).collect::<Vec<_>>()
    );

    // ── Shutdown ──────────────────────────────────────────────────────────────
    cluster.shutdown().await;
}
