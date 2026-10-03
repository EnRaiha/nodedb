// SPDX-License-Identifier: BUSL-1.1

//! A restore whose carried surrogate another key already holds fails with a
//! typed conflict error, and binds nothing.
//!
//! Binds are first-wins per key, so without the check the restored row
//! installs over the row the other key names. The target binds a restored row's
//! surrogate to a different key on every node before the restore runs.

use std::time::Duration;

use nodedb::types::{DatabaseId, TenantId};
use nodedb_types::{CollectionKey, Surrogate};

use crate::common::cluster_harness::shared_steps::{db_detail, drain_backup, try_push_restore};
use crate::common::cluster_harness::{TestCluster, wait_for};

const TENANT: u64 = 1;
const DOCS: &str = "sc_docs";
const ROWS: usize = 4;

fn key() -> CollectionKey<'static> {
    CollectionKey::from_bare(DatabaseId::DEFAULT, DOCS)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_restored_surrogate_bound_to_another_key_fails_the_restore() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    source
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {DOCS} (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "every source node sees the collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            source
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;
    for i in 0..ROWS {
        source.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {DOCS} (id, v) VALUES ('k{i}', 'v{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert k{i}: {}", db_detail(&e)));
    }
    source
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    let restored = source
        .nodes
        .iter()
        .find_map(|n| {
            n.shared
                .surrogate_assigner
                .lookup_bound(key(), TenantId::new(TENANT), b"k0")
                .ok()
                .flatten()
        })
        .expect("a source node binds k0")
        .as_u32();
    let bytes = drain_backup(&source.nodes[0].client, TENANT).await;
    source.shutdown().await;

    let target = TestCluster::spawn_three().await.expect("target cluster");
    for node in &target.nodes {
        let held = node
            .shared
            .surrogate_assigner
            .bind(
                key(),
                TenantId::new(TENANT),
                b"intruder",
                Surrogate::new(restored),
            )
            .expect("pre-existing bind");
        assert_eq!(held.as_u32(), restored);
    }

    let refused = try_push_restore(&target.nodes[0].client, TENANT, bytes)
        .await
        .expect_err("a restored surrogate another key holds fails the restore");
    for needle in [DOCS, "surrogate_identity", "'k0'", "'intruder'"] {
        assert!(
            refused.contains(needle),
            "the conflict names {needle}: {refused}"
        );
    }
    assert!(
        refused.contains(&restored.to_string()),
        "the conflict names surrogate {restored}: {refused}"
    );
    for node in &target.nodes {
        assert_eq!(
            node.shared
                .surrogate_assigner
                .lookup_bound(key(), TenantId::new(TENANT), b"k0")
                .expect("lookup"),
            None,
            "node {} binds nothing of the refused restore",
            node.node_id
        );
    }

    target.shutdown().await;
}
