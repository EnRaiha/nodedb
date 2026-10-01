// SPDX-License-Identifier: BUSL-1.1

//! A restore raises the target's surrogate floor above every restored
//! surrogate, so a row inserted after it never reuses one.
//!
//! The source and the fresh target both issue surrogates from 1, so without
//! the raise the target's first new rows take the restored rows' surrogates.
//! New rows are inserted through every node, so each node's allocator is
//! exercised after the restore.

use std::collections::BTreeMap;
use std::time::Duration;

use nodedb::types::{DatabaseId, TenantId};
use nodedb_types::CollectionKey;

use crate::common::cluster_harness::shared_steps::{db_detail, drain_backup, push_restore};
use crate::common::cluster_harness::{TestCluster, wait_for, wait_for_async};

const TENANT: u64 = 1;
const DOCS: &str = "sf_docs";
const ROWS: usize = 20;

/// The surrogate the nodes of `cluster` bind `pk` to, `None` while no node
/// binds it. Two nodes disagreeing fails the test.
fn bound(cluster: &TestCluster, pk: &str) -> Option<u32> {
    let mut seen: Option<u32> = None;
    for node in &cluster.nodes {
        let s = node
            .shared
            .surrogate_assigner
            .lookup_bound(
                CollectionKey::from_bare(DatabaseId::DEFAULT, DOCS),
                TenantId::new(TENANT),
                pk.as_bytes(),
            )
            .expect("surrogate lookup")
            .map(|s| s.as_u32());
        if let Some(s) = s {
            assert!(
                seen.is_none_or(|prev| prev == s),
                "nodes bind {pk} to different surrogates: {seen:?} and {s}"
            );
            seen = Some(s);
        }
    }
    seen
}

async fn insert(cluster: &TestCluster, node_idx: usize, pk: &str) {
    cluster.nodes[node_idx]
        .client
        .simple_query(&format!("INSERT INTO {DOCS} (id, v) VALUES ('{pk}', 'x')"))
        .await
        .unwrap_or_else(|e| panic!("insert {pk} on node {node_idx}: {}", db_detail(&e)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rows_inserted_after_a_restore_reuse_no_restored_surrogate() {
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
        insert(&source, i % source.nodes.len(), &format!("old{i}")).await;
    }
    source
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    let bytes = drain_backup(&source.nodes[0].client, TENANT).await;
    source.shutdown().await;

    let target = TestCluster::spawn_three().await.expect("target cluster");
    push_restore(&target.nodes[0].client, TENANT, bytes).await;
    target
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    let target_ref = &target;
    wait_for_async(
        "every restored row is bound on the target",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async move { (0..ROWS).all(|i| bound(target_ref, &format!("old{i}")).is_some()) },
    )
    .await;
    let restored: BTreeMap<u32, String> = (0..ROWS)
        .map(|i| {
            let pk = format!("old{i}");
            (bound(&target, &pk).expect("restored bind"), pk)
        })
        .collect();
    assert_eq!(
        restored.len(),
        ROWS,
        "restored rows keep distinct surrogates"
    );
    let floor = restored.keys().copied().max().unwrap_or(0);

    for i in 0..ROWS {
        insert(&target, i % target.nodes.len(), &format!("new{i}")).await;
    }
    target
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    for i in 0..ROWS {
        let pk = format!("new{i}");
        let s = bound(&target, &pk).unwrap_or_else(|| panic!("{pk} is bound"));
        assert!(
            !restored.contains_key(&s),
            "{pk} reuses surrogate {s} of restored row {}",
            restored[&s]
        );
        assert!(
            s > floor,
            "{pk} got surrogate {s}, at or below the restored floor {floor}"
        );
    }

    target.shutdown().await;
}
