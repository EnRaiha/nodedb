// SPDX-License-Identifier: BUSL-1.1
//! Group 0 durable applied floor across a restart.
//!
//! A graceful WAL-only restart of a single-node metadata group resumes
//! delivery above the saved floor. The catalog, descriptor leases, drains,
//! and pending DDL records come back from their `SystemCatalog` rows, and no
//! catalog entry below the floor is applied again.

use crate::common;

use common::cluster_harness::TestClusterNode;
use common::cluster_harness::shared_steps::{propose_and_apply, wait_for_single_node_ready};

use nodedb_cluster::{DescriptorId, DescriptorKind, DescriptorLease, DrainOwner, MetadataEntry};
use nodedb_types::{DatabaseId, Hlc};

const TENANT: u64 = 1;
const COLLECTION: &str = "mfr_orders";
const PENDING_TOKEN: u64 = 9_001;

fn far_future() -> Hlc {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    Hlc::new(now_ns + 3_600_000_000_000, 0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn host_state_survives_restart_above_the_floor() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let data_path = data_dir.path().to_path_buf();
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path.clone())
        .await
        .expect("spawn single-node metadata group");
    wait_for_single_node_ready(&node).await;

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {COLLECTION} (id TEXT PRIMARY KEY, val BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create collection");

    let leased = DescriptorId::new(0, TENANT, DescriptorKind::Collection, "mfr_leased");
    let drained = DescriptorId::new(0, TENANT, DescriptorKind::Collection, "mfr_drained");
    let lease = DescriptorLease {
        descriptor_id: leased.clone(),
        version: 1,
        node_id: node.node_id,
        expires_at: far_future(),
    };
    let owner = DrainOwner::MoveTenant {
        tenant_id: TENANT,
        source_db_id: 0,
    };
    propose_and_apply(&node, &MetadataEntry::DescriptorLeaseGrant(lease.clone())).await;
    propose_and_apply(
        &node,
        &MetadataEntry::DescriptorDrainStart {
            descriptor_id: drained.clone(),
            up_to_version: 5,
            expires_at: far_future(),
            proposer_node_id: node.node_id,
            owner: owner.clone(),
        },
    )
    .await;
    // A pending propose reserves only under the DDL preparation owner's token.
    propose_and_apply(
        &node,
        &MetadataEntry::DdlPrepareAcquire {
            token: PENDING_TOKEN,
            node_id: node.node_id,
        },
    )
    .await;
    propose_and_apply(
        &node,
        &MetadataEntry::DdlPendingPropose {
            token: PENDING_TOKEN,
            objects: Vec::new(),
            proposed_at: far_future(),
        },
    )
    .await;

    node.graceful_shutdown_wal_only().await;
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_path)
        .await
        .expect("restart against the persisted metadata log");
    wait_for_single_node_ready(&node).await;

    assert!(
        node.shared
            .credentials
            .catalog()
            .get_collection(DatabaseId::DEFAULT, TENANT, COLLECTION)
            .expect("read collection")
            .is_some(),
        "the collection row survives the restart"
    );
    {
        let cache = node
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        assert_eq!(
            cache.leases.get(&(leased.clone(), node.node_id)),
            Some(&lease),
            "the lease is seeded from its row"
        );
        assert_eq!(
            cache.catalog_entries_applied, 0,
            "no catalog entry below the floor is applied again"
        );
    }
    assert!(
        node.shared
            .lease_drain
            .snapshot()
            .iter()
            .any(|(id, held_by, _)| *id == drained && *held_by == owner),
        "the drain is seeded from its row"
    );
    assert!(
        node.shared.pending_ddl.contains(PENDING_TOKEN),
        "the pending DDL record is seeded from its row"
    );

    node.shutdown().await;
}
