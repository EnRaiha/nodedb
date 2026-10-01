// SPDX-License-Identifier: BUSL-1.1

//! A crashed lease holder that SWIM declares Dead stops blocking DDL once the
//! dead-holder grace passes, long before its lease expires.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};
use nodedb_cluster::DescriptorKind;

const TENANT: u64 = 1;
const WAIT_BUDGET: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(20);

/// A crashed holder stays in topology, so only its SWIM Dead verdict can end
/// its hold early. The survivors' real SWIM detectors must declare it Dead.
/// The ALTER on the metadata leader must then commit once the dead grace
/// passes, while the holder's lease still outlives the drain timeout. The
/// leader's lease GC must then release the dead holder's lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ddl_proceeds_after_dead_holder_clamp() {
    use nodedb_cluster::DEAD_HOLDER_LEASE_GRACE;

    /// Longer than the drain timeout, so only the dead-holder clamp can clear it.
    const HOLDER_LEASE: Duration = Duration::from_secs(120);
    /// Default SWIM needs a probe round plus a 3-node suspicion timeout.
    const SWIM_DEAD_BUDGET: Duration = Duration::from_secs(30);
    /// One lease-GC sweep after the ALTER commits.
    const GC_BUDGET: Duration = Duration::from_secs(10);
    /// The drain clears no earlier than the dead grace after the Dead
    /// verdict, and the ALTER starts right at that verdict. The extra time
    /// covers the drain propose, the Raft-silence check and the commit.
    const ALTER_BUDGET: Duration = DEAD_HOLDER_LEASE_GRACE.saturating_add(Duration::from_secs(15));

    let mut cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION dead_holder")
        .await
        .expect("create");
    wait_for(
        "collection stamped v1 on every node",
        WAIT_BUDGET,
        POLL,
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.collection_descriptor(TENANT, "dead_holder").map(|s| s.0) == Some(1))
        },
    )
    .await;

    // Kill a node that does not lead the metadata group, so the survivors
    // keep their metadata leader and quorum.
    let metadata_leader = cluster.nodes[0].metadata_group_leader();
    let holder_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id != metadata_leader)
        .expect("a node that does not lead the metadata group");
    let holder_id = cluster.nodes[holder_idx].node_id;
    cluster.nodes[holder_idx]
        .acquire_lease(
            DescriptorKind::Collection,
            TENANT,
            "dead_holder",
            1,
            HOLDER_LEASE,
        )
        .await
        .expect("holder acquires v1");
    wait_for(
        "every node sees the holder's lease",
        WAIT_BUDGET,
        POLL,
        || {
            cluster.nodes.iter().all(|n| {
                n.has_lease(
                    DescriptorKind::Collection,
                    TENANT,
                    "dead_holder",
                    holder_id,
                    1,
                )
            })
        },
    )
    .await;

    // A harness shutdown never releases leases, so the holder dies holding one.
    let holder = cluster.nodes.remove(holder_idx);
    holder.shutdown().await;

    let drainer_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id == metadata_leader)
        .expect("metadata leader survives");
    wait_for(
        "the metadata leader's SWIM declares the holder Dead",
        SWIM_DEAD_BUDGET,
        POLL,
        || {
            cluster.nodes[drainer_idx]
                .shared
                .lease_runtime
                .holder_liveness
                .dead_since(holder_id)
                .is_some()
        },
    )
    .await;

    let drainer = &cluster.nodes[drainer_idx];
    let existing = drainer
        .shared
        .credentials
        .catalog()
        .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, "dead_holder")
        .expect("read existing")
        .expect("exists");
    let alter_shared = Arc::clone(&drainer.shared);
    let alter_result = tokio::spawn(async move {
        let entry = nodedb::control::catalog_entry::CatalogEntry::PutCollection(Box::new(existing));
        match tokio::time::timeout(
            ALTER_BUDGET,
            nodedb::control::metadata_proposer::propose_catalog_entry_async(&alter_shared, &entry),
        )
        .await
        {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(_) => Err(format!(
                "propose_catalog_entry_async timed out after {ALTER_BUDGET:?}"
            )),
        }
    })
    .await
    .expect("join");

    assert!(
        alter_result.is_ok(),
        "ALTER must commit once the dead-holder clamp passes: {:?}",
        alter_result.err()
    );
    let dead_for = drainer
        .shared
        .lease_runtime
        .holder_liveness
        .dead_since(holder_id)
        .expect("holder still recorded Dead")
        .elapsed();
    assert!(
        dead_for >= DEAD_HOLDER_LEASE_GRACE,
        "the drain cleared {dead_for:?} after the Dead verdict, before the \
         {DEAD_HOLDER_LEASE_GRACE:?} grace"
    );

    wait_for(
        "collection stamped v2 and the dead holder's lease released",
        GC_BUDGET,
        POLL,
        || {
            cluster.nodes.iter().all(|n| {
                n.collection_descriptor(TENANT, "dead_holder").map(|s| s.0) == Some(2)
                    && n.leases_for_descriptor(DescriptorKind::Collection, TENANT, "dead_holder")
                        .iter()
                        .all(|l| l.node_id != holder_id)
            })
        },
    )
    .await;

    cluster.shutdown().await;
}
