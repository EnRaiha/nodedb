// SPDX-License-Identifier: BUSL-1.1
//! A native-protocol write, which never runs the SQL planner, is refused while
//! its collection is under a descriptor drain, and succeeds once the drain
//! ends.
//!
//! Every write entry point takes a descriptor lease on the collections it
//! touches. The lease acquire refuses a drained descriptor, so the drain gates
//! non-SQL writes the same way it gates planned ones.

use std::sync::Arc;
use std::time::Duration;

use nodedb_client::NodeDb;
use nodedb_cluster::{DescriptorId, DescriptorKind};
use nodedb_types::document::Document;

use crate::common;
use common::cluster_harness::{TestCluster, wait_for};

const TENANT: u64 = 1;
const COLLECTION: &str = "drain_gate_docs";
const WAIT_BUDGET: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(20);

fn doc(id: &str) -> Document {
    let mut doc = Document::new(id);
    doc.set("body", nodedb_types::Value::String(id.to_string()));
    doc
}

async fn count(cluster: &TestCluster) -> String {
    let messages = cluster.nodes[0]
        .client
        .simple_query(&format!("SELECT COUNT(*) FROM {COLLECTION}"))
        .await
        .expect("count");
    messages
        .into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .expect("a count row")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_write_is_refused_during_a_drain() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION");

    let version = cluster.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_collection(nodedb_types::DatabaseId::DEFAULT, TENANT, COLLECTION)
        .expect("catalog read")
        .expect("collection exists")
        .descriptor_version
        .max(1);
    let id = DescriptorId::new(
        0,
        TENANT,
        DescriptorKind::Collection,
        COLLECTION.to_string(),
    );

    // Start the drain through the metadata group, as a DDL does.
    let shared = Arc::clone(&cluster.nodes[0].shared);
    let start_id = id.clone();
    tokio::spawn(async move {
        let now = shared.hlc_clock.now();
        let entry = nodedb_cluster::MetadataEntry::DescriptorDrainStart {
            descriptor_id: start_id,
            up_to_version: version,
            expires_at: nodedb_types::Hlc::new(now.wall_ns.saturating_add(60_000_000_000), 0),
            proposer_node_id: shared.node_id,
            owner: nodedb_cluster::DrainOwner::Ddl,
        };
        let handle = shared.metadata_raft.get().expect("metadata raft handle");
        handle
            .propose_async(nodedb_cluster::encode_entry(&entry).expect("encode"))
            .await
            .expect("propose drain start");
    })
    .await
    .expect("join");
    wait_for("every node observes the drain", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| n.has_drain_for(&id, version))
    })
    .await;

    let native = cluster.nodes[1].native_client();
    let refused = native.document_put(COLLECTION, doc("during")).await;
    assert!(
        refused.is_err(),
        "a native write to a drained collection must be refused"
    );
    // A read takes a lease on the collection too, so the drain refuses it as
    // well. The count after the drain ends shows the refused write never
    // landed.

    // End the drain. The same write lands.
    let shared = Arc::clone(&cluster.nodes[0].shared);
    nodedb::control::lease::end_drain_async(&shared, id.clone(), nodedb_cluster::DrainOwner::Ddl)
        .await
        .expect("end drain");
    wait_for("every node clears the drain", WAIT_BUDGET, POLL, || {
        cluster.nodes.iter().all(|n| !n.has_drain_for(&id, version))
    })
    .await;

    native
        .document_put(COLLECTION, doc("after"))
        .await
        .expect("a native write after the drain must land");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    assert_eq!(
        count(&cluster).await,
        "1",
        "only the write after the drain lands"
    );

    cluster.shutdown().await;
}
