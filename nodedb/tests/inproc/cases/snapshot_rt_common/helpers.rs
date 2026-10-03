// SPDX-License-Identifier: BUSL-1.1

//! Helpers the snapshot builder→applier round-trip cases share.

use nodedb::control::security::catalog::calvin_base::CalvinBase;
use nodedb::types::TenantDataSnapshot;
use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::routing::vshard_for_collection;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::id::DatabaseId;

/// How long a server's Calvin schedulers take to keep the state of a group.
const CALVIN_KEPT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Wait until `server` keeps the Calvin state of every vShard of `group`.
///
/// A `TestServer` is a one-node Calvin cluster. A group snapshot carries the
/// Calvin cut its storage holds, so the builder refuses to build until every
/// vShard's scheduler started from a whole base and keeps it. The schedulers
/// start on the reconcile pass after the sequencer log takes its first
/// entry, which can follow the server's start.
pub async fn await_group_calvin_kept(server: &TestServer, group: u64) {
    let vshards = server
        .shared
        .cluster_routing
        .as_ref()
        .expect("a booted server has a routing table")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .vshards_for_group(group);
    nodedb_test_support::cluster_harness::wait_for_report(
        &format!("every vShard of group {group} keeps its Calvin state"),
        CALVIN_KEPT_DEADLINE,
        std::time::Duration::from_millis(20),
        || match vshards
            .iter()
            .find(|&&vshard| !CalvinBase::is_kept(server.shared.calvin.bases.base(vshard)))
        {
            Some(vshard) => Err(format!("vShard {vshard} has no kept base")),
            None => Ok(()),
        },
    )
    .await;
}

/// Extract the first column of the first `Row` message.
pub fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// The data group that homes `collection` of the default database on
/// `server`'s one-node cluster, read from its live routing table.
///
/// Every one-node cluster builds the same routing table, so a source and a
/// target server agree on it.
pub fn data_group_of(server: &TestServer, collection: &str) -> u64 {
    let vshard = vshard_for_collection(nodedb_types::CollectionKey::from_bare(
        DatabaseId::DEFAULT,
        collection,
    ));
    let routing = server
        .shared
        .cluster_routing
        .as_ref()
        .expect("a booted server has a routing table");
    let table = routing.read().unwrap_or_else(|p| p.into_inner());
    let group = table
        .group_for_vshard(vshard)
        .expect("every vShard maps to a group");
    assert_ne!(
        group, METADATA_GROUP_ID,
        "the metadata group owns no vShard"
    );
    group
}

/// Move the metadata floor of a snapshot built on one server into the
/// metadata index space of `target`.
///
/// The floor is an index in the builder's metadata group. Each `TestServer`
/// runs its own one-node cluster, so the target's metadata log counts from
/// its own start. A follower of the builder's cluster holds the builder's
/// catalog once it applied the floor. The target holds that catalog once it
/// applied the DDL the case ran on it, so the floor becomes the target's
/// metadata floor after that DDL.
///
/// Call it after the target ran its DDL. The applier still waits for the
/// floor before it installs any row.
pub fn rebase_metadata_floor(bytes: &[u8], target: &TestServer) -> Vec<u8> {
    let mut snapshot: TenantDataSnapshot =
        zerompk::from_msgpack(bytes).expect("decode group snapshot");
    assert!(
        snapshot.metadata_floor > 0,
        "the builder stamps the metadata index its catalog view holds"
    );
    snapshot.metadata_floor = target
        .shared
        .applied_index_watcher(METADATA_GROUP_ID)
        .floor();
    zerompk::to_msgpack_vec(&snapshot).expect("encode group snapshot")
}
