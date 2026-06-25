// SPDX-License-Identifier: BUSL-1.1

//! Production Raft-snapshot builder→applier round-trip, single process.
//!
//! This drives the REAL snapshot SEND/RECEIVE path end to end without a live
//! cluster:
//!
//! 1. A SOURCE single-node `TestServer` creates a strict-document collection
//!    and inserts rows over pgwire, so surrogates are allocated and the
//!    pk→surrogate bindings land in the source catalog.
//! 2. The production [`DataPlaneSnapshotBuilder`] builds a group snapshot
//!    (group-filtered `TenantDataSnapshot` bytes).
//! 3. A FRESH TARGET `TestServer` pre-creates the identical schema, then the
//!    production [`DataPlaneSnapshotApplier`] installs the bytes.
//! 4. The target is verified through the normal query paths: `COUNT(*)`, a PK
//!    point-lookup (which exercises pk→surrogate resolution against the target
//!    catalog — proving the applier rebound the binding), and a direct catalog
//!    surrogate-equality check against the source.
//!
//! Routing: a single-node `TestServer` is normally `cluster_routing == None`,
//! which makes the builder ship an empty snapshot. Both nodes are started with
//! `RoutingTable::uniform(1, &[1], 1)`. With one data group, every vShard maps
//! to data group `1` (group `0` is metadata and owns no vShards), so the test
//! collection's vShard is guaranteed to land in group `1` — the group built and
//! applied here. The test asserts this membership explicitly.

mod common;

use common::pgwire_harness::TestServer;

use nodedb::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
use nodedb::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder;
use nodedb::types::TenantId;
use nodedb_cluster::SnapshotApplier;
use nodedb_cluster::SnapshotBuilder;
use nodedb_cluster::routing::RoutingTable;
use nodedb_cluster::routing::vshard_for_collection;
use nodedb_types::id::DatabaseId;

/// The single data group every vShard maps into under `uniform(1, ..)`.
const DATA_GROUP_ID: u64 = 1;

/// Extract the first column of the first `Row` message.
fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

/// Build a uniform single-data-group routing table for a single node.
fn single_node_routing() -> RoutingTable {
    RoutingTable::uniform(1, &[1], 1)
}

#[tokio::test]
async fn snapshot_round_trip_builder_to_applier() {
    const COLL: &str = "snap_rt_docs";
    let pks = ["pk0", "pk1", "pk2", "pk3", "pk4"];

    // ── Sanity: the collection's vShard belongs to the data group we build. ───
    let vshard = vshard_for_collection(DatabaseId::DEFAULT, COLL);
    let routing = single_node_routing();
    assert!(
        routing.vshards_for_group(DATA_GROUP_ID).contains(&vshard),
        "collection vShard {vshard} must belong to data group {DATA_GROUP_ID}"
    );

    // ── SOURCE node: create collection + insert rows over pgwire. ─────────────
    let source = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*source.client;
        client
            .simple_query(&format!(
                "CREATE COLLECTION {COLL} \
                 (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')"
            ))
            .await
            .expect("CREATE COLLECTION on source");
        for pk in pks {
            client
                .simple_query(&format!(
                    "INSERT INTO {COLL} (id, val) VALUES ('{pk}', 'v_{pk}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT {pk} on source: {e}"));
        }
    }

    // ── Discover the tenant the inserts actually bound under. ─────────────────
    // The pgwire connection resolves to a concrete tenant; rather than hard-code
    // it, read it from the source catalog so the surrogate assertions use the
    // exact tenant the builder captured.
    let source_catalog = source
        .shared
        .credentials
        .catalog()
        .as_ref()
        .expect("source catalog present")
        .clone();
    let tenant_id = source_catalog
        .load_all_collections(DatabaseId::DEFAULT)
        .expect("load source collections")
        .into_iter()
        .find(|c| c.is_active && c.name == COLL)
        .map(|c| c.tenant_id)
        .expect("source collection descriptor present");
    let tid = TenantId::new(tenant_id);

    // The source must have a surrogate binding for pk0 (proves inserts allocated
    // identities the snapshot will carry).
    let source_surrogate = source_catalog
        .get_surrogate_for_pk(DatabaseId::DEFAULT, tid, COLL, pks[0].as_bytes())
        .expect("source get_surrogate_for_pk")
        .expect("source must have a surrogate for pk0");

    // ── Build the group snapshot via the PRODUCTION builder. ──────────────────
    let builder = DataPlaneSnapshotBuilder::new(source.shared.clone());
    let bytes = builder
        .build_group_snapshot(DATA_GROUP_ID, 0, 0)
        .await
        .expect("build_group_snapshot");
    assert!(
        !bytes.is_empty(),
        "production builder must produce a non-empty group snapshot"
    );

    // ── TARGET node: fresh server, same routing, identical schema pre-created. ─
    let target = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*target.client;
        client
            .simple_query(&format!(
                "CREATE COLLECTION {COLL} \
                 (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')"
            ))
            .await
            .expect("CREATE COLLECTION on target");
    }

    // ── Apply via the PRODUCTION applier. ─────────────────────────────────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    applier
        .apply_snapshot(DATA_GROUP_ID, &bytes)
        .await
        .expect("apply_snapshot");

    // ── Verify on the TARGET through the normal query paths. ──────────────────
    let client = &*target.client;

    // (a) All inserted rows are present.
    let count_msgs = client
        .simple_query(&format!("SELECT COUNT(*) FROM {COLL}"))
        .await
        .expect("SELECT COUNT(*) on target");
    assert_eq!(
        first_value(&count_msgs).as_deref(),
        Some(pks.len().to_string().as_str()),
        "target must contain all {} snapshot-installed rows",
        pks.len()
    );

    // (b) PK point-lookup resolves — exercises pk→surrogate resolution against
    //     the target catalog, proving the applier rebound the binding.
    let lookup_msgs = client
        .simple_query(&format!("SELECT val FROM {COLL} WHERE id = '{}'", pks[0]))
        .await
        .expect("SELECT val WHERE id = pk0 on target");
    assert_eq!(
        first_value(&lookup_msgs).as_deref(),
        Some(format!("v_{}", pks[0]).as_str()),
        "PK point-lookup on target must return the snapshot-installed value"
    );

    // (c) Direct catalog check: the target's surrogate for pk0 equals the
    //     source's — the identity map travelled with the data group and was
    //     rebound on apply.
    let target_catalog = target
        .shared
        .credentials
        .catalog()
        .as_ref()
        .expect("target catalog present")
        .clone();
    let target_surrogate = target_catalog
        .get_surrogate_for_pk(DatabaseId::DEFAULT, tid, COLL, pks[0].as_bytes())
        .expect("target get_surrogate_for_pk")
        .expect("target must have a rebound surrogate for pk0");
    assert_eq!(
        target_surrogate, source_surrogate,
        "rebound target surrogate must equal the source surrogate for pk0"
    );
}
