// SPDX-License-Identifier: BUSL-1.1

//! Clear-then-install proof for the production Raft-snapshot applier.
//!
//! A lagging follower's LOCAL catalog can still list collections + keys that
//! were dropped/deleted on the leader after the follower's lag point. The
//! applier resolves the target group's vshards from local routing, clears every
//! in-group collection BEFORE installing the snapshot, then installs only the
//! survivors the snapshot carries. This binary proves stale state is removed:
//!
//! - A stale KEY (`a4`) present on the target but absent from the snapshot is
//!   gone after apply (cleared, not reinstalled).
//! - A dropped COLLECTION (`B`) present on the target but absent from the
//!   snapshot has its data gone after apply.
//! - A SURVIVOR key (`a1`) is intact (cleared-then-reinstalled).
//!
//! Both nodes are started with `single_node_routing()`: the applier now needs
//! routing on the TARGET to resolve the group's vshards for the clear pass.

mod common;

use common::pgwire_harness::TestServer;

use nodedb::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
use nodedb::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder;
use nodedb_cluster::SnapshotApplier;
use nodedb_cluster::SnapshotBuilder;
use nodedb_cluster::routing::vshard_for_collection;
use nodedb_types::id::DatabaseId;

mod snapshot_rt_common;
use snapshot_rt_common::{DATA_GROUP_ID, first_value, single_node_routing};

#[tokio::test]
async fn snapshot_round_trip_stale_state() {
    const COLL_A: &str = "stale_rt_a";
    const COLL_B: &str = "stale_rt_b";
    const CREATE_A: &str = "CREATE COLLECTION stale_rt_a \
         (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')";
    const CREATE_B: &str = "CREATE COLLECTION stale_rt_b \
         (id TEXT PRIMARY KEY, val TEXT) WITH (engine='document_strict')";

    // ── Sanity: both collections' vShards belong to the data group we build. ──
    let routing = single_node_routing();
    for coll in [COLL_A, COLL_B] {
        let vshard = vshard_for_collection(DatabaseId::DEFAULT, coll);
        assert!(
            routing.vshards_for_group(DATA_GROUP_ID).contains(&vshard),
            "collection {coll} vShard {vshard} must belong to data group {DATA_GROUP_ID}"
        );
    }

    // ── SOURCE node: only collection A with rows a1,a2,a3 (NO B). ─────────────
    let source = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*source.client;
        client
            .simple_query(CREATE_A)
            .await
            .expect("CREATE A on source");
        for pk in ["a1", "a2", "a3"] {
            client
                .simple_query(&format!(
                    "INSERT INTO {COLL_A} (id, val) VALUES ('{pk}', 'v_{pk}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT {pk} on source: {e}"));
        }
    }

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

    // ── TARGET node: lagging follower with STALE state. Routing is REQUIRED so
    //    the applier can resolve the group's vshards for the clear pass. ───────
    let target = TestServer::start_with_routing(single_node_routing()).await;
    {
        let client = &*target.client;
        // A carries an extra stale key `a4` absent from the snapshot.
        client
            .simple_query(CREATE_A)
            .await
            .expect("CREATE A on target");
        for pk in ["a1", "a2", "a3", "a4"] {
            client
                .simple_query(&format!(
                    "INSERT INTO {COLL_A} (id, val) VALUES ('{pk}', 'v_{pk}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT {pk} into A on target: {e}"));
        }
        // B is a dropped collection absent from the snapshot entirely.
        client
            .simple_query(CREATE_B)
            .await
            .expect("CREATE B on target");
        for pk in ["b1", "b2"] {
            client
                .simple_query(&format!(
                    "INSERT INTO {COLL_B} (id, val) VALUES ('{pk}', 'v_{pk}')"
                ))
                .await
                .unwrap_or_else(|e| panic!("INSERT {pk} into B on target: {e}"));
        }
    }

    // ── Apply via the PRODUCTION applier (clear-then-install). ────────────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    applier
        .apply_snapshot(DATA_GROUP_ID, &bytes)
        .await
        .expect("apply_snapshot");

    // ── Verify on the TARGET through the normal query paths. ──────────────────
    let client = &*target.client;

    // (a) A has exactly the 3 survivors — `a4` was cleared, not reinstalled.
    let count_a = client
        .simple_query(&format!("SELECT COUNT(*) FROM {COLL_A}"))
        .await
        .expect("SELECT COUNT(*) FROM A on target");
    assert_eq!(
        first_value(&count_a).as_deref(),
        Some("3"),
        "A must contain exactly the 3 snapshot survivors (stale a4 cleared)"
    );

    // (b) The stale key `a4` is gone.
    let a4 = client
        .simple_query(&format!("SELECT val FROM {COLL_A} WHERE id = 'a4'"))
        .await
        .expect("SELECT a4 on target");
    assert_eq!(
        first_value(&a4),
        None,
        "stale key a4 must be cleared (absent from snapshot, not reinstalled)"
    );

    // (c) A survivor key `a1` is intact (cleared-then-reinstalled).
    let a1 = client
        .simple_query(&format!("SELECT val FROM {COLL_A} WHERE id = 'a1'"))
        .await
        .expect("SELECT a1 on target");
    assert_eq!(
        first_value(&a1).as_deref(),
        Some("v_a1"),
        "survivor a1 must be reinstalled with its snapshot value"
    );

    // (d) The dropped collection B has no data — its rows are gone (the snapshot
    //     omits B entirely). The collection definition may linger (catalog
    //     convergence is the metadata group's concern), so these queries resolve
    //     B without error and return no row; if the clear had not run they would
    //     return the pre-seeded values. Per-key absence is asserted directly to
    //     avoid depending on COUNT-of-empty-collection semantics.
    for pk in ["b1", "b2"] {
        let row = client
            .simple_query(&format!("SELECT val FROM {COLL_B} WHERE id = '{pk}'"))
            .await
            .unwrap_or_else(|e| panic!("SELECT {pk} FROM B on target: {e}"));
        assert_eq!(
            first_value(&row),
            None,
            "dropped collection B's row {pk} must be cleared (absent from snapshot)"
        );
    }
}
