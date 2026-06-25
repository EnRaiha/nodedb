// SPDX-License-Identifier: BUSL-1.1

//! Follower boot-restore of a persisted Raft snapshot, single process.
//!
//! This proves that the boot-time `.snap` reload re-installs engine state that
//! was NEVER in the restored node's own WAL:
//!
//! 1. A SOURCE single-node `TestServer` creates a strict-document collection
//!    and inserts rows over pgwire, so the production
//!    [`DataPlaneSnapshotBuilder`] can serialize a group snapshot.
//! 2. A TARGET `TestServer` pre-creates the identical schema but inserts NO
//!    rows — so the rows are absent from the target's WAL. The source's
//!    snapshot bytes are written to `<target_data_dir>/recv_snapshots/
//!    <DATA_GROUP_ID>.snap`, simulating a committed install-snapshot left by a
//!    prior run.
//! 3. The REAL boot-restore entry point
//!    [`restore_persisted_snapshots`] is invoked directly against the target,
//!    simulating the startup hook in `start_raft`.
//! 4. The target is verified through pgwire: every row is present even though it
//!    was never inserted on the target — its presence can only come from the
//!    persisted `.snap`, not WAL replay.

mod common;

use common::pgwire_harness::TestServer;

use nodedb::control::cluster::boot_restore::restore_persisted_snapshots;
use nodedb::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
use nodedb::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder;
use nodedb_cluster::SnapshotBuilder;

mod snapshot_rt_common;
use snapshot_rt_common::{DATA_GROUP_ID, first_value, single_node_routing};

#[tokio::test]
async fn boot_restore_reinstalls_persisted_snapshot() {
    const COLL: &str = "boot_restore_docs";
    let pks = ["a1", "a2", "a3"];

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

    // ── TARGET node: fresh server, same routing, identical schema — NO rows. ──
    // No rows are inserted, so the snapshot rows are absent from the target's
    // own WAL: their later presence can only be explained by boot-restore.
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

    // ── Obtain the TARGET's live data directory. ──────────────────────────────
    // `take_dir` returns the still-running server plus a handle to the data dir
    // it actually loaded from (the swap only replaces the deletion-on-drop
    // guard). Keep `target_dir` alive for the rest of the test so the files
    // survive.
    let (target, target_dir) = target.take_dir();
    let recv_dir = target_dir.path().join("recv_snapshots");
    std::fs::create_dir_all(&recv_dir).expect("create recv_snapshots dir");
    let snap_path = recv_dir.join(format!("{DATA_GROUP_ID}.snap"));
    std::fs::write(&snap_path, &bytes).expect("write persisted .snap");

    // ── Invoke the REAL boot-restore entry point against the target. ──────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    let restored = restore_persisted_snapshots(target_dir.path(), &applier)
        .await
        .expect("boot restore");
    assert_eq!(restored, 1, "boot-restore must apply exactly one snapshot");

    // ── Verify on the TARGET: each row is present via per-key point lookups. ──
    // The rows were never inserted on the target, so their presence proves
    // boot-restore re-installed the persisted `.snap`.
    let client = &*target.client;
    for pk in pks {
        let msgs = client
            .simple_query(&format!("SELECT val FROM {COLL} WHERE id = '{pk}'"))
            .await
            .unwrap_or_else(|e| panic!("SELECT val WHERE id = {pk} on target: {e}"));
        assert_eq!(
            first_value(&msgs).as_deref(),
            Some(format!("v_{pk}").as_str()),
            "boot-restored row {pk} must be present with its snapshot value"
        );
    }
}
