// SPDX-License-Identifier: BUSL-1.1
//! CRDT constraint sets are delivered to every replica's per-core validator.
//!
//! ## What this guards
//!
//! A collection's UNIQUE / NOT NULL constraints are derived from the catalog
//! and must be installed into every data-group replica's CRDT validator — the
//! component that rejects constraint-violating peer deltas at commit. The
//! metadata leader's reconcile loop re-derives each collection's constraint set
//! and replicates it via a `ConstraintChange` entry on the collection's vshard
//! data Raft log; each replica installs it, fenced by `constraint_version`.
//!
//! Reading the catalog row on a follower would only prove the catalog
//! replicated — NOT that the validator installed. These tests read the
//! validator itself through `crdt_constraints`, which dispatches a
//! `CrdtOp::ReadConstraints` to each node's local data core.
//!
//! Three scenarios:
//!   1. happy path — CREATE + UNIQUE index converges on every replica.
//!   2. alter — a later schema mutation grows the set on every replica.
//!   3. crash recovery — killing the metadata leader leaves the set installed
//!      on every survivor; a new metadata leader takes over reconciliation.
//!      This is what justifies a recurring reconcile loop over a one-shot
//!      create-time hook: leadership can move, and the surviving cluster must
//!      still converge without the original proposer alive.

mod common;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};

use std::time::{Duration, Instant};

use nodedb_crdt::{Constraint, ConstraintKind};
use nodedb_types::TenantId;

/// Default tenant id every cluster-test DDL is created under.
const TENANT: u64 = 1;
const COLL: &str = "t";

/// Constraint set sorted by name — the deterministic order
/// `collection_constraints` emits, so comparisons are order-stable.
fn sorted(mut v: Vec<Constraint>) -> Vec<Constraint> {
    v.sort_by(|a, b| a.name.cmp(&b.name));
    v
}

fn not_null(name: &str, field: &str) -> Constraint {
    Constraint {
        name: name.to_string(),
        collection: COLL.to_string(),
        field: field.to_string(),
        kind: ConstraintKind::NotNull,
    }
}

fn unique(name: &str, field: &str) -> Constraint {
    Constraint {
        name: name.to_string(),
        collection: COLL.to_string(),
        field: field.to_string(),
        kind: ConstraintKind::Unique,
    }
}

/// Bounded poll until every node in `nodes` reports exactly `expected` for
/// `COLL`. On timeout, panics with the per-node observed set so a failure
/// names which replica diverged and how.
async fn await_constraints_on(nodes: &[TestClusterNode], expected: &[Constraint]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut converged = true;
        let mut observed: Vec<(u64, Vec<Constraint>)> = Vec::new();
        for node in nodes {
            let got = sorted(node.crdt_constraints(TenantId::new(TENANT), COLL).await);
            if got != expected {
                converged = false;
            }
            observed.push((node.node_id, got));
        }
        if converged {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "constraint set did not converge on every replica for '{COLL}' within 30s\n\
                 expected: {expected:?}\n\
                 observed per node: {observed:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constraints_delivered_to_every_replica() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION t \
             (id TEXT PRIMARY KEY, email TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION t");
    cluster
        .exec_ddl_on_any_leader("CREATE UNIQUE INDEX t_email_uniq ON t (email)")
        .await
        .expect("CREATE UNIQUE INDEX t_email_uniq");

    // id PRIMARY KEY → NOT NULL; email NOT NULL; unique index → UNIQUE(email).
    let expected = vec![
        not_null("t_email_notnull", "email"),
        unique("t_email_uniq", "email"),
        not_null("t_id_notnull", "id"),
    ];
    await_constraints_on(&cluster.nodes, &expected).await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constraint_set_grows_on_alter() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION t \
             (id TEXT PRIMARY KEY, email TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION t");

    let initial = vec![
        not_null("t_email_notnull", "email"),
        not_null("t_id_notnull", "id"),
    ];
    await_constraints_on(&cluster.nodes, &initial).await;

    // A later schema mutation (adding a UNIQUE index) bumps the descriptor
    // version; the reconcile loop must re-deliver the larger set everywhere.
    cluster
        .exec_ddl_on_any_leader("CREATE UNIQUE INDEX t_email_uniq ON t (email)")
        .await
        .expect("CREATE UNIQUE INDEX t_email_uniq");

    let grown = vec![
        not_null("t_email_notnull", "email"),
        unique("t_email_uniq", "email"),
        not_null("t_id_notnull", "id"),
    ];
    await_constraints_on(&cluster.nodes, &grown).await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constraints_survive_metadata_leader_crash() {
    let mut cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION t \
             (id TEXT PRIMARY KEY, email TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION t");

    let expected = vec![
        not_null("t_email_notnull", "email"),
        not_null("t_id_notnull", "id"),
    ];
    // Every replica installs the set before the crash.
    await_constraints_on(&cluster.nodes, &expected).await;

    // Identify and kill the metadata-group leader — the node running the
    // reconcile loop. A survivor must take over and keep the cluster converged.
    let old_leader = cluster.nodes[0].metadata_group_leader();
    assert!(
        old_leader != 0,
        "expected a stable metadata leader before crash"
    );
    let leader_idx = cluster
        .nodes
        .iter()
        .position(|n| n.node_id == old_leader)
        .expect("metadata leader node present in cluster");
    cluster.nodes.remove(leader_idx).shutdown().await;

    // Survivors re-elect a new, different metadata leader.
    wait_for_async(
        "a new metadata leader is elected among survivors",
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async {
            let l = cluster.nodes[0].metadata_group_leader();
            l != 0 && l != old_leader
        },
    )
    .await;

    // Every surviving replica still holds the constraint set; the new leader's
    // reconcile loop keeps re-deriving and re-delivering it.
    await_constraints_on(&cluster.nodes, &expected).await;

    cluster.shutdown().await;
}
