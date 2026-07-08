// SPDX-License-Identifier: BUSL-1.1

//! 3-node sequencer leader-failover integration test.
//!
//! This test verifies that:
//! 1. Several txns commit on all replicas before the leader is killed.
//! 2. After the leader is dropped, a new leader is elected.
//! 3. The new leader's `SequencerStateMachine::last_applied_epoch` is
//!    consistent with what was committed (no regression beyond in-flight batch).
//! 4. New txns submitted to the new leader commit successfully.

mod cluster_common;

use std::time::Duration;

use nodedb_cluster::calvin::{
    sequencer::{SequencerConfig, new_inbox},
    types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass, VersionedReadSet},
};
use nodedb_types::{
    TenantId,
    id::{DatabaseId, VShardId},
};

use cluster_common::{spawn_with_sequencer, wait_for_sequencer_leader};

fn two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("col_{i}");
        let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

fn make_txclass(surr_a: u32, surr_b: u32) -> TxClass {
    let (col_a, col_b) = two_distinct_collections();
    let write_set = ReadWriteSet::new(vec![
        EngineKeySet::Document {
            collection: col_a,
            surrogates: SortedVec::new(vec![surr_a]),
        },
        EngineKeySet::Document {
            collection: col_b,
            surrogates: SortedVec::new(vec![surr_b]),
        },
    ]);
    TxClass::new(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid TxClass")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequencer_leader_failover_no_committed_epoch_loss() {
    let node_ids = vec![1u64, 2, 3];
    let mut nodes = spawn_with_sequencer(node_ids)
        .await
        .expect("spawn_with_sequencer");

    // Wait for initial sequencer leader election.
    let leader_idx =
        wait_for_sequencer_leader(&nodes, Duration::from_secs(10), Duration::from_millis(50)).await;

    let config = SequencerConfig {
        epoch_duration: Duration::from_millis(10),
        ..SequencerConfig::default()
    };

    // Submit pre-failover txns through the leader.
    let (inbox_pre, inbox_receiver_pre) = new_inbox(1024, &config);
    let (_seq_shutdown, _, _seq_handle) =
        nodes[leader_idx].start_sequencer_service(inbox_receiver_pre, config.clone());

    const PRE_FAILOVER_TXNS: u32 = 3;
    for i in 0..PRE_FAILOVER_TXNS {
        inbox_pre
            .submit(make_txclass(i * 2, i * 2 + 1))
            .expect("pre-failover submit");
    }

    // Wait for pre-failover txns to be applied on all surviving nodes.
    cluster_common::wait_for(
        "pre-failover epochs applied on all nodes",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || nodes.iter().all(|n| n.last_applied_epoch().is_some()),
    )
    .await;

    // Record the last committed epoch before failover.
    let epoch_before_failover = nodes[leader_idx]
        .last_applied_epoch()
        .expect("leader must have applied epochs");

    // Identify and kill the sequencer leader by draining the nodes vec.
    // Swap-remove puts the last element at `leader_idx` and leaves a
    // 2-node survivor slice.
    let dead_leader = nodes.swap_remove(leader_idx);
    dead_leader.shutdown().await;

    // Two surviving nodes remain.  Wait for one of them to become the new
    // sequencer leader.
    let new_leader_idx =
        wait_for_sequencer_leader(&nodes, Duration::from_secs(15), Duration::from_millis(50)).await;

    // The new leader's applied epoch must be >= the epoch recorded before
    // failover — no committed epoch may be lost across the leadership change.
    let epoch_after_failover = nodes[new_leader_idx]
        .last_applied_epoch()
        .expect("new leader must have applied epochs");

    assert!(
        epoch_after_failover >= epoch_before_failover,
        "new leader applied epoch {} < pre-failover epoch {} — committed epochs were lost",
        epoch_after_failover,
        epoch_before_failover
    );

    // Submit new txns to the new leader to verify it is functional.
    let (inbox_post, inbox_receiver_post) = new_inbox(1024, &config);
    let (_post_shutdown, _, _post_handle) =
        nodes[new_leader_idx].start_sequencer_service(inbox_receiver_post, config.clone());

    const POST_FAILOVER_TXNS: u32 = 2;
    for i in 0..POST_FAILOVER_TXNS {
        inbox_post
            .submit(make_txclass(100 + i * 2, 100 + i * 2 + 1))
            .expect("post-failover submit");
    }

    // Wait for post-failover epochs to be applied.
    let expected_min_epoch = epoch_after_failover + 1;
    cluster_common::wait_for(
        "post-failover epochs applied on new leader",
        Duration::from_secs(10),
        Duration::from_millis(20),
        || {
            nodes[new_leader_idx]
                .last_applied_epoch()
                .map(|e| e >= expected_min_epoch)
                .unwrap_or(false)
        },
    )
    .await;

    // Shut down survivors.
    for node in nodes {
        node.shutdown().await;
    }
}
