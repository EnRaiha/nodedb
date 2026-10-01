// SPDX-License-Identifier: BUSL-1.1

//! Integration test: a joiner advertising a different `WIRE_BUILD_ID` is
//! refused by `handle_join_request` and never joins the cluster.
//!
//! Exercises the config-level `wire_build_id` override end to end: the
//! joiner's `JoinRequest` carries the overridden build, the seed's
//! `handle_join_request` compares it against its own real
//! `nodedb_types::wire_version::WIRE_BUILD_ID`, and rejects.

use std::time::Duration;

use super::cluster_common::TestNode;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joiner_with_mismatched_build_id_is_refused() {
    let node1 = TestNode::spawn(1, vec![]).await.expect("node 1 bootstrap");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let seeds = vec![node1.listen_addr()];
    let result = TestNode::spawn_with_wire_build_id(2, seeds, "some-other-build".to_owned()).await;

    assert!(
        result.is_err(),
        "joiner with a mismatched build_id must fail to join"
    );

    // The seed's topology must not have admitted the rejected joiner.
    assert_eq!(node1.topology_size(), 1);

    node1.shutdown().await;
}
