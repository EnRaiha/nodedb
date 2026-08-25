// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: DROP CONSUMER GROUP ... IF EXISTS must succeed silently
//! when the named group is absent rather than returning a string error.

use crate::harness::TestServer;

/// Dropping a non-existent consumer group with IF EXISTS must return success (not an error).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_consumer_group_if_exists_succeeds_when_absent() {
    let server = TestServer::start().await;

    // Create a change stream to have something to attach a group to.
    server
        .exec(
            "CREATE COLLECTION cg_ifex_coll (\
                id TEXT PRIMARY KEY, \
                val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    server
        .exec("CREATE CHANGE STREAM cg_ifex_stream ON cg_ifex_coll")
        .await
        .unwrap();

    // Group does not exist yet — IF EXISTS must not error.
    let result = server
        .exec("DROP CONSUMER GROUP ghost_group ON cg_ifex_stream IF EXISTS")
        .await;
    assert!(
        result.is_ok(),
        "DROP CONSUMER GROUP IF EXISTS on absent group should succeed: {result:?}"
    );

    // Create the group, drop it, then drop again with IF EXISTS.
    server
        .exec("CREATE CONSUMER GROUP real_group ON cg_ifex_stream")
        .await
        .unwrap();

    server
        .exec("DROP CONSUMER GROUP real_group ON cg_ifex_stream IF EXISTS")
        .await
        .unwrap();

    // Second drop — group is now gone; IF EXISTS must still succeed.
    let result = server
        .exec("DROP CONSUMER GROUP real_group ON cg_ifex_stream IF EXISTS")
        .await;
    assert!(
        result.is_ok(),
        "second DROP CONSUMER GROUP IF EXISTS after deletion should succeed: {result:?}"
    );
}
