// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: DROP RLS POLICY ... IF EXISTS must succeed silently
//! when the named policy is absent rather than returning error 42P01/42704.

use crate::harness::TestServer;

/// Dropping a non-existent RLS policy with IF EXISTS must return success (not an error).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_rls_policy_if_exists_succeeds_when_absent() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION rls_ifex_coll (\
                id TEXT PRIMARY KEY, \
                val INT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    // Policy does not exist yet — IF EXISTS must not error.
    let result = server
        .exec("DROP RLS POLICY ghost_policy ON rls_ifex_coll IF EXISTS")
        .await;
    assert!(
        result.is_ok(),
        "DROP RLS POLICY IF EXISTS on absent policy should succeed: {result:?}"
    );

    // Create the policy, then drop it for real, then drop again with IF EXISTS.
    server
        .exec("CREATE RLS POLICY real_policy ON rls_ifex_coll FOR READ USING (val > 0)")
        .await
        .unwrap();

    server
        .exec("DROP RLS POLICY real_policy ON rls_ifex_coll IF EXISTS")
        .await
        .unwrap();

    // Second drop — policy is now gone; IF EXISTS must still succeed.
    let result = server
        .exec("DROP RLS POLICY real_policy ON rls_ifex_coll IF EXISTS")
        .await;
    assert!(
        result.is_ok(),
        "second DROP RLS POLICY IF EXISTS after deletion should succeed: {result:?}"
    );
}
