// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: a revoked permission-tree grant must not keep being
//! served from a stale session plan cache entry on the same connection.
//!
//! Unlike an RLS policy write (synchronous — the policy store bumps its
//! tenant version inside the DDL handler), a permission-tree grant has no
//! synchronous SQL surface. It lands as a plain `INSERT`/`DELETE` on the
//! collection named `permission_table` in the tree definition, and the
//! in-memory `PermissionCache` is updated asynchronously off `WriteEvent`s
//! consumed by the Event Plane (`control/security/permission_tree/event_handler.rs`).
//! That asynchrony is why this test polls: the grant/revoke DML returns as
//! soon as it is WAL/Raft-durable, before CDC has necessarily applied it to
//! the cache.
//!
//! The poll uses a FRESH, differently-worded `SELECT` every iteration (a
//! trivially-true extra predicate makes the SQL text unique) so it always
//! misses the session plan cache and reads the live `PermissionCache` on
//! every attempt — this establishes ground truth for "has CDC applied the
//! write yet" without touching the cache path under test. The actual
//! assertion then reuses one FIXED statement text, issued repeatedly on one
//! connection, so a cache hit is the only way it can be served: that is what
//! exercises `DescriptorVersionSet::permission_tree_version` re-validation
//! in `PlanCache::get`.
//!
//! The probing identity is a non-superuser: a superuser produces no
//! `PermCtx` at all (`inject_permission_tree` returns early for one), so a
//! superuser-issued read cannot exercise this path no matter what the cache
//! does (`control/planner/rls_injection/permission_tree/plan.rs`).

use std::time::Duration;

use crate::harness::TestServer;

const PASSWORD: &str = "perm-tree-cache-probe-19";

/// How long to wait for asynchronous CDC application to the permission
/// cache, and separately for the session-cache eviction it drives. Generous
/// — every poll returns as soon as its condition holds, so a high ceiling
/// costs nothing on the passing path.
const CDC_TIMEOUT: Duration = Duration::from_secs(30);

/// Run `sql` on `client` and return the first column of each row.
async fn select_ids(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = Vec::new();
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            rows.push(row.get(0).unwrap_or("").to_string());
        }
    }
    rows
}

/// Poll with SQL text unique to each attempt (an always-true extra predicate)
/// until the live permission-tree state yields exactly `expected`, or panic
/// once `timeout` elapses. Never reuses one statement text across attempts,
/// so this always replans from the current `PermissionCache` and never
/// depends on — or pollutes — the session plan cache the real assertion
/// below exercises.
async fn wait_for_live_visibility(
    probe: &tokio_postgres::Client,
    tag: &str,
    expected: &[&str],
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempt: u64 = 0;
    loop {
        attempt += 1;
        let sql = format!(
            "SELECT id FROM perm_tree_docs WHERE '{tag}-{attempt}' = '{tag}-{attempt}' \
             ORDER BY id"
        );
        let got = select_ids(probe, &sql).await;
        if got == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for live permission-tree state to reach {expected:?}, \
             last observed {got:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_permission_tree_grant_is_not_served_from_stale_session_plan_cache() {
    let server = TestServer::start().await;

    // Governed collection: two resources, no hierarchy needed for a direct
    // grant on the resource's own id.
    server
        .exec(
            "CREATE COLLECTION perm_tree_docs (id TEXT PRIMARY KEY, title TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO perm_tree_docs (id, title) VALUES ('d1', 'Doc One')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO perm_tree_docs (id, title) VALUES ('d2', 'Doc Two')")
        .await
        .unwrap();

    // The grant table CDC feeds into the permission cache. Untyped/schemaless
    // is fine — the CDC extractor reads fields off the decoded row body.
    server
        .exec("CREATE COLLECTION perm_tree_grants")
        .await
        .unwrap();

    // Binds `perm_tree_docs` to the tree: default levels
    // (none/viewer/commenter/editor/owner), default read level "viewer".
    server
        .exec(
            "ALTER COLLECTION perm_tree_docs SET PERMISSION_TREE = '{\
                \"resource_column\":\"id\",\
                \"graph_index\":\"perm_tree_docs_tree\",\
                \"permission_table\":\"perm_tree_grants\"\
             }'",
        )
        .await
        .unwrap();

    // A custom role is the grantee: `PermissionGrant.grantee` is matched
    // against the caller's user id OR any of their role names
    // (`accessible_resources` in `resolver.rs`), and a role name is a fixed
    // string chosen by this test — unlike the numeric user id, which the
    // catalog assigns and this test has no way to predict.
    server
        .exec("CREATE ROLE perm_tree_grant_role")
        .await
        .unwrap();
    server
        .exec("CREATE USER perm_tree_probe PASSWORD 'perm-tree-cache-probe-19'")
        .await
        .unwrap();
    // General collection access (RBAC), independent of the permission tree.
    server
        .exec("GRANT ROLE readwrite TO perm_tree_probe")
        .await
        .unwrap();
    server
        .exec("GRANT ROLE perm_tree_grant_role TO perm_tree_probe")
        .await
        .unwrap();

    // Grant: the role can view d1. d2 stays outside the subtree for everyone.
    server
        .exec(
            "INSERT INTO perm_tree_grants (resource_id, grantee, level, inherited) \
             VALUES ('d1', 'perm_tree_grant_role', 'viewer', false)",
        )
        .await
        .unwrap();

    // One dedicated connection for the probing identity, reused for every
    // read below — the session plan cache lives on this connection, and
    // reusing it is the entire point: a fresh connection per query would
    // carry no cache to go stale in the first place.
    let (probe, probe_handle) = server
        .connect_as("perm_tree_probe", PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as perm_tree_probe: {e}"));

    // Ground truth: wait for CDC to apply the grant, via SQL text that can
    // never hit the session plan cache.
    wait_for_live_visibility(&probe, "grant-landed", &["d1"], CDC_TIMEOUT).await;

    let select = "SELECT id FROM perm_tree_docs ORDER BY id";

    // This plans and caches the statement on the probing identity's session
    // plan cache, stamped with the permission-tree version observed here.
    let before = select_ids(&probe, select).await;
    assert_eq!(before, vec!["d1"], "only the granted resource is visible");

    // Repeat on the SAME connection to confirm a cache entry actually exists
    // for this statement text before the grant is revoked.
    let before_again = select_ids(&probe, select).await;
    assert_eq!(before_again, before);

    // Revoke, from the superuser connection (the probing identity holds no
    // write access to the grant table).
    server
        .exec(
            "DELETE FROM perm_tree_grants \
             WHERE resource_id = 'd1' AND grantee = 'perm_tree_grant_role'",
        )
        .await
        .unwrap();

    // Ground truth again: wait for CDC to apply the revoke, independent of
    // the cached statement below.
    wait_for_live_visibility(&probe, "revoke-landed", &[], CDC_TIMEOUT).await;

    // SAME probe connection, SAME statement text, issued exactly once: must
    // reflect the revoke rather than replaying the plan cached before it.
    // This is the discriminator — a `DescriptorVersionSet` that never
    // recorded (or never re-validated) the permission-tree version would
    // still hold the stale `IN ('d1')` filter here and return `["d1"]`.
    let after = select_ids(&probe, select).await;
    assert_eq!(
        after,
        Vec::<String>::new(),
        "the revoked resource must not be served from a stale cached plan"
    );

    // And once more, on the same connection, to confirm the revoke keeps
    // applying rather than only taking effect for the one query issued
    // immediately after CDC caught up.
    let after_again = select_ids(&probe, select).await;
    assert_eq!(after_again, after);

    drop(probe);
    probe_handle.abort();
}
