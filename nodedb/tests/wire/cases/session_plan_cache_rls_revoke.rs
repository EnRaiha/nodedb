// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: a newly-created read policy must not keep being
//! served from a cached plan on the same session.
//!
//! `plan_statement_to_tasks` stamps `DescriptorVersionSet` with the
//! tenant's RLS/permission-tree versions at build time and re-validates
//! both on every cache hit, so a policy write evicts a cached entry instead
//! of replaying a frozen filter. This test drives that path end-to-end
//! over pgwire, on one connection, as the identity the policy actually
//! governs: the read must be issued by a non-superuser, because RLS
//! evaluation short-circuits to "no filter" for a superuser
//! (`control/security/rls/eval.rs`) before it ever consults the policy
//! store or the cache — a superuser-issued read cannot exercise this path
//! no matter what the cache does.
//!
//! DDL (`CREATE COLLECTION`, `CREATE USER`, `CREATE RLS POLICY`) runs on
//! the bootstrapped superuser connection: `CREATE RLS POLICY` requires
//! superuser or `tenant_admin` (`authorize_rls_scope`), and the probing
//! identity below holds neither. The two `SELECT`s run on one dedicated
//! connection for that identity — reusing the same connection is the
//! entire point, since a fresh connection per query would carry no session
//! plan cache to go stale in the first place.

use crate::harness::TestServer;

const PASSWORD: &str = "plan-cache-probe-77";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_rls_grant_is_not_served_from_stale_session_plan_cache() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION plan_cache_revoke_docs (\
                id TEXT PRIMARY KEY, \
                owner TEXT) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO plan_cache_revoke_docs (id, owner) VALUES ('d1', 'alice')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO plan_cache_revoke_docs (id, owner) VALUES ('d2', 'bob')")
        .await
        .unwrap();
    server
        .exec("CREATE USER plan_cache_probe PASSWORD 'plan-cache-probe-77'")
        .await
        .unwrap();
    server
        .exec("GRANT ROLE readwrite TO plan_cache_probe")
        .await
        .unwrap();

    // One dedicated connection for the probing identity, reused for every
    // read below — the session plan cache lives on this connection.
    let (probe, probe_handle) = server
        .connect_as("plan_cache_probe", PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as plan_cache_probe: {e}"));

    let select = "SELECT id FROM plan_cache_revoke_docs ORDER BY id";

    // No RLS policy yet: both rows are visible — the implicit "grant" before
    // any restriction exists. This plans and caches the statement on the
    // probing identity's session plan cache.
    let before = select_ids(&probe, select).await;
    assert_eq!(
        before,
        vec!["d1", "d2"],
        "both rows visible before any policy"
    );

    // Repeat on the SAME connection to confirm a cache entry actually
    // exists for this statement text before the policy is created.
    let before_again = select_ids(&probe, select).await;
    assert_eq!(before_again, before);

    // Restrict reads to alice's row. Issued by the superuser connection:
    // the probing identity holds neither superuser nor tenant_admin, so it
    // cannot run CREATE RLS POLICY itself (authorize_rls_scope).
    server
        .exec(
            "CREATE RLS POLICY plan_cache_revoke_alice_only ON plan_cache_revoke_docs \
                FOR READ USING (owner = 'alice')",
        )
        .await
        .unwrap();

    // SAME probe connection, SAME statement text: must reflect the new
    // policy immediately rather than replaying the plan cached before the
    // policy existed.
    let after = select_ids(&probe, select).await;
    assert_eq!(
        after,
        vec!["d1"],
        "the excluded row must not be served from a stale cached plan"
    );

    // And once more, on the same connection, to confirm the policy keeps
    // applying rather than only taking effect for the one query issued
    // immediately after the CREATE RLS POLICY round-trip.
    let after_again = select_ids(&probe, select).await;
    assert_eq!(after_again, after);

    drop(probe);
    probe_handle.abort();
}
