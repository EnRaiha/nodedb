// SPDX-License-Identifier: BUSL-1.1

//! pgwire wire-level tests for per-connection tenant scoping and Trust-mode
//! identity resolution.
//!
//! Covers the class of bug where the pgwire handler's planning context is
//! built once with a fixed tenant id, so queries from tenant-scoped users
//! plan against the wrong tenant's catalog. Also covers Trust-mode accepting
//! usernames that were never created as if they were superusers.

mod common;

use common::pgwire_harness::TestServer;

/// Helper: superuser-side bootstrap — create a tenant and a tenant-scoped
/// user, plus a collection owned by that tenant. The harness's default
/// connection is Trust/superuser (tenant 1) and is used for setup.
async fn bootstrap_tenant_user(server: &TestServer, user: &str, collection: &str) {
    server
        .exec("CREATE TENANT acme ID 2")
        .await
        .expect("CREATE TENANT");
    server
        .exec(&format!(
            "CREATE USER {user} WITH PASSWORD 'x' ROLE readwrite TENANT 2"
        ))
        .await
        .expect("CREATE USER");
    // Create the collection as the tenant-scoped user so ownership lands on
    // tenant 2. This itself exercises the DDL path, which already reads
    // identity.tenant_id correctly.
    let (svc, _h) = server
        .connect_as(user, "x")
        .await
        .expect("tenant user connect");
    svc.simple_query(&format!(
        "CREATE COLLECTION {collection}  \
         (id TEXT PRIMARY KEY, content TEXT NOT NULL) WITH (engine='document_strict')"
    ))
    .await
    .expect("tenant user CREATE COLLECTION");
    drop(svc);
}

/// Run a simple query under a freshly opened tenant-user connection and
/// return either the rows (first column) or the server error message.
async fn query_as(server: &TestServer, user: &str, sql: &str) -> Result<Vec<String>, String> {
    let (client, _h) = server.connect_as(user, "x").await?;
    match client.simple_query(sql).await {
        Ok(msgs) => {
            let mut rows = Vec::new();
            for msg in msgs {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    rows.push(row.get(0).unwrap_or("").to_string());
                }
            }
            Ok(rows)
        }
        Err(e) => Err(pg_err(&e)),
    }
}

fn pg_err(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e:?}")
    }
}

// ── tenant-scoped planning via shared query_ctx ────────────────────────

#[tokio::test]
async fn tenant_user_can_select_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_sel", "t2_sel").await;

    let (svc, _h) = server.connect_as("svc_sel", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_sel (id, content) VALUES ('a', 'alpha')")
        .await
        .expect("INSERT under tenant user");

    let rows = query_as(&server, "svc_sel", "SELECT id FROM t2_sel")
        .await
        .expect("SELECT under tenant user must not fail with 'unknown table'");
    assert_eq!(rows.len(), 1, "expected 1 row, got {rows:?}");
    assert_eq!(rows[0], "a", "row should contain id 'a': {rows:?}");
}

#[tokio::test]
async fn tenant_user_can_insert_into_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_ins", "t2_ins").await;

    let (svc, _h) = server.connect_as("svc_ins", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_ins (id, content) VALUES ('k', 'v')")
        .await
        .expect("INSERT must succeed for tenant-owned collection");
}

#[tokio::test]
async fn tenant_user_can_update_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_upd", "t2_upd").await;

    let (svc, _h) = server.connect_as("svc_upd", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_upd (id, content) VALUES ('a', 'old')")
        .await
        .unwrap();
    svc.simple_query("UPDATE t2_upd SET content = 'new' WHERE id = 'a'")
        .await
        .expect("UPDATE must not fail with 'unknown table'");

    let rows = query_as(
        &server,
        "svc_upd",
        "SELECT content FROM t2_upd WHERE id = 'a'",
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0], "new",
        "row should reflect updated content: {rows:?}"
    );
}

#[tokio::test]
async fn tenant_user_can_delete_from_own_collection() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_del", "t2_del").await;

    let (svc, _h) = server.connect_as("svc_del", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_del (id, content) VALUES ('a', 'x')")
        .await
        .unwrap();
    svc.simple_query("DELETE FROM t2_del WHERE id = 'a'")
        .await
        .expect("DELETE must not fail with 'unknown table'");

    let rows = query_as(&server, "svc_del", "SELECT id FROM t2_del")
        .await
        .unwrap();
    assert!(rows.is_empty(), "row should be deleted, got {rows:?}");
}

#[tokio::test]
async fn tenant_user_prepared_select_resolves_own_collection() {
    // Extended protocol goes through NodeDbQueryParser::parse_sql, which
    // builds its own OriginCatalog with a hardcoded tenant id. A tenant-2
    // user must still be able to prepare and execute a statement against
    // a tenant-2 collection.
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_prep", "t2_prep").await;

    let (svc, _h) = server.connect_as("svc_prep", "x").await.unwrap();
    svc.simple_query("INSERT INTO t2_prep (id, content) VALUES ('a', 'alpha')")
        .await
        .unwrap();

    // `prepare` drives Parse + Describe through NodeDbQueryParser::parse_sql,
    // which constructs an OriginCatalog. Before the fix this catalog was
    // hardcoded to tenant 1 and could not see tenant-2 collections —
    // `prepare` would surface the server's "unknown table" error.
    svc.prepare("SELECT content FROM t2_prep WHERE id = 'a'")
        .await
        .expect("prepare must resolve tenant-owned collection via parser.rs");
}

#[tokio::test]
async fn tenant_user_cannot_see_other_tenants_collection_as_empty() {
    // Asymmetric-isolation guard: a tenant-2 user issuing SELECT against a
    // tenant-1 collection must NOT silently return an empty result set.
    // The correct behavior is the same "unknown table" a cross-tenant
    // planner produces the other direction. Silent empty is a data-shape
    // leak vector even though no rows cross.
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION t1_only  \
             (id TEXT PRIMARY KEY, secret TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO t1_only (id, secret) VALUES ('a', 'classified')")
        .await
        .unwrap();

    server.exec("CREATE TENANT acme ID 2").await.unwrap();
    server
        .exec("CREATE USER svc_xtn WITH PASSWORD 'x' ROLE readwrite TENANT 2")
        .await
        .unwrap();

    let result = query_as(&server, "svc_xtn", "SELECT secret FROM t1_only").await;
    match result {
        Err(msg) => {
            // Cross-tenant isolation is enforced via lookup failure: the
            // collection must appear absent. Accept the legacy "unknown table"
            // / "table not found" wordings and the now-canonical
            // "does not exist" (SQLSTATE 42P01) — semantics are identical.
            let lower = msg.to_lowercase();
            assert!(
                lower.contains("unknown table")
                    || lower.contains("table not found")
                    || lower.contains("does not exist"),
                "expected isolation error, got: {msg}"
            );
        }
        Ok(rows) => panic!(
            "tenant-2 user must not see tenant-1 collection via silent empty result; got rows={rows:?}"
        ),
    }
}

// ── Trust-mode identity resolution ─────────────────────────────────────

#[tokio::test]
async fn trust_mode_rejects_unknown_username() {
    // Under Trust mode NodeDB skips password verification, but it must
    // still resolve the connecting username against the credential store.
    // Accepting a fabricated username silently promotes arbitrary clients
    // to tenant-1 superuser (see core.rs resolve_identity fallback).
    let server = TestServer::start().await;

    let result = server
        .connect_as("nosuchuser_ever_created", "anything")
        .await;
    assert!(
        result.is_err(),
        "Trust mode must reject a username that was never CREATE USER'd; got an accepted connection"
    );
}

#[tokio::test]
async fn trust_mode_unknown_user_cannot_run_superuser_ddl() {
    // Defense-in-depth regression guard for the same root cause: even if
    // a connection is somehow permitted, an unknown username MUST NOT be
    // silently fabricated as a superuser identity capable of tenant/user
    // management DDL. Today core.rs sets `is_superuser: true` in the
    // fallback branch, so `CREATE USER` from a fabricated name succeeds.
    let server = TestServer::start().await;

    let Ok((client, _h)) = server.connect_as("ghost_admin", "anything").await else {
        // If connect_as is already rejected by the prior test's fix, that
        // is a strictly stronger guarantee — the bug is still captured.
        return;
    };

    let result = client
        .simple_query("CREATE USER mallory WITH PASSWORD 'y' ROLE readwrite TENANT 1")
        .await;
    assert!(
        result.is_err(),
        "unknown Trust-mode user must not be granted superuser privileges; CREATE USER succeeded"
    );
    if let Err(e) = result {
        let msg = pg_err(&e);
        assert!(
            msg.contains("42501") || msg.to_lowercase().contains("permission"),
            "expected a permission-denied error, got: {msg}"
        );
    }
}

// ── Runtime SET keys that touch identity / security ────────────────────────
//
// Superusers may switch session tenant context at runtime via
// `SET TENANT = '<name>' | <id> | DEFAULT` and `SET nodedb.tenant_id = <id>`.
// The override is applied at every `resolve_identity` call so every planning
// / routing path naturally honors it without per-callsite changes. The
// switch is rejected for non-superusers (42501) and inside active
// transactions (25001), and invalidates the session's plan and prepared-
// statement caches so plans against the prior tenant's catalog cannot be
// reused. `RESET TENANT` (and `SET TENANT = DEFAULT`) restores the
// identity-bound tenant.
//
// `SET ROLE` / `SET SESSION AUTHORIZATION` are not implemented (identity is
// otherwise bound at connection time) and reject with 0A000 instead of
// silently storing the value. Unknown runtime parameters reject with 42704,
// mirroring the existing SHOW asymmetry.

/// Run a single statement under a freshly opened connection and return either
/// `Ok(())` or the server's error message.
async fn exec_as(server: &TestServer, user: &str, sql: &str) -> Result<(), String> {
    let (client, _h) = server.connect_as(user, "x").await?;
    client
        .simple_query(sql)
        .await
        .map(|_| ())
        .map_err(|e| pg_err(&e))
}

fn assert_rejected_with_any(result: Result<(), String>, codes: &[&str], context: &str) {
    match result {
        Ok(()) => panic!(
            "{context}: spec requires rejection so silent identity misrouting is impossible, \
             but the server returned success"
        ),
        Err(msg) => {
            let lower = msg.to_lowercase();
            let code_hit = codes.iter().any(|c| msg.contains(c));
            let wording_hit = lower.contains("not supported")
                || lower.contains("unrecognized")
                || lower.contains("unknown")
                || lower.contains("permission")
                || lower.contains("insufficient");
            assert!(
                code_hit || wording_hit,
                "{context}: expected one of {codes:?} or an explanatory wording, got: {msg}"
            );
        }
    }
}

#[tokio::test]
async fn set_tenant_as_superuser_switches_effective_tenant() {
    // The end-to-end spec: a superuser issues SET TENANT and subsequent
    // writes land in the target tenant's catalog, not tenant 0. This is
    // the behavior the silent-no-op bug obscured.
    let server = TestServer::start().await;
    server.exec("CREATE TENANT acme ID 2").await.unwrap();

    let (svc, _h) = server.connect_as("nodedb", "nodedb").await.unwrap();
    svc.simple_query("SET TENANT = 'acme'")
        .await
        .expect("superuser SET TENANT must succeed");

    // Collection created under the switched tenant must belong to tenant 2.
    svc.simple_query(
        "CREATE COLLECTION switched_under_acme  \
         (id TEXT PRIMARY KEY, content TEXT NOT NULL) WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION under switched tenant must succeed");
    svc.simple_query("INSERT INTO switched_under_acme (id, content) VALUES ('a', 'x')")
        .await
        .expect("INSERT under switched tenant must succeed");

    // SHOW TENANT (singular) reports the effective tenant on this connection.
    let msgs = svc.simple_query("SHOW TENANT").await.expect("SHOW TENANT");
    let row = msgs
        .iter()
        .find_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .expect("SHOW TENANT must return a row");
    assert_eq!(
        row.get("tenant_id"),
        Some("2"),
        "SHOW TENANT must report the switched tenant id"
    );
    assert_eq!(
        row.get("tenant_name"),
        Some("acme"),
        "SHOW TENANT must report the tenant name"
    );

    // The same collection name created under tenant 0 must NOT collide,
    // because the switched session writes to tenant 2's catalog.
    server
        .exec(
            "CREATE COLLECTION switched_under_acme  \
             (id TEXT PRIMARY KEY, content TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("same name in tenant 0 must succeed — proof switched session was tenant 2");
}

#[tokio::test]
async fn set_tenant_as_tenant_user_must_not_silently_noop() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "svc_settnt", "t2_settnt").await;
    server.exec("CREATE TENANT other ID 3").await.unwrap();

    let res = exec_as(&server, "svc_settnt", "SET TENANT = 'other'").await;
    assert_rejected_with_any(
        res,
        &["0A000", "42501"],
        "SET TENANT as tenant-scoped user crossing tenants",
    );
}

#[tokio::test]
async fn set_nodedb_tenant_id_switches_effective_tenant() {
    // The integer alias resolves directly without a name lookup. End-to-end:
    // SET nodedb.tenant_id = 2 → writes route to tenant 2.
    let server = TestServer::start().await;
    server.exec("CREATE TENANT acme ID 2").await.unwrap();

    let (svc, _h) = server.connect_as("nodedb", "nodedb").await.unwrap();
    svc.simple_query("SET nodedb.tenant_id = 2")
        .await
        .expect("SET nodedb.tenant_id must succeed for superuser");
    svc.simple_query(
        "CREATE COLLECTION numeric_alias_check  \
         (id TEXT PRIMARY KEY, content TEXT NOT NULL) WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE under integer-alias switch must succeed");
    server
        .exec(
            "CREATE COLLECTION numeric_alias_check  \
             (id TEXT PRIMARY KEY, content TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .expect("same name under tenant 0 must succeed — proof switched session was tenant 2");
}

#[tokio::test]
async fn reset_tenant_restores_identity_bound_tenant() {
    let server = TestServer::start().await;
    server.exec("CREATE TENANT acme ID 2").await.unwrap();

    let (svc, _h) = server.connect_as("nodedb", "nodedb").await.unwrap();
    svc.simple_query("SET TENANT = 'acme'").await.unwrap();
    svc.simple_query("RESET TENANT")
        .await
        .expect("RESET TENANT must succeed");

    // After RESET, a collection created here lands back in tenant 0; a
    // duplicate name in tenant 0 must conflict.
    svc.simple_query(
        "CREATE COLLECTION post_reset_check  \
         (id TEXT PRIMARY KEY, content TEXT NOT NULL) WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE after RESET must land in tenant 0");
    let res = server
        .exec(
            "CREATE COLLECTION post_reset_check  \
             (id TEXT PRIMARY KEY, content TEXT NOT NULL) WITH (engine='document_strict')",
        )
        .await;
    assert!(
        res.is_err(),
        "duplicate name in tenant 0 must conflict — proves RESET TENANT actually restored"
    );
}

#[tokio::test]
async fn set_tenant_default_clears_override() {
    let server = TestServer::start().await;
    server.exec("CREATE TENANT acme ID 2").await.unwrap();

    let (svc, _h) = server.connect_as("nodedb", "nodedb").await.unwrap();
    svc.simple_query("SET TENANT = 'acme'").await.unwrap();
    svc.simple_query("SET TENANT = DEFAULT")
        .await
        .expect("SET TENANT = DEFAULT must succeed");

    // SHOW TENANT should now report identity-bound tenant (0 for superuser).
    let msgs = svc.simple_query("SHOW TENANT").await.unwrap();
    let row = msgs
        .iter()
        .find_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .expect("SHOW TENANT row");
    assert_ne!(
        row.get("tenant_id"),
        Some("2"),
        "after DEFAULT, SHOW TENANT must not still report tenant 2"
    );
}

#[tokio::test]
async fn set_tenant_inside_transaction_is_rejected() {
    let server = TestServer::start().await;
    server.exec("CREATE TENANT acme ID 2").await.unwrap();

    let (svc, _h) = server.connect_as("nodedb", "nodedb").await.unwrap();
    svc.simple_query("BEGIN").await.unwrap();
    let res = svc.simple_query("SET TENANT = 'acme'").await;
    match res {
        Err(e) => {
            let msg = pg_err(&e);
            assert!(
                msg.contains("25001") || msg.to_lowercase().contains("transaction"),
                "expected 25001 active_sql_transaction, got: {msg}"
            );
        }
        Ok(_) => panic!(
            "SET TENANT inside an active transaction must reject — \
             tenant context cannot change while snapshot / locks are held"
        ),
    }
    let _ = svc.simple_query("ROLLBACK").await;
}

#[tokio::test]
async fn set_nodedb_tenant_id_non_integer_value_is_rejected() {
    // Regression guard for the only currently-correct branch in handle_set:
    // a non-integer value already returns 22023. Keep it locked.
    let server = TestServer::start().await;

    let res = exec_as(&server, "nodedb", "SET nodedb.tenant_id = 'not-an-int'").await;
    match res {
        Err(msg) => assert!(
            msg.contains("22023"),
            "expected 22023 invalid_parameter_value, got: {msg}"
        ),
        Ok(()) => panic!("non-integer nodedb.tenant_id must be rejected with 22023"),
    }
}

#[tokio::test]
async fn set_role_must_not_silently_noop() {
    let server = TestServer::start().await;

    let res = exec_as(&server, "nodedb", "SET ROLE readonly").await;
    assert_rejected_with_any(
        res,
        &["0A000"],
        "SET ROLE (no runtime role-switch path is wired)",
    );
}

#[tokio::test]
async fn set_session_authorization_must_not_silently_noop() {
    let server = TestServer::start().await;
    server
        .exec("CREATE USER svc_other WITH PASSWORD 'x' ROLE readwrite TENANT 1")
        .await
        .unwrap();

    let res = exec_as(&server, "nodedb", "SET SESSION AUTHORIZATION 'svc_other'").await;
    assert_rejected_with_any(
        res,
        &["0A000"],
        "SET SESSION AUTHORIZATION (no runtime identity-switch path is wired)",
    );
}

#[tokio::test]
async fn set_unknown_runtime_parameter_is_rejected_like_show() {
    // SHOW <unknown> returns 42704 (params.rs::is_known_pg_runtime_parameter).
    // SET <unknown> must match — silently storing an arbitrary key in the
    // session bag is what allowed SET TENANT to look successful in the first
    // place. The asymmetry between SET and SHOW is the structural flaw.
    let server = TestServer::start().await;

    let res = exec_as(&server, "nodedb", "SET nodedb.no_such_knob = 'x'").await;
    assert_rejected_with_any(res, &["42704"], "SET on an unknown runtime parameter");
}

/// `SHOW TENANTS` reports each tenant's name alongside its id, so a tenant
/// can be identified by name without grepping the audit log.
#[tokio::test]
async fn show_tenants_includes_tenant_name() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TENANT acme ID 7")
        .await
        .expect("CREATE TENANT");

    let rows = server
        .query_named_rows("SHOW TENANTS")
        .await
        .expect("SHOW TENANTS");
    let acme = rows
        .iter()
        .find(|r| r.get("tenant_id").map(String::as_str) == Some("7"))
        .expect("tenant 7 present in SHOW TENANTS");
    assert_eq!(
        acme.get("name").map(String::as_str),
        Some("acme"),
        "SHOW TENANTS must report the tenant name; row was {acme:?}"
    );
}

/// Tenant names are unique: a second `CREATE TENANT` with the same name is
/// rejected rather than minting a second tenant id sharing that name. A
/// duplicate name makes every by-name lookup ambiguous (ownership fallback,
/// admin provisioning, `DROP TENANT`) and strands the older tenant's objects.
#[tokio::test]
async fn duplicate_tenant_name_is_rejected() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TENANT probe")
        .await
        .expect("first CREATE TENANT must succeed");

    let err = server
        .exec("CREATE TENANT probe")
        .await
        .expect_err("duplicate tenant name must be rejected");
    assert!(
        err.contains("already exists"),
        "duplicate CREATE TENANT must report the name conflict, got: {err}"
    );

    // IF NOT EXISTS remains a no-op success on the same name.
    server
        .exec("CREATE TENANT IF NOT EXISTS probe")
        .await
        .expect("CREATE TENANT IF NOT EXISTS on an existing name must succeed");

    let rows = server
        .query_named_rows("SHOW TENANTS")
        .await
        .expect("SHOW TENANTS");
    let named: Vec<_> = rows
        .iter()
        .filter(|r| r.get("name").map(String::as_str) == Some("probe"))
        .collect();
    assert_eq!(
        named.len(),
        1,
        "exactly one tenant may carry the name 'probe'; rows were {named:?}"
    );
}

/// A superuser can administer a pre-existing tenant-homed collection it does
/// not own by switching the session's effective tenant. Without `SET TENANT`
/// the superuser is scoped to its own tenant and the collection is correctly
/// invisible; with it, ownership is satisfied by superuser status.
#[tokio::test]
async fn superuser_can_drop_tenant_homed_collection_after_set_tenant() {
    let server = TestServer::start().await;
    bootstrap_tenant_user(&server, "acme_user", "acme_notes").await;

    // Unqualified, the superuser session does not see the tenant's collection.
    let unscoped = server.exec("DROP COLLECTION acme_notes").await;
    assert!(
        unscoped.is_err(),
        "superuser must not resolve a tenant-homed collection without SET TENANT"
    );

    let (svc, _h) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("superuser connect");
    svc.simple_query("SET TENANT = 'acme'")
        .await
        .expect("superuser SET TENANT");
    svc.simple_query("DROP COLLECTION acme_notes")
        .await
        .expect("superuser must drop a tenant-homed collection it does not own");

    let remaining = svc
        .simple_query("SELECT id FROM acme_notes")
        .await
        .expect_err("dropped collection must no longer resolve");
    let _ = remaining;
}
