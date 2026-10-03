// SPDX-License-Identifier: BUSL-1.1

//! Quota admission and metering of `RESTORE DATABASE`.
//!
//! A restore writes rows, so it counts against the write quota of every
//! collection it restores, under the same name DML charges. A hard write cap
//! on one collection refuses the restore before any tenant writes. A restore
//! under the caps charges its backup scope with the tenant COPY restore's row
//! formula, and each write scope with the rows it verified. A DRY RUN writes
//! nothing and charges no write quota.

use nodedb::control::backup::database::{check_database, database_tenants, open_database_backup};
use nodedb::control::security::metering::config::MeteringConfig;
use nodedb::control::security::time::now_secs;
use nodedb::types::DatabaseId;
use nodedb_test_support::pgwire_harness::TestServer;

const ORDERS: &str = "rq_orders";
const OTHER: &str = "rq_other";
const OBJECT: &str = "quota/default.ndbb";

fn metering_on() -> MeteringConfig {
    MeteringConfig {
        enabled: true,
        ..Default::default()
    }
}

/// The token cost of one restored row: the restore meters under the `sql`
/// operation.
fn row_cost() -> u64 {
    metering_on()
        .operation_costs
        .get("sql")
        .copied()
        .unwrap_or(1)
}

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The numeric user id the harness client authenticates as: scope grants and
/// quota usage are keyed by it.
async fn harness_user_id(server: &TestServer) -> String {
    let rows = server.query_rows("SHOW USERS").await.expect("SHOW USERS");
    let row = rows
        .iter()
        .find(|row| row.iter().any(|field| field == "nodedb"))
        .unwrap_or_else(|| panic!("the harness user must be listed: {rows:?}"));
    row.iter()
        .find(|field| field.parse::<u64>().is_ok())
        .unwrap_or_else(|| panic!("SHOW USERS row must carry a numeric id: {row:?}"))
        .clone()
}

/// The one tenant holding the default database's collections.
fn only_tenant(server: &TestServer) -> u64 {
    let tenants = database_tenants(&server.shared, DatabaseId::DEFAULT).expect("tenants");
    assert_eq!(
        tenants.len(),
        1,
        "one tenant owns the collections: {tenants:?}"
    );
    *tenants.iter().next().expect("one tenant")
}

/// Define `scope` as `grants`, grant it to the harness user, and cap it at
/// `max` tokens with hard enforcement.
async fn capped_scope(server: &TestServer, user: &str, scope: &str, grants: &str, max: u64) {
    exec(server, &format!("DEFINE SCOPE '{scope}' AS {grants}")).await;
    exec(server, &format!("GRANT SCOPE '{scope}' TO USER '{user}'")).await;
    exec(
        server,
        &format!(
            "DEFINE QUOTA ON SCOPE '{scope}' MAX {max} TOKENS PER 3600 SECONDS ENFORCEMENT HARD"
        ),
    )
    .await;
}

fn used(server: &TestServer, scope: &str, user: &str) -> u64 {
    server
        .shared
        .quota_manager
        .get_status(scope, user, now_secs())
        .unwrap_or_else(|| panic!("quota on '{scope}' is defined"))
        .used_tokens
}

/// A metered server whose default database holds three rows in [`ORDERS`]
/// and two in [`OTHER`], backed up to [`OBJECT`].
async fn backed_up_server() -> TestServer {
    let server = TestServer::start_with_metering_and_backup_root(metering_on()).await;
    for coll in [ORDERS, OTHER] {
        exec(
            &server,
            &format!(
                "CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, name TEXT) \
                 WITH (engine='document_strict')"
            ),
        )
        .await;
    }
    for id in ["a", "b", "c"] {
        exec(
            &server,
            &format!("INSERT INTO {ORDERS} (id, name) VALUES ('{id}', 'order')"),
        )
        .await;
    }
    for id in ["p", "q"] {
        exec(
            &server,
            &format!("INSERT INTO {OTHER} (id, name) VALUES ('{id}', 'other')"),
        )
        .await;
    }
    exec(
        &server,
        &format!("BACKUP DATABASE default TO '{}'", server.backup_uri(OBJECT)),
    )
    .await;
    server
}

async fn ids(server: &TestServer, coll: &str) -> Vec<String> {
    let mut ids = server
        .query_text(&format!("SELECT id FROM {coll}"))
        .await
        .unwrap_or_else(|e| panic!("read {coll}: {e}"));
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spent_collection_write_cap_refuses_the_restore_before_any_write() {
    let server = backed_up_server().await;
    // Rows the restore brings back, in both collections.
    exec(&server, &format!("DELETE FROM {ORDERS} WHERE id = 'a'")).await;
    exec(&server, &format!("DELETE FROM {OTHER} WHERE id = 'p'")).await;

    let user = harness_user_id(&server).await;
    let scope = "rq:orders_write";
    capped_scope(&server, &user, scope, &format!("WRITE ON {ORDERS}"), 1).await;
    // Spend the cap with ordinary writes. Each is admitted while the scope is
    // at or under its cap.
    for id in ["x", "y", "z"] {
        if used(&server, scope, &user) > 1 {
            break;
        }
        exec(
            &server,
            &format!("INSERT INTO {ORDERS} (id, name) VALUES ('{id}', 'spend')"),
        )
        .await;
    }
    assert!(used(&server, scope, &user) > 1, "the writes spent the cap");

    let refused = server
        .exec(&format!(
            "RESTORE DATABASE default FROM '{}' FORCE",
            server.backup_uri(OBJECT)
        ))
        .await
        .expect_err("a spent write cap on a restored collection refuses the restore");
    assert!(
        refused.contains("quota exceeded") && refused.contains(scope),
        "the refusal names the spent write scope: {refused}"
    );

    let orders = ids(&server, ORDERS).await;
    assert!(
        !orders.contains(&"a".to_string()),
        "no row of the capped collection was restored: {orders:?}"
    );
    let other = ids(&server, OTHER).await;
    assert!(
        !other.contains(&"p".to_string()),
        "no row of another collection was restored: {other:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_under_the_caps_is_metered_and_a_dry_run_charges_no_write_quota() {
    let server = backed_up_server().await;
    let user = harness_user_id(&server).await;
    let tenant = only_tenant(&server);
    let cap = 1_000_000;
    let backup_scope = "rq:backup";
    let orders_scope = "rq:orders_write";
    let all_scope = "rq:all_write";
    let tenant_scope = "rq:tenant_write";
    capped_scope(
        &server,
        &user,
        backup_scope,
        &format!("BACKUP ON 'tenant:{tenant}'"),
        cap,
    )
    .await;
    capped_scope(
        &server,
        &user,
        orders_scope,
        &format!("WRITE ON {ORDERS}"),
        cap,
    )
    .await;
    capped_scope(&server, &user, all_scope, "WRITE ON *", cap).await;
    capped_scope(
        &server,
        &user,
        tenant_scope,
        &format!("WRITE ON 'tenant:{tenant}'"),
        cap,
    )
    .await;

    // The tenant COPY restore's row formula, from the same checks the restore
    // runs first.
    let bytes = std::fs::read(server.backup_root().join(OBJECT)).expect("read the backup");
    let backup = open_database_backup(&server.shared, "default", &bytes).expect("open backup");
    let checked = check_database(&server.shared, &backup, true)
        .await
        .expect("check the backup");
    let formula: u64 = checked
        .tenants
        .iter()
        .map(|(_, s)| (s.documents + s.kv_tables + s.vectors + s.timeseries + s.edges) as u64)
        .sum();
    assert!(formula >= 5, "the backup holds all five rows: {formula}");
    let per_restore = formula * row_cost();

    exec(
        &server,
        &format!(
            "RESTORE DATABASE default FROM '{}' FORCE DRY RUN",
            server.backup_uri(OBJECT)
        ),
    )
    .await;
    assert_eq!(
        used(&server, backup_scope, &user),
        per_restore,
        "a dry run meters its backup scope"
    );
    for scope in [orders_scope, all_scope, tenant_scope] {
        assert_eq!(
            used(&server, scope, &user),
            0,
            "a DRY RUN charges no write quota to '{scope}'"
        );
    }

    exec(
        &server,
        &format!(
            "RESTORE DATABASE default FROM '{}' FORCE",
            server.backup_uri(OBJECT)
        ),
    )
    .await;
    assert_eq!(
        used(&server, backup_scope, &user),
        2 * per_restore,
        "the restore meters its backup scope with the COPY row formula"
    );
    assert_eq!(
        used(&server, orders_scope, &user),
        3 * row_cost(),
        "a collection write scope is charged the rows restored into it"
    );
    assert_eq!(
        used(&server, all_scope, &user),
        5 * row_cost(),
        "a `*` write scope is charged each restored row once"
    );
    assert_eq!(
        used(&server, tenant_scope, &user),
        5 * row_cost(),
        "the tenant's write marker is charged the tenant's rows once"
    );
    assert_eq!(ids(&server, ORDERS).await, ["a", "b", "c"]);
    assert_eq!(ids(&server, OTHER).await, ["p", "q"]);
}
