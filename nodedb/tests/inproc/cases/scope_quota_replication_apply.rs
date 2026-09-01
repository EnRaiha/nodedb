// SPDX-License-Identifier: BUSL-1.1

//! `DEFINE QUOTA` is replicated catalog state, not node-local config.
//!
//! A node that did not run the statement receives the mutation as a
//! `CatalogEntry` and must end up enforcing the same cap. These tests drive
//! that follower path directly — apply, then the synchronous post-apply —
//! and assert on `QuotaManager`, the component admission checks read.

use nodedb::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync;
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb::control::security::catalog::auth_types::StoredScopeQuota;
use nodedb::control::security::metering::quota::QuotaEnforcement;
use nodedb_test_support::pgwire_harness::TestServer;

const SCOPE: &str = "ops:replicated";

fn definition() -> StoredScopeQuota {
    StoredScopeQuota {
        scope_name: SCOPE.to_string(),
        max_tokens: 5_000,
        period_secs: 60,
        enforcement: "hard".to_string(),
        warning_threshold: 0.5,
    }
}

/// The scope names of every durable scope-quota row.
fn stored_scopes(server: &TestServer) -> Vec<String> {
    server
        .shared
        .credentials
        .catalog()
        .load_all_scope_quotas()
        .expect("load scope quotas")
        .into_iter()
        .map(|q| q.scope_name)
        .collect()
}

/// Apply an entry the way the metadata applier does: durable write first,
/// then the synchronous in-memory install.
fn apply_entry(server: &TestServer, entry: &CatalogEntry) {
    apply::apply_to(entry, server.shared.credentials.catalog()).expect("apply catalog entry");
    apply_post_apply_side_effects_sync(entry, &server.shared);
}

/// A replicated `PutScopeQuota` makes the cap durable and live on a node that
/// never parsed the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_installs_the_scope_quota() {
    let server = TestServer::start().await;
    assert!(
        !server.shared.quota_manager.has_quota(SCOPE),
        "no quota is defined before the entry applies"
    );

    apply_entry(
        &server,
        &CatalogEntry::PutScopeQuota(Box::new(definition())),
    );

    let live = server
        .shared
        .quota_manager
        .list_quotas()
        .into_iter()
        .find(|q| q.scope_name == SCOPE)
        .expect("apply must install the definition in live enforcement");
    assert_eq!(live.max_tokens, 5_000);
    assert_eq!(live.period_secs, 60);
    assert_eq!(live.enforcement, QuotaEnforcement::Hard);

    let stored = server
        .shared
        .credentials
        .catalog()
        .load_all_scope_quotas()
        .expect("load scope quotas");
    assert!(
        stored.contains(&definition()),
        "apply must write the durable row too: {stored:?}"
    );
}

/// A replicated `DeleteScopeQuota` drops both the row and the live cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_delete_removes_the_scope_quota() {
    let server = TestServer::start().await;
    apply_entry(
        &server,
        &CatalogEntry::PutScopeQuota(Box::new(definition())),
    );
    assert!(server.shared.quota_manager.has_quota(SCOPE));

    apply_entry(
        &server,
        &CatalogEntry::DeleteScopeQuota {
            scope_name: SCOPE.to_string(),
        },
    );

    assert!(
        !server.shared.quota_manager.has_quota(SCOPE),
        "the live cap must be gone"
    );
    assert!(
        !stored_scopes(&server).contains(&SCOPE.to_string()),
        "the durable row must be gone"
    );
}

/// `DEFINE QUOTA` routes through the replicated entry, so the statement path
/// leaves the same live and durable state the follower path produces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn define_quota_statement_leaves_the_replicated_state() {
    let server = TestServer::start().await;
    server
        .exec(&format!(
            "DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 5000 TOKENS PER 60 SECONDS \
             ENFORCEMENT HARD WARN AT 0.5"
        ))
        .await
        .expect("DEFINE QUOTA");

    assert!(server.shared.quota_manager.has_quota(SCOPE));
    let stored = server
        .shared
        .credentials
        .catalog()
        .load_all_scope_quotas()
        .expect("load scope quotas");
    assert!(stored.contains(&definition()), "row written: {stored:?}");

    server
        .exec(&format!("DROP QUOTA ON SCOPE '{SCOPE}'"))
        .await
        .expect("DROP QUOTA");

    assert!(!server.shared.quota_manager.has_quota(SCOPE));
    assert!(!stored_scopes(&server).contains(&SCOPE.to_string()));
}

/// Dropping an undefined scope is refused before any propose.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_quota_on_an_undefined_scope_is_refused() {
    let server = TestServer::start().await;
    server
        .exec("DROP QUOTA ON SCOPE 'never:defined'")
        .await
        .expect_err("an undefined scope must be refused");
}
