// SPDX-License-Identifier: BUSL-1.1

//! Retention policies are replicated catalog state, not node-local config.
//!
//! A node that did not run the statement receives the mutation as a
//! `CatalogEntry` and must end up enforcing the same policy. These tests drive
//! that follower path directly — apply, then the synchronous post-apply — and
//! assert on `RetentionPolicyRegistry`, the component the enforcement loop and
//! the auto-tier planner read.

use nodedb::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync;
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb::engine::timeseries::retention_policy::{RetentionPolicyDef, TierDef};
use nodedb_test_support::pgwire_harness::TestServer;

const DB: u64 = 0;
const TENANT: u64 = 1;
const POLICY: &str = "replicated_policy";
const COLLECTION: &str = "replicated_metrics";

fn definition() -> RetentionPolicyDef {
    RetentionPolicyDef {
        database_id: DB,
        tenant_id: TENANT,
        name: POLICY.to_string(),
        collection: COLLECTION.to_string(),
        tiers: vec![TierDef {
            tier_index: 0,
            resolution_ms: 0,
            aggregates: Vec::new(),
            retain_ms: 604_800_000,
            archive: None,
        }],
        auto_tier: true,
        enabled: true,
        eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
        owner: "admin".to_string(),
        created_at: 1_000,
    }
}

fn delete_entry() -> CatalogEntry {
    CatalogEntry::DeleteRetentionPolicy {
        database_id: DB,
        tenant_id: TENANT,
        name: POLICY.to_string(),
        collection: COLLECTION.to_string(),
    }
}

/// The names of every durable retention policy row.
fn stored_names(server: &TestServer) -> Vec<String> {
    server
        .shared
        .credentials
        .catalog()
        .load_all_retention_policies()
        .expect("load retention policies")
        .into_iter()
        .map(|p| p.name)
        .collect()
}

/// Apply an entry the way the metadata applier does: durable write first,
/// then the synchronous in-memory install.
fn apply_entry(server: &TestServer, entry: &CatalogEntry) {
    apply::apply_to(entry, server.shared.credentials.catalog()).expect("apply catalog entry");
    apply_post_apply_side_effects_sync(entry, &server.shared);
}

/// A replicated `PutRetentionPolicy` makes the policy durable and live on a
/// node that never parsed the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_installs_the_retention_policy() {
    let server = TestServer::start().await;
    assert!(
        server
            .shared
            .retention_policy_registry
            .get(DB, TENANT, POLICY)
            .is_none(),
        "no policy is registered before the entry applies"
    );

    apply_entry(
        &server,
        &CatalogEntry::PutRetentionPolicy(Box::new(definition())),
    );

    let live = server
        .shared
        .retention_policy_registry
        .get(DB, TENANT, POLICY)
        .expect("apply must install the definition in the live registry");
    assert_eq!(live.collection, COLLECTION);
    assert!(live.auto_tier);
    assert!(live.enabled);
    assert_eq!(live.tiers.len(), 1);

    assert!(
        stored_names(&server).contains(&POLICY.to_string()),
        "apply must write the durable row too"
    );
}

/// A replicated re-put carries an ALTER: the registry reflects the new record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_replaces_the_registered_definition() {
    let server = TestServer::start().await;
    apply_entry(
        &server,
        &CatalogEntry::PutRetentionPolicy(Box::new(definition())),
    );

    let disabled = RetentionPolicyDef {
        enabled: false,
        auto_tier: false,
        ..definition()
    };
    apply_entry(
        &server,
        &CatalogEntry::PutRetentionPolicy(Box::new(disabled)),
    );

    let live = server
        .shared
        .retention_policy_registry
        .get(DB, TENANT, POLICY)
        .expect("the policy stays registered after the re-put");
    assert!(!live.enabled, "the re-put must reach live enforcement");
    assert!(
        !live.auto_tier,
        "the re-put must reach the auto-tier planner"
    );
    assert_eq!(
        stored_names(&server)
            .iter()
            .filter(|n| n.as_str() == POLICY)
            .count(),
        1,
        "a re-put overwrites one row"
    );
}

/// A replicated `DeleteRetentionPolicy` drops both the row and the live entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_delete_removes_the_retention_policy() {
    let server = TestServer::start().await;
    apply_entry(
        &server,
        &CatalogEntry::PutRetentionPolicy(Box::new(definition())),
    );
    assert!(
        server
            .shared
            .retention_policy_registry
            .get(DB, TENANT, POLICY)
            .is_some()
    );

    apply_entry(&server, &delete_entry());

    assert!(
        server
            .shared
            .retention_policy_registry
            .get(DB, TENANT, POLICY)
            .is_none(),
        "the live registry entry must be gone"
    );
    assert!(
        !stored_names(&server).contains(&POLICY.to_string()),
        "the durable row must be gone"
    );
}
