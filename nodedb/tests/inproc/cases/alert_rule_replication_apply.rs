// SPDX-License-Identifier: BUSL-1.1

//! Alert rules are replicated catalog state, not node-local config.
//!
//! A node that did not run the statement receives the mutation as a
//! `CatalogEntry` and must end up evaluating the same rule. These tests drive
//! that follower path directly — apply, then the synchronous post-apply — and
//! assert on `AlertRegistry` and `HysteresisManager`, the components the alert
//! eval loop reads.

use nodedb::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync;
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb::event::alert::hysteresis::EvaluateParams;
use nodedb::event::alert::types::{AlertCondition, AlertDef, CompareOp, NotifyTarget};
use nodedb_test_support::pgwire_harness::TestServer;

const DB: u64 = 0;
const TENANT: u64 = 1;
const ALERT: &str = "replicated_high_temp";
const COLLECTION: &str = "replicated_metrics";
const GROUP: &str = "device-1";

fn definition() -> AlertDef {
    AlertDef {
        database_id: DB,
        tenant_id: TENANT,
        name: ALERT.to_string(),
        collection: COLLECTION.to_string(),
        where_filter: None,
        condition: AlertCondition {
            agg_func: "avg".to_string(),
            column: "temperature".to_string(),
            op: CompareOp::Gt,
            threshold: 90.0,
        },
        group_by: vec!["device_id".to_string()],
        window_ms: 300_000,
        fire_after: 1,
        recover_after: 1,
        severity: "critical".to_string(),
        notify_targets: vec![NotifyTarget::Topic {
            name: "alerts".to_string(),
        }],
        enabled: true,
        owner: "admin".to_string(),
        created_at: 1_000,
    }
}

fn delete_entry() -> CatalogEntry {
    CatalogEntry::DeleteAlertRule {
        database_id: DB,
        tenant_id: TENANT,
        name: ALERT.to_string(),
    }
}

/// The names of every durable alert rule row.
fn stored_names(server: &TestServer) -> Vec<String> {
    server
        .shared
        .credentials
        .catalog()
        .load_all_alert_rules()
        .expect("load alert rules")
        .into_iter()
        .map(|a| a.name)
        .collect()
}

/// Apply an entry the way the metadata applier does: durable write first,
/// then the synchronous in-memory install.
fn apply_entry(server: &TestServer, entry: &CatalogEntry) {
    apply::apply_to(entry, server.shared.credentials.catalog()).expect("apply catalog entry");
    apply_post_apply_side_effects_sync(entry, &server.shared);
}

/// A replicated `PutAlertRule` makes the rule durable and live on a node that
/// never parsed the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_installs_the_alert_rule() {
    let server = TestServer::start().await;
    assert!(
        server
            .shared
            .alert_registry
            .get(DB, TENANT, ALERT)
            .is_none(),
        "no rule is registered before the entry applies"
    );

    apply_entry(&server, &CatalogEntry::PutAlertRule(Box::new(definition())));

    let live = server
        .shared
        .alert_registry
        .get(DB, TENANT, ALERT)
        .expect("apply must install the definition in the live registry");
    assert_eq!(live.collection, COLLECTION);
    assert!(live.enabled);
    assert_eq!(live.condition.threshold, 90.0);
    assert_eq!(live.notify_targets.len(), 1);

    assert!(
        stored_names(&server).contains(&ALERT.to_string()),
        "apply must write the durable row too"
    );
}

/// A replicated re-put carries an ALTER: the registry reflects the new record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_replaces_the_registered_definition() {
    let server = TestServer::start().await;
    apply_entry(&server, &CatalogEntry::PutAlertRule(Box::new(definition())));

    let disabled = AlertDef {
        enabled: false,
        ..definition()
    };
    apply_entry(&server, &CatalogEntry::PutAlertRule(Box::new(disabled)));

    let live = server
        .shared
        .alert_registry
        .get(DB, TENANT, ALERT)
        .expect("the rule stays registered after the re-put");
    assert!(!live.enabled, "the re-put must reach the eval loop");
    assert!(
        !server
            .shared
            .alert_registry
            .list_all_enabled()
            .iter()
            .any(|a| a.name == ALERT),
        "a disabled rule must drop out of the eval loop's list"
    );
    assert_eq!(
        stored_names(&server)
            .iter()
            .filter(|n| n.as_str() == ALERT)
            .count(),
        1,
        "a re-put overwrites one row"
    );
}

/// A replicated `DeleteAlertRule` drops the row, the live entry, and the
/// hysteresis counters the eval loop carries for the rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_delete_removes_the_alert_rule_and_its_hysteresis() {
    let server = TestServer::start().await;
    apply_entry(&server, &CatalogEntry::PutAlertRule(Box::new(definition())));
    assert!(
        server
            .shared
            .alert_registry
            .get(DB, TENANT, ALERT)
            .is_some()
    );

    server.shared.alert_hysteresis.evaluate(EvaluateParams {
        tenant_id: TENANT,
        alert_name: ALERT,
        group_key: GROUP,
        condition_met: true,
        value: 99.0,
        fire_after: 1,
        recover_after: 1,
        now_ms: 1_000,
    });
    assert!(
        server
            .shared
            .alert_hysteresis
            .get_state(TENANT, ALERT, GROUP)
            .is_some(),
        "the evaluation must leave hysteresis state to clean up"
    );

    apply_entry(&server, &delete_entry());

    assert!(
        server
            .shared
            .alert_registry
            .get(DB, TENANT, ALERT)
            .is_none(),
        "the live registry entry must be gone"
    );
    assert!(
        server
            .shared
            .alert_hysteresis
            .get_state(TENANT, ALERT, GROUP)
            .is_none(),
        "the replicated delete must clear hysteresis state on every node"
    );
    assert!(
        !stored_names(&server).contains(&ALERT.to_string()),
        "the durable row must be gone"
    );
}
