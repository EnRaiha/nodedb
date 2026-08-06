// SPDX-License-Identifier: BUSL-1.1

//! A `CREATE MATERIALIZED VIEW ... STREAMING` created over SQL must wire the
//! Event-Plane incremental aggregation: the DDL handler has to register a
//! streaming MV definition into `mv_registry` so that writes to the source
//! collection — fanned out through the MV's source change stream — drive an
//! O(1)-per-event incremental aggregate update.
//!
//! Read surface: streaming-MV aggregate state lives in the in-memory
//! `mv_registry` (per-group partial aggregate `MvState`), not in a batch
//! target collection like `REFRESH MATERIALIZED VIEW`. The harness exposes
//! `TestServer::shared`, so this test observes the aggregation directly on the
//! registry the Event Plane maintains. The default harness connection runs as
//! tenant id 1.

mod common;

use std::time::Duration;

use common::pgwire_harness::TestServer;

/// The default harness superuser (`nodedb`) is provisioned under tenant id 1.
const TENANT_ID: u64 = 1;

/// Streaming materialized view fed by a change stream must incrementally
/// aggregate source writes: two `active` orders and one `pending` order must
/// surface as per-group COUNT/SUM in the MV's live aggregate state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_mv_incrementally_aggregates_source_writes() {
    let server = TestServer::start().await;

    // Base collection whose writes feed the change stream.
    server.exec("CREATE COLLECTION smv_orders").await.unwrap();

    // Change stream over the base collection. Streaming MVs source from a
    // change stream (registered in `stream_registry`), not from the base
    // collection directly. Registered via catalog post-apply on CREATE.
    server
        .exec("CREATE CHANGE STREAM smv_order_changes ON smv_orders")
        .await
        .unwrap();

    // Streaming MV: aggregate per `status` with COUNT(*) and SUM(amount),
    // sourced from the change stream via the FROM clause. The `ON smv_orders`
    // clause names the lineage collection so the handler's source-existence
    // check passes; `STREAMING` selects incremental refresh mode.
    server
        .exec(
            "CREATE MATERIALIZED VIEW smv_order_stats ON smv_orders STREAMING AS \
             SELECT status, COUNT(*) AS cnt, SUM(amount) AS total \
             FROM smv_order_changes GROUP BY status",
        )
        .await
        .unwrap();

    // Writes to the base collection produce WriteEvents that the Event Plane
    // fans out to the change stream and, from there, into every streaming MV
    // sourced from that stream.
    server
        .exec("INSERT INTO smv_orders { id: 'o1', status: 'active', amount: 10 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO smv_orders { id: 'o2', status: 'active', amount: 20 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO smv_orders { id: 'o3', status: 'pending', amount: 5 }")
        .await
        .unwrap();

    // The Event Plane consumes WriteEvents asynchronously. Poll the registry
    // until the MV state materializes both group keys (or time out). This is a
    // deterministic convergence poll, not a blind fixed sleep.
    let mut results: Vec<(String, Vec<(String, f64)>)> = Vec::new();
    for _ in 0..80 {
        if let Some(state) = server.shared.mv_registry.get_state(
            nodedb::types::DatabaseId::DEFAULT,
            TENANT_ID,
            "smv_order_stats",
        ) {
            results = state.read_results();
            if results.len() >= 2 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A correctly-wired streaming MV registers its definition on CREATE and
    // then incrementally aggregates the three source writes into two groups.
    // Today the neutral DDL handler never registers a `StreamingMvDef`, so the
    // registry has no state for the view and this assertion fails.
    assert_eq!(
        results.len(),
        2,
        "streaming MV must aggregate source writes into two groups (active, pending); got {results:?}"
    );

    let active = results
        .iter()
        .find(|(k, _)| k == "active")
        .expect("`active` group must be present in streaming MV state");
    // Aggregate order matches the SELECT list: index 0 = COUNT(*), 1 = SUM(amount).
    assert!(
        (active.1[0].1 - 2.0).abs() < f64::EPSILON,
        "active COUNT(*) must be 2; got {active:?}"
    );
    assert!(
        (active.1[1].1 - 30.0).abs() < f64::EPSILON,
        "active SUM(amount) must be 10 + 20 = 30; got {active:?}"
    );

    let pending = results
        .iter()
        .find(|(k, _)| k == "pending")
        .expect("`pending` group must be present in streaming MV state");
    assert!(
        (pending.1[0].1 - 1.0).abs() < f64::EPSILON,
        "pending COUNT(*) must be 1; got {pending:?}"
    );
    assert!(
        (pending.1[1].1 - 5.0).abs() < f64::EPSILON,
        "pending SUM(amount) must be 5; got {pending:?}"
    );
}

/// Install a mask on `collection`.`field` for a single role, exactly as the
/// metadata applier does when a policy replicates.
fn install_mask(server: &TestServer, collection: &str, field: &str) {
    let stored = nodedb::control::security::catalog::redaction::StoredRedactionPolicy {
        tenant_id: TENANT_ID,
        collection: collection.to_string(),
        for_role: "support".to_string(),
        name: format!("mask_{collection}_{field}"),
        rules_json: format!(r#"[{{"field":"{field}","mode":{{"Mask":"***"}}}}]"#),
    };
    server
        .shared
        .redaction
        .install_replicated_policy(stored.to_runtime().expect("policy parses"));
}

/// A streaming MV keeps its group key and its aggregate state in storage the
/// result-path mask never reaches, so a definition over a protected column
/// would put that column's cleartext at rest — keyed by it, or accumulated from
/// it. The DDL must be refused before the definition is proposed, and the
/// refusal must stay per column: a rule elsewhere on the same collection cannot
/// block a view that never reads it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_mv_over_a_redacted_column_is_refused() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION smv_pii").await.unwrap();
    server
        .exec("CREATE CHANGE STREAM smv_pii_changes ON smv_pii")
        .await
        .unwrap();
    install_mask(&server, "smv_pii", "amount");

    let grouped = server
        .exec(
            "CREATE MATERIALIZED VIEW smv_pii_by_amount ON smv_pii STREAMING AS \
             SELECT amount, COUNT(*) AS cnt FROM smv_pii_changes GROUP BY amount",
        )
        .await
        .expect_err("grouping by a redacted column must be refused");
    assert!(
        grouped.contains("amount") && grouped.contains("smv_pii"),
        "the refusal must name the column and the collection; got {grouped}"
    );

    let aggregated = server
        .exec(
            "CREATE MATERIALIZED VIEW smv_pii_total ON smv_pii STREAMING AS \
             SELECT status, SUM(amount) AS total FROM smv_pii_changes GROUP BY status",
        )
        .await
        .expect_err("aggregating a redacted column must be refused");
    assert!(
        aggregated.contains("amount"),
        "the refusal must name the aggregated column; got {aggregated}"
    );

    // A rule on `amount` must not block a view that groups by `status` and
    // counts events: nothing it persists comes from the protected column.
    server
        .exec(
            "CREATE MATERIALIZED VIEW smv_pii_by_status ON smv_pii STREAMING AS \
             SELECT status, COUNT(*) AS cnt FROM smv_pii_changes GROUP BY status",
        )
        .await
        .expect("a view reading no redacted column must still be created");
    assert!(
        server
            .shared
            .mv_registry
            .get_def(
                nodedb::types::DatabaseId::DEFAULT,
                TENANT_ID,
                "smv_pii_by_status"
            )
            .is_some(),
        "the allowed view must be registered"
    );
    assert!(
        server
            .shared
            .mv_registry
            .get_def(
                nodedb::types::DatabaseId::DEFAULT,
                TENANT_ID,
                "smv_pii_by_amount"
            )
            .is_none(),
        "a refused view must never be proposed to the catalog"
    );
}
